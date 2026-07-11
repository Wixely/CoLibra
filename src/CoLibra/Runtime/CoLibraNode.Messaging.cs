using System.Collections.Concurrent;
using CoLibra.Protocol;
using CoLibra.Transport;
using Microsoft.Extensions.Logging;

namespace CoLibra.Runtime;

internal sealed partial class CoLibraNode : ICoLibraMessenger
{
    // ---- handler table (written from any thread via RegisterHandler; read on delivery) ----
    private readonly ConcurrentDictionary<string, Func<ReceivedMessage, CancellationToken, ValueTask>> _messageHandlers =
        new(StringComparer.Ordinal);

    // ---- actor-owned messaging state ----
    private readonly Dictionary<Guid, TaskCompletionSource<DirectAckStatus>> _pendingMessageAcks = [];

    public ICoLibraMessenger Messenger => _options.Messaging.Enabled
        ? this
        : throw new InvalidOperationException(
            "Node-to-node messaging is disabled; set CoLibraOptions.Messaging.Enabled = true to use the Messenger.");

    // =====================================================================================
    // Handler registration
    // =====================================================================================

    IAsyncDisposable ICoLibraMessenger.RegisterHandler(
        string channel, Func<ReceivedMessage, CancellationToken, ValueTask> handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(channel);
        ArgumentNullException.ThrowIfNull(handler);
        if (!_messageHandlers.TryAdd(channel, handler))
            throw new InvalidOperationException($"A message handler for channel '{channel}' is already registered on this node.");

        return new MessageHandlerRegistration(this, channel);
    }

    IAsyncDisposable ICoLibraMessenger.RegisterHandler<T>(
        string channel, Func<ReceivedMessage<T>, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var serializer = _options.Messaging.PayloadSerializer;
        return ((ICoLibraMessenger)this).RegisterHandler(channel, (message, ct) =>
        {
            var value = serializer.Deserialize<T>(message.Payload)
                ?? throw new InvalidDataException($"Message on channel '{channel}' deserialized to null as {typeof(T).Name}.");
            return handler(new ReceivedMessage<T>
            {
                Channel = message.Channel,
                Value = value,
                Origin = message.Origin,
                OriginName = message.OriginName,
            }, ct);
        });
    }

    private sealed class MessageHandlerRegistration(CoLibraNode node, string channel) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            node._messageHandlers.TryRemove(channel, out _);
            return ValueTask.CompletedTask;
        }
    }

    // =====================================================================================
    // Sending
    // =====================================================================================

    ValueTask<SendResult> ICoLibraMessenger.SendAsync(
        NodeId target, string channel, byte[] payload, MessageDelivery delivery, CancellationToken cancellationToken) =>
        ((ICoLibraMessenger)this).SendAsync(target, channel, (ReadOnlyMemory<byte>)payload, delivery, cancellationToken);

    ValueTask<SendResult> ICoLibraMessenger.SendAsync<T>(
        NodeId target, string channel, T value, MessageDelivery delivery, CancellationToken cancellationToken) =>
        ((ICoLibraMessenger)this).SendAsync(
            target, channel, (ReadOnlyMemory<byte>)_options.Messaging.PayloadSerializer.Serialize(value), delivery, cancellationToken);

    async ValueTask<SendResult> ICoLibraMessenger.SendAsync(
        NodeId target, string channel, ReadOnlyMemory<byte> payload, MessageDelivery delivery, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(channel);
        if (payload.Length > _options.Messaging.MaxPayloadBytes)
            return new SendResult(SendStatus.PayloadTooLarge, target);

        // Self-send: deliver in-process, no network, no membership requirement.
        if (target == LocalNodeId)
        {
            var localStatus = DeliverMessageLocal(channel, payload.ToArray(), LocalNodeId, _options.NodeName);
            if (!IsAckedDelivery(delivery))
                return new SendResult(SendStatus.Sent, target);
            return new SendResult(localStatus == DirectAckStatus.Delivered ? SendStatus.Delivered : SendStatus.NoHandler, target);
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token);
        timeout.CancelAfter(_options.Messaging.DeliveryTimeout);

        // Target ids normally come from the member list, so "unknown" usually means the
        // membership update announcing it hasn't reached this node yet — grant a short grace.
        var member = _members.FirstOrDefault(m => m.NodeId == target);
        for (var graceDeadline = _time.GetTimestamp() + ToTicks(TimeSpan.FromSeconds(1));
             member is null && _time.GetTimestamp() < graceDeadline && !timeout.Token.IsCancellationRequested;)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            member = _members.FirstOrDefault(m => m.NodeId == target);
        }

        if (member is null)
            return new SendResult(SendStatus.UnknownTarget, target);
        try
        {
            var status = await SendDirectMessageAsync(member, channel, payload, delivery, timeout.Token).ConfigureAwait(false);
            return new SendResult(status switch
            {
                DirectAckStatus.Delivered when !IsAckedDelivery(delivery) => SendStatus.Sent,
                DirectAckStatus.Delivered => SendStatus.Delivered,
                DirectAckStatus.NoHandler => SendStatus.NoHandler,
                _ => SendStatus.UnknownTarget,
            }, target);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new SendResult(SendStatus.Timeout, target);
        }
    }

    async ValueTask<IReadOnlyList<SendResult>> ICoLibraMessenger.SendByNameAsync(
        string name, string channel, ReadOnlyMemory<byte> payload, MessageDelivery delivery, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var targets = _members.Where(m => string.Equals(m.Name, name, StringComparison.Ordinal)).ToList();
        if (targets.Count == 0)
            return [];

        var sends = targets.Select(m => ((ICoLibraMessenger)this).SendAsync(m.NodeId, channel, payload, delivery, cancellationToken).AsTask());
        return await Task.WhenAll(sends).ConfigureAwait(false);
    }

    ValueTask<IReadOnlyList<SendResult>> ICoLibraMessenger.SendByNameAsync<T>(
        string name, string channel, T value, MessageDelivery delivery, CancellationToken cancellationToken) =>
        ((ICoLibraMessenger)this).SendByNameAsync(
            name, channel, (ReadOnlyMemory<byte>)_options.Messaging.PayloadSerializer.Serialize(value), delivery, cancellationToken);

    async ValueTask<IReadOnlyList<SendResult>> ICoLibraMessenger.BroadcastAsync(
        string channel, ReadOnlyMemory<byte> payload, MessageDelivery delivery, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(channel);
        var targets = _members.Where(m => m.NodeId != LocalNodeId).Select(m => m.NodeId).ToList();
        if (targets.Count == 0)
            return [];

        var sends = targets.Select(id => ((ICoLibraMessenger)this).SendAsync(id, channel, payload, delivery, cancellationToken).AsTask());
        return await Task.WhenAll(sends).ConfigureAwait(false);
    }

    ValueTask<IReadOnlyList<SendResult>> ICoLibraMessenger.BroadcastAsync(
        string channel, byte[] payload, MessageDelivery delivery, CancellationToken cancellationToken) =>
        ((ICoLibraMessenger)this).BroadcastAsync(channel, (ReadOnlyMemory<byte>)payload, delivery, cancellationToken);

    ValueTask<IReadOnlyList<SendResult>> ICoLibraMessenger.BroadcastAsync<T>(
        string channel, T value, MessageDelivery delivery, CancellationToken cancellationToken) =>
        ((ICoLibraMessenger)this).BroadcastAsync(
            channel, (ReadOnlyMemory<byte>)_options.Messaging.PayloadSerializer.Serialize(value), delivery, cancellationToken);

    private static bool IsAckedDelivery(MessageDelivery delivery) =>
        delivery is MessageDelivery.ReliableOrdered or MessageDelivery.Reliable;

    private async Task<DirectAckStatus> SendDirectMessageAsync(
        ClusterMember target, string channel, ReadOnlyMemory<byte> payload, MessageDelivery delivery, CancellationToken ct)
    {
        var wantAck = IsAckedDelivery(delivery);

        // Prefer the UDP data plane when available; a null result falls through to TCP.
        if (UdpEligible(target, payload))
        {
            var udpStatus = await TrySendUdpAsync(target, channel, payload, delivery, wantAck, ct).ConfigureAwait(false);
            if (udpStatus is { } status)
                return status;
        }

        var messageId = Guid.NewGuid();
        var tcs = new TaskCompletionSource<DirectAckStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            IMessageChannel? direct = null;
            if (_options.Messaging.UseDirectChannels && !_isCoordinatorRole)
                direct = await GetOrCreateDirectChannelAsync(target.NodeId, target.Endpoint, ct).ConfigureAwait(false);

            await PostWithResult<bool>(inner =>
            {
                if (wantAck)
                    _pendingMessageAcks[messageId] = tcs;
                var message = new DirectMessageMessage(messageId, channel, LocalNodeId.Value, _options.NodeName,
                    RelayToNodeId: null, WantAck: wantAck)
                {
                    Payload = payload.ToArray(),
                };

                if (_coordinator is { } coordinator)
                {
                    if (coordinator.Sessions.TryGetValue(target.NodeId, out var session))
                        _ = SendSafeAsync(session.Connection, message with { RelayToNodeId = target.NodeId.Value });
                    else
                        CompleteMessageAck(new DirectMessageAckMessage(messageId, DirectAckStatus.Unreachable, null));
                }
                else if (direct is not null)
                {
                    _ = SendSafeAsync(direct, message);
                }
                else if (_member is { Connection: { } conn })
                {
                    _ = SendSafeAsync(conn, message with { RelayToNodeId = target.NodeId.Value });
                }
                else
                {
                    CompleteMessageAck(new DirectMessageAckMessage(messageId, DirectAckStatus.Unreachable, null));
                }

                inner.TrySetResult(true);
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);

            if (!wantAck)
                return DirectAckStatus.Delivered; // fire-and-forget: mapped to SendStatus.Sent by the caller

            return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            if (wantAck)
            {
                Post(() =>
                {
                    _pendingMessageAcks.Remove(messageId);
                    return ValueTask.CompletedTask;
                });
            }
        }
    }

    // =====================================================================================
    // Delivery + relay (actor)
    // =====================================================================================

    private DirectAckStatus DeliverMessageLocal(string channel, byte[] payload, NodeId origin, string? originName)
    {
        if (!_options.Messaging.Enabled || !_messageHandlers.TryGetValue(channel, out var handler))
            return DirectAckStatus.NoHandler;

        var message = new ReceivedMessage
        {
            Channel = channel,
            Payload = payload,
            Origin = origin,
            OriginName = originName,
        };
        _ = Task.Run(async () =>
        {
            try
            {
                await handler(message, _stopping.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Message handler for channel '{Channel}' threw; delivery was already acknowledged", channel);
            }
        }, CancellationToken.None);
        return DirectAckStatus.Delivered;
    }

    private void HandleDirectMessage(PeerConn peer, DirectMessageMessage message)
    {
        var origin = new NodeId(message.OriginNodeId);

        if (message.RelayToNodeId is { } target && target != LocalNodeId.Value)
        {
            // Relay hop: forward to the target member, preserving RelayTo so it acks through us.
            if (_coordinator is { } coordinator && coordinator.Sessions.TryGetValue(new NodeId(target), out var session))
                _ = SendSafeAsync(session.Connection, message);
            else
                _ = SendSafeAsync(peer.Channel, new DirectMessageAckMessage(message.MessageId, DirectAckStatus.Unreachable, origin.Value));
            return;
        }

        var status = DeliverMessageLocal(message.Channel, message.Payload, origin, message.OriginName);
        if (!message.WantAck)
            return; // fire-and-forget delivery: the sender isn't waiting

        var ackRelay = message.RelayToNodeId is not null && origin != LocalNodeId ? origin.Value : (Guid?)null;
        _ = SendSafeAsync(peer.Channel, new DirectMessageAckMessage(message.MessageId, status, ackRelay));
    }

    private void HandleDirectMessageAck(PeerConn peer, DirectMessageAckMessage ack)
    {
        if (ack.RelayToNodeId is { } target && target != LocalNodeId.Value)
        {
            if (_coordinator is { } coordinator && coordinator.Sessions.TryGetValue(new NodeId(target), out var session))
                _ = SendSafeAsync(session.Connection, ack with { RelayToNodeId = null });
            return;
        }

        CompleteMessageAck(ack);
    }

    private void CompleteMessageAck(DirectMessageAckMessage ack)
    {
        if (_pendingMessageAcks.Remove(ack.MessageId, out var tcs))
            tcs.TrySetResult(ack.Status);
    }
}
