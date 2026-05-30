import 'dart:async';
import 'message.dart';

/// A subscription to a NATS subject.
///
/// Implements [Stream<Message>] so callers can `await for` messages or
/// use any Stream combinator.
class Subscriber extends Stream<Message> {
  final String subject;
  final String sid;
  final String? queueGroup;
  final _controller = StreamController<Message>.broadcast();

  Subscriber({
    required this.subject,
    required this.sid,
    this.queueGroup,
  });

  @override
  bool get isBroadcast => _controller.stream.isBroadcast;

  /// Deliver a message to this subscriber.
  void deliver(Message message) {
    if (!_controller.isClosed) {
      _controller.add(message);
    }
  }

  /// Close this subscriber's stream. Called on unsubscribe or connection close.
  void close() {
    if (!_controller.isClosed) {
      _controller.close();
    }
  }

  @override
  StreamSubscription<Message> listen(
    void Function(Message event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _controller.stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }
}
