import 'dart:typed_data';

/// A message received from a NATS subscription.
class Message {
  /// The subject the message was published to.
  final String subject;

  /// The subscription ID this message was delivered to.
  final String sid;

  /// Optional reply-to subject.
  final String? replyTo;

  /// Message headers (from HMSG), may be empty.
  final Map<String, List<String>> headers;

  /// Raw message payload.
  final Uint8List payload;

  const Message({
    required this.subject,
    required this.sid,
    this.replyTo,
    this.headers = const {},
    required this.payload,
  });
}
