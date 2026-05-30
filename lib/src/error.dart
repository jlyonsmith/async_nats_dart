/// Errors thrown by the async_nats library.
class NatsError implements Exception {
  final String message;
  final String? serverMessage;

  const NatsError(this.message, {this.serverMessage});

  @override
  String toString() {
    if (serverMessage != null) {
      return 'NatsError: $message (server: $serverMessage)';
    }
    return 'NatsError: $message';
  }
}
