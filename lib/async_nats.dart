/// A pure-Dart NATS protocol client.
library async_nats;

export 'src/client.dart' show Client, DisconnectedCallback;
export 'src/error.dart' show NatsError;
export 'src/message.dart' show Message;
export 'src/server_info.dart' show ServerInfo;
export 'src/subscriber.dart' show Subscriber;

import 'dart:io';
import 'src/client.dart';

/// Connect to a NATS server at [host]:[port].
///
/// Optional parameters:
/// - [name]       : client name sent in CONNECT
/// - [user]       : username for authentication
/// - [password]   : password for authentication
/// - [authToken]  : token for authentication
/// - [onDisconnected] : called when the connection drops
Future<Client> connect(
  String host, {
  int port = 4222,
  String? name,
  String? user,
  String? password,
  String? authToken,
  DisconnectedCallback? onDisconnected,
}) async {
  final socket = await Socket.connect(host, port);
  return Client.create(
    socket,
    name: name,
    user: user,
    password: password,
    authToken: authToken,
    onDisconnected: onDisconnected,
  );
}
