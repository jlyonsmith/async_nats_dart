import 'dart:convert';
import 'dart:typed_data';

import 'package:async_nats/async_nats.dart';

Future<void> main() async {
  final client = await connect(
    'localhost',
    port: 4222,
    name: 'example-client',
    onDisconnected: (_) => print('Disconnected.'),
  );

  // Subscribe to a subject.
  final sub = client.subscribe('greetings');

  // Subscribe to a subject with a queue group.
  final queueSub = client.subscribe('tasks', queueGroup: 'workers');

  // Listen for messages on the plain subscription.
  sub.listen((msg) {
    print('[greetings] ${utf8.decode(msg.payload)}');
    if (msg.replyTo != null) {
      client.publish(msg.replyTo!, Uint8List.fromList(utf8.encode('pong')));
    }
  });

  queueSub.listen((msg) {
    print('[tasks/workers] ${utf8.decode(msg.payload)}');
  });

  // Publish a plain message.
  client.publish('greetings', Uint8List.fromList(utf8.encode('hello world')));

  // Publish with a reply-to subject.
  client.publish(
    'greetings',
    Uint8List.fromList(utf8.encode('ping')),
    replyTo: '_INBOX.123',
  );

  // Publish with headers.
  client.publish(
    'greetings',
    Uint8List.fromList(utf8.encode('hello with headers')),
    headers: {
      'X-Source': ['dart-client']
    },
  );

  // Publish with reply-to and headers.
  client.publish(
    'greetings',
    Uint8List.fromList(utf8.encode('important ping')),
    replyTo: '_INBOX.456',
    headers: {
      'X-Source': ['dart-client'],
      'X-Priority': ['high']
    },
  );

  await Future<void>.delayed(const Duration(seconds: 2));

  client.unsubscribe(sub);
  client.unsubscribe(queueSub);

  await client.close();
}
