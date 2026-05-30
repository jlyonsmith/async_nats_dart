# Async NATS

A pure-Dart [NATS](https://nats.io) messaging protocol client with no runtime dependencies.

## Features

- Pure Dart — no native bindings, no external dependencies
- Publish/subscribe with optional reply-to subjects
- NATS headers (`HPUB` / `HMSG`)
- Queue groups
- Username/password and token authentication
- Stream-based subscriptions (`Subscriber` extends `Stream<Message>`)
- Connected/disconnected lifecycle callbacks

## Usage

### Connect

```dart
import 'package:async_nats/async_nats.dart';

final client = await connect(
  'localhost',
  port: 4222,
  name: 'my-client',
  onDisconnected: (_) => print('Disconnected'),
);
```

Optional authentication parameters:

| Parameter   | Purpose           |
| ----------- | ----------------- |
| `user`      | Username          |
| `password`  | Password          |
| `authToken` | Single-token auth |

### Publish

```dart
import 'dart:convert';
import 'dart:typed_data';

// Plain publish
client.publish('greetings', Uint8List.fromList(utf8.encode('hello')));

// With reply-to
client.publish('greetings', Uint8List.fromList(utf8.encode('ping')), replyTo: '_INBOX.123');

// With headers
client.publish(
  'greetings',
  Uint8List.fromList(utf8.encode('hello')),
  headers: {'X-Source': ['dart-client']},
);

// With reply-to and headers
client.publish(
  'greetings',
  Uint8List.fromList(utf8.encode('important')),
  replyTo: '_INBOX.456',
  headers: {'X-Priority': ['high']},
);
```

### Subscribe

```dart
final sub = client.subscribe('greetings');

sub.listen((msg) {
  print('${msg.subject}: ${utf8.decode(msg.payload)}');
  if (msg.replyTo != null) {
    client.publish(msg.replyTo!, Uint8List.fromList(utf8.encode('pong')));
  }
});
```

`Subscriber` is a `Stream<Message>`, so all standard stream operators work — `await for`, `map`, `where`, etc.

### Queue groups

```dart
final sub = client.subscribe('tasks', queueGroup: 'workers');
```

### Unsubscribe and close

```dart
client.unsubscribe(sub); // immediate
client.unsubscribe(sub, maxMessages: 10); // after N messages

await client.close();               // closes socket and all subscribers
```

## API reference

### Top-level

| Symbol                 | Description                                  |
| ---------------------- | -------------------------------------------- |
| `connect(host, {...})` | Connect to a NATS server; returns a `Client` |

### `Client`

| Member                                        | Description                                          |
| --------------------------------------------- | ---------------------------------------------------- |
| `serverInfo`                                  | `ServerInfo?` sent by the server on connect          |
| `isClosed`                                    | Whether the connection has been closed               |
| `publish(subject, payload, replyTo, headers)` | Publish raw bytes with optional reply-to and headers |
| `subscribe(subject, {queueGroup})`            | Subscribe; returns a `Subscriber`                    |
| `unsubscribe(subscriber, {maxMessages})`      | Unsubscribe immediately or after N messages          |
| `close()`                                     | Close the connection                                 |

### `Message`

| Field     | Type                        | Description                          |
| --------- | --------------------------- | ------------------------------------ |
| `subject` | `String`                    | Subject the message was published to |
| `replyTo` | `String?`                   | Optional reply-to subject            |
| `headers` | `Map<String, List<String>>` | NATS message headers (may be empty)  |
| `payload` | `Uint8List`                 | Raw message payload                  |

## Running tests and examples

```sh
dart test
# Or use the Justfile
just test
just example
```

Tests use an in-process fake NATS server and require no external infrastructure.

## License

See [LICENSE](LICENSE).
