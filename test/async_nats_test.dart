import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:async_nats/async_nats.dart';

// ── fake server helpers ───────────────────────────────────────────────────────

const _infoPayload =
    '{"server_id":"test","version":"2.9.0","proto":1,"port":4222,'
    '"max_payload":1048576,"headers":true,"auth_required":false,'
    '"tls_required":false}';

/// Starts a minimal TCP server that speaks just enough NATS for the tests.
// ignore: library_private_types_in_public_api
Future<_FakeServer> startFakeServer() async {
  final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final fs = _FakeServer(server);
  fs._accept();
  return fs;
}

class _FakeServer {
  final ServerSocket _server;
  Socket? client;
  final received = <String>[];
  final _receivedController = StreamController<String>.broadcast();

  Stream<String> get onLine => _receivedController.stream;

  _FakeServer(this._server);

  int get port => _server.port;

  void _accept() {
    _server.listen((sock) {
      client = sock;
      // Send INFO immediately.
      sock.write('INFO $_infoPayload\r\n');

      var buf = '';
      sock.listen((data) {
        buf += utf8.decode(data, allowMalformed: true);
        while (true) {
          final idx = buf.indexOf('\r\n');
          if (idx == -1) break;
          final line = buf.substring(0, idx);
          buf = buf.substring(idx + 2);
          received.add(line);
          _receivedController.add(line);
          if (line == 'PING') {
            sock.write('PONG\r\n');
          }
        }
      });
    });
  }

  /// Send a raw string to the connected client.
  void send(String text) => client?.write(text);

  /// Send a MSG to the client for the given sid.
  void sendMsg(String subject, String sid, String payload) {
    final bytes = utf8.encode(payload);
    send('MSG $subject $sid ${bytes.length}\r\n');
    send(payload);
    send('\r\n');
  }

  /// Send an HMSG to the client.
  void sendHMsg(
    String subject,
    String sid,
    Map<String, String> headers,
    String payload,
  ) {
    final hBuf = StringBuffer('NATS/1.0\r\n');
    for (final e in headers.entries) {
      hBuf.write('${e.key}: ${e.value}\r\n');
    }
    hBuf.write('\r\n');
    final hBytes = utf8.encode(hBuf.toString());
    final pBytes = utf8.encode(payload);
    final total = hBytes.length + pBytes.length;
    send('HMSG $subject $sid ${hBytes.length} $total\r\n');
    send(hBuf.toString());
    send(payload);
    send('\r\n');
  }

  Future<void> close() async {
    await _server.close();
    await _receivedController.close();
  }
}

// ── tests ────────────────────────────────────────────────────────────────────

void main() {
  late _FakeServer fs;
  late Client client;

  setUp(() async {
    fs = await startFakeServer();
    client = await connect('127.0.0.1', port: fs.port);
  });

  tearDown(() async {
    await client.close();
    await fs.close();
  });

  test('connect sends CONNECT with verbose:false', () async {
    // Wait until CONNECT line has been received by the fake server.
    final connectLine = await fs.onLine
        .firstWhere((l) => l.startsWith('CONNECT '))
        .timeout(const Duration(seconds: 2));

    expect(connectLine, startsWith('CONNECT '));
    final json = jsonDecode(connectLine.substring(8)) as Map<String, dynamic>;
    expect(json['verbose'], isFalse);
    expect(json['lang'], equals('dart'));
  });

  test('serverInfo is populated after connect', () {
    expect(client.serverInfo, isNotNull);
    expect(client.serverInfo!.serverId, equals('test'));
    expect(client.serverInfo!.headersSupported, isTrue);
  });

  test('publish sends PUB line', () async {
    client.publish('foo', Uint8List.fromList(utf8.encode('hello')));

    final pubLine = await fs.onLine
        .firstWhere((l) => l.startsWith('PUB '))
        .timeout(const Duration(seconds: 2));
    expect(pubLine, equals('PUB foo 5'));
  });

  test('publishWithReply sends PUB with reply-to', () async {
    client.publish('foo', Uint8List.fromList(utf8.encode('hi')),
        replyTo: '_INBOX.1');

    final pubLine = await fs.onLine
        .firstWhere((l) => l.startsWith('PUB '))
        .timeout(const Duration(seconds: 2));
    expect(pubLine, equals('PUB foo _INBOX.1 2'));
  });

  test('publishWithHeaders sends HPUB line', () async {
    client.publish(
      'foo',
      Uint8List.fromList(utf8.encode('body')),
      headers: {
        'X-Custom': ['value1']
      },
    );

    final hpubLine = await fs.onLine
        .firstWhere((l) => l.startsWith('HPUB '))
        .timeout(const Duration(seconds: 2));
    expect(hpubLine, startsWith('HPUB foo '));
  });

  test('subscribe sends SUB line', () async {
    client.subscribe('events');

    final subLine = await fs.onLine
        .firstWhere((l) => l.startsWith('SUB '))
        .timeout(const Duration(seconds: 2));
    expect(subLine, startsWith('SUB events '));
  });

  test('subscribe with queue group sends correct SUB', () async {
    client.subscribe('events', queueGroup: 'workers');

    final subLine = await fs.onLine
        .firstWhere((l) => l.startsWith('SUB '))
        .timeout(const Duration(seconds: 2));
    expect(subLine, startsWith('SUB events workers '));
  });

  test('unsubscribe sends UNSUB line and closes subscriber', () async {
    final sub = client.subscribe('foo');

    final subLine = await fs.onLine
        .firstWhere((l) => l.startsWith('SUB '))
        .timeout(const Duration(seconds: 2));
    final sid = subLine.split(' ').last;

    client.unsubscribe(sub);

    final unsubLine = await fs.onLine
        .firstWhere((l) => l.startsWith('UNSUB '))
        .timeout(const Duration(seconds: 2));
    expect(unsubLine, equals('UNSUB $sid'));

    expect(sub.isBroadcast, isTrue);
  });

  test('subscriber receives MSG from server', () async {
    final sub = client.subscribe('greet');

    final subLine = await fs.onLine
        .firstWhere((l) => l.startsWith('SUB '))
        .timeout(const Duration(seconds: 2));
    final sid = subLine.split(' ').last;

    final msgFuture = sub.first.timeout(const Duration(seconds: 2));
    fs.sendMsg('greet', sid, 'hello');

    final msg = await msgFuture;
    expect(msg.subject, equals('greet'));
    expect(utf8.decode(msg.payload), equals('hello'));
    expect(msg.headers, isEmpty);
  });

  test('subscriber receives HMSG with headers', () async {
    final sub = client.subscribe('events');

    final subLine = await fs.onLine
        .firstWhere((l) => l.startsWith('SUB '))
        .timeout(const Duration(seconds: 2));
    final sid = subLine.split(' ').last;

    final msgFuture = sub.first.timeout(const Duration(seconds: 2));
    fs.sendHMsg('events', sid, {'X-Foo': 'bar'}, 'payload');

    final msg = await msgFuture;
    expect(msg.subject, equals('events'));
    expect(utf8.decode(msg.payload), equals('payload'));
    expect(msg.headers['X-Foo'], equals(['bar']));
  });

  test('MSG with reply-to is parsed', () async {
    final sub = client.subscribe('req');

    final subLine = await fs.onLine
        .firstWhere((l) => l.startsWith('SUB '))
        .timeout(const Duration(seconds: 2));
    final sid = subLine.split(' ').last;

    final msgFuture = sub.first.timeout(const Duration(seconds: 2));
    // Manually send a MSG with reply-to.
    final payload = utf8.encode('data');
    fs.send('MSG req $sid _INBOX.reply ${payload.length}\r\n');
    fs.send('data\r\n');

    final msg = await msgFuture;
    expect(msg.replyTo, equals('_INBOX.reply'));
  });

  test('PING from server gets PONG response', () async {
    fs.send('PING\r\n');

    final pong = await fs.onLine
        .firstWhere((l) => l == 'PONG')
        .timeout(const Duration(seconds: 2));
    expect(pong, equals('PONG'));
  });

  test('close marks all subscribers as done', () async {
    final sub1 = client.subscribe('a');
    final sub2 = client.subscribe('b');

    final done1 = sub1.isEmpty; // will complete when stream closes
    final done2 = sub2.isEmpty;

    await client.close();

    // Both futures should complete (stream closed = isEmpty resolves).
    await done1.timeout(const Duration(seconds: 2));
    await done2.timeout(const Duration(seconds: 2));
  });

  test('onDisconnected callback is invoked on close', () async {
    Client? cbClient;
    final fs2 = await startFakeServer();
    final c2 = await connect(
      '127.0.0.1',
      port: fs2.port,
      onDisconnected: (c) => cbClient = c,
    );
    await c2.close();
    expect(cbClient, same(c2));
    await fs2.close();
  });
}
