import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'error.dart';
import 'message.dart';
import 'server_info.dart';
import 'subscriber.dart';

typedef DisconnectedCallback = void Function(Client client);

/// NATS client. Obtain via the top-level [connect] function.
class Client {
  final Socket _socket;
  ServerInfo? _serverInfo;

  DisconnectedCallback? onDisconnected;

  final Map<String, Subscriber> _subscribers = {};
  int _nextSid = 1;

  bool _closed = false;

  // Incomplete line/bytes from the previous read chunk.
  final _lineBuffer = StringBuffer();

  // State machine for reading MSG/HMSG bodies.
  _ReadState _readState = _ReadState.idle;
  _PendingMessage? _pendingMessage;

  Client._(this._socket);

  /// The INFO sent by the server on connect.
  ServerInfo? get serverInfo => _serverInfo;

  bool get isClosed => _closed;

  // ── write helpers ────────────────────────────────────────────────────────

  void _writeLine(String line) {
    if (_closed) throw NatsError('Connection is closed');
    _socket.write('$line\r\n');
  }

  void _writeBytes(List<int> bytes) {
    if (_closed) throw NatsError('Connection is closed');
    _socket.add(bytes);
  }

  // ── publish ──────────────────────────────────────────────────────────────

  /// Publish [payload] to [subject].
  void publish(String subject, Uint8List payload,
      {String? replyTo, Map<String, List<String>>? headers}) {
    if (headers != null) {
      final headerBytes = _encodeHeaders(headers);
      final totalBytes = headerBytes.length + payload.length;

      if (replyTo != null) {
        _writeLine('HPUB $subject $replyTo ${headerBytes.length} $totalBytes');
      } else {
        _writeLine('HPUB $subject ${headerBytes.length} $totalBytes');
      }

      _writeBytes(headerBytes);
    } else {
      if (replyTo != null) {
        _writeLine('PUB $subject $replyTo ${payload.length}');
      } else {
        _writeLine('PUB $subject ${payload.length}');
      }
    }
    _writeBytes(payload);
    _writeLine('');
  }

  // ── subscribe / unsubscribe ───────────────────────────────────────────────

  /// Subscribe to [subject], optionally joining [queueGroup].
  ///
  /// Returns a [Subscriber] which is a [Stream<Message>].
  Subscriber subscribe(String subject, {String? queueGroup}) {
    final sid = '${_nextSid++}';
    final sub = Subscriber(subject: subject, sid: sid, queueGroup: queueGroup);
    _subscribers[sid] = sub;

    if (queueGroup != null) {
      _writeLine('SUB $subject $queueGroup $sid');
    } else {
      _writeLine('SUB $subject $sid');
    }
    return sub;
  }

  /// Unsubscribe the given [subscriber], optionally after [maxMessages].
  void unsubscribe(Subscriber subscriber, {int? maxMessages}) {
    final sid = subscriber.sid;
    if (!_subscribers.containsKey(sid)) return;

    if (maxMessages != null) {
      _writeLine('UNSUB $sid $maxMessages');
    } else {
      _writeLine('UNSUB $sid');
      _subscribers.remove(sid);
      subscriber.close();
    }
  }

  // ── close ────────────────────────────────────────────────────────────────

  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _closeAllSubscribers();
    await _socket.close();
    onDisconnected?.call(this);
  }

  void _closeAllSubscribers() {
    for (final sub in _subscribers.values) {
      sub.close();
    }
    _subscribers.clear();
  }

  // ── header encoding ──────────────────────────────────────────────────────

  static Uint8List _encodeHeaders(Map<String, List<String>> headers) {
    final buf = StringBuffer('NATS/1.0\r\n');
    for (final entry in headers.entries) {
      for (final value in entry.value) {
        buf.write('${entry.key}: $value\r\n');
      }
    }
    buf.write('\r\n');
    return Uint8List.fromList(utf8.encode(buf.toString()));
  }

  // ── incoming data ────────────────────────────────────────────────────────

  void _handleData(Uint8List data) {
    if (_readState == _ReadState.readingBody) {
      _handleBodyChunk(data);
      return;
    }

    // Append to line buffer and process complete lines.
    final text = utf8.decode(data, allowMalformed: true);
    _lineBuffer.write(text);
    final buffered = _lineBuffer.toString();
    _lineBuffer.clear();

    // Split on \r\n keeping the remainder.
    var start = 0;
    while (true) {
      final idx = buffered.indexOf('\r\n', start);
      if (idx == -1) {
        _lineBuffer.write(buffered.substring(start));
        break;
      }
      final line = buffered.substring(start, idx);
      start = idx + 2;
      _processLine(line);
      if (_readState == _ReadState.readingBody) {
        // Body bytes may follow in this same chunk.
        final remaining = buffered.substring(start);
        if (remaining.isNotEmpty) {
          _handleBodyChunk(
            Uint8List.fromList(utf8.encode(remaining)),
          );
        }
        break;
      }
    }
  }

  void _processLine(String line) {
    if (line.isEmpty) return;

    if (line.startsWith('INFO ')) {
      _handleInfo(line.substring(5));
    } else if (line == 'PING') {
      _writeLine('PONG');
    } else if (line == 'PONG') {
      // Nothing to do; we send PING only in response to server PING.
    } else if (line.startsWith('MSG ')) {
      _startMsg(line.substring(4), hasHeaders: false);
    } else if (line.startsWith('HMSG ')) {
      _startMsg(line.substring(5), hasHeaders: true);
    } else if (line.startsWith('-ERR ')) {
      _handleErr(line.substring(5));
    }
    // +OK is suppressed by verbose:false; ignore if it somehow appears.
  }

  void _handleInfo(String jsonStr) {
    _serverInfo = ServerInfo.fromJson(jsonStr);
  }

  void _handleErr(String errMsg) {
    final trimmed = errMsg.replaceAll("'", '').trim();
    _closed = true;
    _closeAllSubscribers();
    _socket.destroy();
    onDisconnected?.call(this);
    throw NatsError('Server error', serverMessage: trimmed);
  }

  // ── MSG / HMSG parsing ───────────────────────────────────────────────────

  void _startMsg(String args, {required bool hasHeaders}) {
    final parts = args.trim().split(RegExp(r'\s+'));

    if (!hasHeaders) {
      // MSG <subject> <sid> [reply-to] <#bytes>
      if (parts.length < 3) return;
      final subject = parts[0];
      final sid = parts[1];
      String? replyTo;
      int byteCount;

      if (parts.length == 4) {
        replyTo = parts[2];
        byteCount = int.tryParse(parts[3]) ?? 0;
      } else {
        byteCount = int.tryParse(parts[2]) ?? 0;
      }

      _pendingMessage = _PendingMessage(
        subject: subject,
        sid: sid,
        replyTo: replyTo,
        totalBytes: byteCount,
        headerBytes: 0,
        hasHeaders: false,
      );
    } else {
      // HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>
      if (parts.length < 4) return;
      final subject = parts[0];
      final sid = parts[1];
      String? replyTo;
      int headerLen;
      int totalLen;

      if (parts.length == 5) {
        replyTo = parts[2];
        headerLen = int.tryParse(parts[3]) ?? 0;
        totalLen = int.tryParse(parts[4]) ?? 0;
      } else {
        headerLen = int.tryParse(parts[2]) ?? 0;
        totalLen = int.tryParse(parts[3]) ?? 0;
      }

      _pendingMessage = _PendingMessage(
        subject: subject,
        sid: sid,
        replyTo: replyTo,
        totalBytes: totalLen,
        headerBytes: headerLen,
        hasHeaders: true,
      );
    }

    _readState = _ReadState.readingBody;
    _pendingMessage!.buffer = Uint8List(_pendingMessage!.totalBytes);
  }

  void _handleBodyChunk(Uint8List chunk) {
    final pending = _pendingMessage!;
    final needed = pending.totalBytes - pending.received;
    final take = chunk.length < needed ? chunk.length : needed;

    pending.buffer.setRange(pending.received, pending.received + take, chunk);
    pending.received += take;

    if (pending.received >= pending.totalBytes) {
      _readState = _ReadState.idle;
      _pendingMessage = null;
      _dispatchMessage(pending);

      // Any leftover bytes after the body (plus trailing \r\n) go back through
      // line processing.
      if (chunk.length > take) {
        final leftover = chunk.sublist(take);
        _handleData(leftover);
      }
    }
  }

  void _dispatchMessage(_PendingMessage pending) {
    final sub = _subscribers[pending.sid];
    if (sub == null) return;

    Map<String, List<String>> headers = const {};

    if (pending.hasHeaders) {
      final headerText = utf8.decode(
        pending.buffer.sublist(0, pending.headerBytes),
        allowMalformed: true,
      );
      headers = _parseHeaders(headerText);
    }

    final payloadStart = pending.hasHeaders ? pending.headerBytes : 0;
    final payload = Uint8List.fromList(
      pending.buffer.sublist(payloadStart),
    );

    final msg = Message(
      subject: pending.subject,
      sid: pending.sid,
      replyTo: pending.replyTo,
      headers: headers,
      payload: payload,
    );
    sub.deliver(msg);
  }

  static Map<String, List<String>> _parseHeaders(String headerText) {
    final result = <String, List<String>>{};
    final lines = headerText.split('\r\n');
    // lines[0] is the status line (e.g. "NATS/1.0"), skip it.
    for (var i = 1; i < lines.length; i++) {
      final line = lines[i];
      final colon = line.indexOf(':');
      if (colon == -1) continue;
      final name = line.substring(0, colon).trim();
      final value = line.substring(colon + 1).trim();
      result.putIfAbsent(name, () => []).add(value);
    }
    return result;
  }

  // ── internal factory ──────────────────────────────────────────────────────

  static Future<Client> create(
    Socket socket, {
    String? name,
    String? user,
    String? password,
    String? authToken,
    DisconnectedCallback? onDisconnected,
  }) async {
    final client = Client._(socket);
    client.onDisconnected = onDisconnected;

    final completer = Completer<void>();

    // Buffer incoming data until the initial INFO is received before we
    // finish the handshake.
    late StreamSubscription<Uint8List> sub;
    sub = socket.cast<Uint8List>().listen(
      (data) {
        client._handleData(data);
        if (!completer.isCompleted && client._serverInfo != null) {
          completer.complete();
        }
      },
      onError: (Object error) {
        if (!completer.isCompleted) {
          completer.completeError(NatsError('Socket error: $error'));
        } else if (!client._closed) {
          client._closed = true;
          client._closeAllSubscribers();
          client.onDisconnected?.call(client);
        }
      },
      onDone: () {
        if (!completer.isCompleted) {
          completer.completeError(NatsError('Connection closed before INFO'));
        } else if (!client._closed) {
          client._closed = true;
          client._closeAllSubscribers();
          client.onDisconnected?.call(client);
        }
        sub.cancel();
      },
      cancelOnError: false,
    );

    // Wait for the INFO message.
    await completer.future;

    // Send CONNECT.
    final connectOpts = <String, dynamic>{
      'verbose': false,
      'pedantic': false,
      'lang': 'dart',
      'version': '0.1.0',
      'protocol': 1,
      'headers': true,
    };
    if (name != null) connectOpts['name'] = name;
    if (user != null) connectOpts['user'] = user;
    if (password != null) connectOpts['pass'] = password;
    if (authToken != null) connectOpts['auth_token'] = authToken;

    socket.write('CONNECT ${jsonEncode(connectOpts)}\r\n');

    return client;
  }
}

// ── internal helpers ──────────────────────────────────────────────────────────

enum _ReadState { idle, readingBody }

class _PendingMessage {
  final String subject;
  final String sid;
  final String? replyTo;
  final int totalBytes;
  final int headerBytes;
  final bool hasHeaders;

  late Uint8List buffer;
  int received = 0;

  _PendingMessage({
    required this.subject,
    required this.sid,
    this.replyTo,
    required this.totalBytes,
    required this.headerBytes,
    required this.hasHeaders,
  });
}
