import 'dart:convert';

/// Information sent by the server in the initial INFO message.
class ServerInfo {
  final String serverId;
  final String? serverName;
  final String version;
  final int proto;
  final String? host;
  final int port;
  final bool headersSupported;
  final bool authRequired;
  final bool tlsRequired;
  final int maxPayload;
  final List<String> connectUrls;
  final bool jetstream;

  const ServerInfo({
    required this.serverId,
    this.serverName,
    required this.version,
    required this.proto,
    this.host,
    required this.port,
    required this.headersSupported,
    required this.authRequired,
    required this.tlsRequired,
    required this.maxPayload,
    required this.connectUrls,
    required this.jetstream,
  });

  factory ServerInfo.fromJson(String jsonStr) {
    final map = jsonDecode(jsonStr) as Map<String, dynamic>;
    return ServerInfo(
      serverId: map['server_id'] as String? ?? '',
      serverName: map['server_name'] as String?,
      version: map['version'] as String? ?? '',
      proto: map['proto'] as int? ?? 0,
      host: map['host'] as String?,
      port: map['port'] as int? ?? 4222,
      headersSupported: map['headers'] as bool? ?? false,
      authRequired: map['auth_required'] as bool? ?? false,
      tlsRequired: map['tls_required'] as bool? ?? false,
      maxPayload: map['max_payload'] as int? ?? 1048576,
      connectUrls: (map['connect_urls'] as List<dynamic>?)
              ?.map((e) => e as String)
              .toList() ??
          [],
      jetstream: map['jetstream'] as bool? ?? false,
    );
  }
}
