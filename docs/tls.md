# TLS

`tls => true` enables peer and hostname verification using system CA
certificates. Use `tls_handshake => first` for a NATS server configured with
`tls.handshake_first: true`. `verify_none` is intended only for local tests
and must be explicitly supplied in `ssl_opts`.
