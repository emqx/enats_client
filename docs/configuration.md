# Configuration

`enats_client:start_link/1` accepts a map. The main options are `host`,
`port`, `servers`, `tls`, `tls_handshake`, `ssl_opts`, `auth`,
`connect_timeout`, `reconnect`, `reconnect_delay`, `socket_active_n` and
`slow_consumer_limit`.

`socket_active_n` is the bounded number of socket messages delivered before
the client re-arms the socket. `slow_consumer_limit` is the owner mailbox
watermark; a subscription over the watermark is unsubscribed and emits a
`slow_consumer` event.
