# Configuration

`enats_client:start_link/1` accepts a map. The main options are `host`,
`port`, `servers`, `tls`, `tls_handshake`, `ssl_opts`, `auth`,
`connect_timeout`, `reconnect`, `reconnect_delay`, `socket_active_n` and
`slow_consumer_limit`, `max_control_line`, `max_message_size` and
`max_parser_buffer`.

Parser limits default to 4 KiB for a control line and 8 MiB for a message or
the aggregate parser buffer. They can be lowered or raised explicitly after
considering the server's `max_payload` setting. `reconnect` may also be a map
with `min_delay`, `max_delay`, `multiplier`, `jitter` and `max_attempts`.

`socket_active_n` is the bounded number of socket messages delivered before
the client re-arms the socket. `slow_consumer_limit` is the owner mailbox
watermark; a subscription over the watermark is unsubscribed and emits a
`slow_consumer` event.
