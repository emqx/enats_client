# enats_client

enats_client is an Erlang/OTP client for the Core NATS protocol.

The public entry point is the enats_client module. It provides connection
lifecycle management, TCP/TLS transport, Core NATS publish/subscribe,
headers, flush barriers, reconnect, server-list failover, NKey,
username/password, token, JWT, and `.creds` authentication.

JetStream publish is supported through `jetstream_publish/4`. It waits for
the JetStream PubAck and accepts an optional stable `msg_id` for server-side
deduplication. WebSocket transport and client-side persistent buffering are
outside the current scope.

## Example

    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => 4222,
        auth => #{
            mechanism => user_password,
            username => <<"alice">>,
            password => fun() -> get_password() end
        }
    }).

    ok = enats_client:connect(Client),
    ok = enats_client:publish(Client, <<"events.created">>, <<"payload">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client).

Secret providers are evaluated on every connection and reconnect. Resolved
secret values are not retained in client state.

For standard `.creds` authentication, use
`enats_credentials:from_file/1` and pass the returned authentication map to
the client. The file is read and parsed on every connection and reconnect;
only its path is retained in client state. `enats_credentials:from_binary/1`
is a one-shot validator for inline credentials; callers that need inline
credentials for reconnects must provide their own lazy provider.

The credentials parser follows the standard NATS `.creds` format, including
the JWT and user NKey seed blocks. TLS is strict when enabled:
the client never silently downgrades to plaintext. By default, the client
uses the NATS `starttls` handshake, where it receives the server `INFO`
before upgrading the connection. NATS servers configured with
`tls.handshake_first: true` require the TLS handshake to happen before `INFO`;
use `tls_handshake => first` for those servers:

    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => 4222,
        tls => true,
        tls_handshake => first,
        ssl_opts => [{verify, verify_none}]
    }),

`tls_handshake => starttls` and `tls_handshake => first` are the two supported
modes. The mode must match the server configuration; the client does not
downgrade a TLS-first connection to plaintext.

publish/3 reports a successful socket write. It does not imply persistence
or subscriber delivery. flush/2 uses a PING/PONG barrier to confirm that
the server processed earlier protocol messages. Concurrent flush/2 calls are
supported; each call waits for the PONG corresponding to its own PING.

`jetstream_publish/4` reports the JetStream PubAck. Use a stable `msg_id`
when retrying a publish after a connection failure. Core NATS publish may be
replayed by the caller after an uncertain failure; the client deliberately
does not claim exactly-once delivery for Core NATS.

## Tests

The integration suite starts temporary nats-server instances for token and
NKey authentication. It also supports externally started authenticated and
TLS servers:

    nats-server -a 127.0.0.1 -p 14222
    nats-server -a 127.0.0.1 -p 14223 --user alice --pass secret
    nats-server -a 127.0.0.1 -p 14224 --tls --tlscert server.crt --tlskey server.key

    ENATS_AUTH_PORT=14223 ENATS_TLS_PORT=14224 make test

make coverage runs the same suite with OTP cover and requires total source
coverage above 80%.
