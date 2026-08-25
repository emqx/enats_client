# enats_client

enats_client is an Erlang/OTP client for the Core NATS protocol.

The public entry point is the enats_client module. It provides connection
lifecycle management, TCP/TLS transport, Core NATS publish/subscribe,
headers, flush barriers, reconnect, server-list failover, NKey,
username/password, and token authentication.

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

publish/3 reports a successful socket write. It does not imply persistence
or subscriber delivery. flush/2 uses a PING/PONG barrier to confirm that
the server processed earlier protocol messages.

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
