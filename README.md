# enats_client

`enats_client` is a small Erlang/OTP client for the NATS Core protocol.

The public interface is `enats_client`. It provides TCP, STARTTLS and
TLS-first transport, Core NATS publish/subscribe, headers, flush barriers,
request/reply, reconnect and server failover, user/password, token, NKey,
JWT and standard `.creds` authentication.

JetStream publish is supported through `jetstream_publish/4`. It waits for a
JetStream PubAck and accepts an optional stable `msg_id` for server-side
deduplication. WebSocket transport and client-side persistent buffering are
outside the current scope.

## Basic example

```erlang
{ok, Client} = enats_client:start_link(#{
    host => "127.0.0.1",
    port => 4222,
    auth => #{
        mechanism => user_password,
        username => <<"alice">>,
        password => fun() -> get_password() end
    }
}),
ok = enats_client:connect(Client),
ok = enats_client:publish(Client, <<"events.created">>, <<"payload">>),
ok = enats_client:flush(Client, 1000),
ok = enats_client:stop(Client).
```

`publish/3,4` reports a successful socket write. It does not imply
persistence or subscriber delivery. `flush/2` is a PING/PONG barrier and
confirms that the server processed earlier protocol messages.

## Authentication and TLS

Secret providers are evaluated on every connection and reconnect. Resolved
secret values are not retained in client state when a provider function is
used. Use `enats_auth:credentials_file/1` for standard `.creds` files; the
file is validated at startup and read again on reconnect while only its path
is retained in state.

TLS is secure by default when `tls => true`: peer verification, system CA
certificates and hostname verification are enabled. `verify_none` is only
available as an explicit development/test option.

The two supported handshake modes are `starttls` (the default) and `first`.

## Runtime diagnostics

Diagnostics are disabled by default and add no message-path timestamp or
histogram work while disabled. Enable them only during an investigation:

```erlang
ok = enats_client:enable_diagnostics(Client, #{message_sample_every => 100}),
{ok, Snapshot} = enats_client:diagnostics(Client),
ok = enats_client:disable_diagnostics(Client).
```

The snapshot contains fixed-memory counters and approximate P50/P90/P95/P99
latencies for transport connect, NATS connect, total connect, publish,
request, JetStream publish and message delivery. Latencies use monotonic time
and are reported in microseconds. Message delivery latency is measured from
socket receipt to delivery to the owner; it is not business end-to-end latency.

## Ingress and reconnect

Socket ingress uses a bounded `{active, N}` mode. Configure the batch size with
`socket_active_n` (default `100`) to balance throughput and mailbox safety.
`slow_consumer_limit` protects subscription owners from unbounded mailbox
growth and removes a subscription that exceeds the limit.

Reconnect does not buffer or replay Core NATS publishes. Use `drain/2` for a
graceful shutdown: it unsubscribes, waits for a flush barrier and closes the
connection.

## Tests and development

The test suite uses temporary `nats-server` instances for authentication,
TLS and JetStream integration paths. Install `nats-server` 2.10 or 2.11 for
the full suite.

```text
make test
make coverage
make static_checks
```

The supported CI baseline is Erlang/OTP 27 and 28. The package uses `jiffy`
2.0.1 for JSON encoding and decoding.
