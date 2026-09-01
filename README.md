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

For higher throughput, `publish_batch/2,3` writes a bounded list of messages in
one socket operation while preserving the same successful-write guarantee.

Core NATS is at-most-once from the client's perspective. The client does not
persist offline messages and does not claim exactly-once delivery. Use
JetStream publish with a stable `msg_id` when server-side deduplication is
required.

## Configuration

`enats_client:start_link/1` accepts a map. The main options are `host`,
`port`, `servers`, `tls`, `tls_handshake`, `ssl_opts`, `auth`,
`connect_timeout`, `reconnect`, `reconnect_delay`, `socket_active_n`,
`slow_consumer_limit`, `max_control_line`, `max_message_size`,
`max_parser_buffer`, `max_publish_batch_messages` and
`max_publish_batch_bytes`.

Parser limits default to 4 KiB for a control line and 8 MiB for a message or
the aggregate parser buffer. They can be lowered or raised explicitly after
considering the server's `max_payload` setting. `reconnect` may also be a map
with `min_delay`, `max_delay`, `multiplier`, `jitter` and `max_attempts`.

Socket ingress uses a bounded `{active, N}` mode. Configure the batch size with
`socket_active_n` (default `100`) to balance throughput and mailbox safety.
`slow_consumer_limit` is the owner mailbox watermark; a subscription over the
watermark is unsubscribed and emits a `slow_consumer` event.

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

## Delivery semantics

`drain/2` rejects new work, unsubscribes user subscriptions, allows in-flight
requests to finish, and waits for the final PING/PONG barrier. A finite drain
timeout fails remaining requests and closes the transport; a transport failure
during drain never starts reconnect.

## Performance reference

The following local comparison uses `nats-server 2.11.6`, Erlang/OTP 27,
`nats.go v1.53.1`, 5,000 messages per run, and the median of three runs on
the same host. Core NATS and JetStream use separate server instances.

For Core NATS, `pub` is direct publish followed by one final flush. The Go
client buffers `Publish` calls internally, so this is not an apples-to-apples
comparison; use `pubsync`, request/reply, or synchronous pub/sub for the
closer comparison.

### Core NATS, 128B payload

| Scenario | enats | nats.go | enats / Go |
| --- | ---: | ---: | ---: |
| Direct publish + final flush | 48.4k msg/s | 3.26M msg/s | 1/67 |
| Publish + flush per message | 9.37k msg/s | 13.40k msg/s | 70% |
| Request/reply | 5.23k msg/s | 6.20k msg/s | 84% |
| Burst pub/sub | 25.9k msg/s | 174.4k msg/s | 15% |
| Synchronous pub/sub | 8.19k msg/s | 11.98k msg/s | 68% |
| `publish_batch` | 384k msg/s | — | 8x direct enats |

Request latency was p50/p99 `187/241 us` for enats and `156/194 us` for
nats.go. Synchronous pub/sub latency was `118/154 us` and `79/102 us`,
respectively. Burst pub/sub latency includes queueing while the publisher is
still producing messages and is not a pure network latency measurement.

### Core NATS, 4KiB payload

| Scenario | enats | nats.go | enats / Go |
| --- | ---: | ---: | ---: |
| Direct publish + final flush | 40.4k msg/s | 368k msg/s | 1/9.1 |
| Publish + flush per message | 9.77k msg/s | 13.14k msg/s | 74% |
| Request/reply | 4.05k msg/s | 5.94k msg/s | 68% |
| Burst pub/sub | 10.9k msg/s | 118.8k msg/s | 9% |
| Synchronous pub/sub | 6.39k msg/s | 11.40k msg/s | 56% |
| `publish_batch` | 190k msg/s | — | 4.7x direct enats |

### JetStream PubAck

Each run created a dedicated stream, warmed up with one publish, and measured
the synchronous PubAck request path over 2,000 messages.

| Payload / scenario | enats | nats.go | enats / Go |
| --- | ---: | ---: | ---: |
| 128B, no message ID | 7.86k msg/s | 8.99k msg/s | 87.5% |
| 128B, unique `msg_id` | 7.46k msg/s | 8.59k msg/s | 86.9% |
| 4KiB, no message ID | 7.20k msg/s | 8.13k msg/s | 88.6% |
| 4KiB, unique `msg_id` | 7.33k msg/s | 7.90k msg/s | 92.8% |

JetStream p50/p99 latency ranged from `124/164 us` to `138/182 us` for enats,
and from `110/141 us` to `125/164 us` for nats.go. Existing integration tests
also verify duplicate `msg_id` handling: the duplicate publish returns the
same sequence with `duplicate => true`.

These figures are reference measurements rather than fixed hardware SLAs;
rerun the benchmark on an isolated, CPU-pinned host before making capacity
decisions.

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

Counters include connection attempts/failures/reconnects, started and timed
out requests, slow consumers, protocol/transport errors, and messages in/out.
All counters and histograms are held in bounded in-memory maps and are reset
with `reset_diagnostics/1`.

For a local throughput check, run `scripts/benchmark.escript 10000 4222`
(`direct` is the default), or select `batch` with
`scripts/benchmark.escript batch 10000 4222`. Add a final `on` argument to
enable the default one-in-one-hundred message sampling.

Reconnect does not buffer or replay Core NATS publishes. Use `drain/2` for a
graceful shutdown: it unsubscribes, waits for a flush barrier and closes the
connection.

## Errors

Public calls return `{error, Reason}` for invalid arguments, transport and
TLS failures, protocol errors, timeouts, server errors and JetStream
rejections. The client process is not terminated by malformed user input.

Unknown option keys are rejected with
`{error, {invalid_option, Scope, {unknown_keys, Keys}}}`. This prevents a
misspelled TLS, reconnect, subscription or diagnostics setting from being
silently ignored.

## Tests and development

The test suite uses temporary `nats-server` instances for authentication,
TLS and JetStream integration paths. Install `nats-server` 2.10 or 2.11 for
the full suite.

```text
make test
make coverage
make static_checks
```

`make specs` runs `scripts/check_specs.escript`, which verifies that every
exported source function has an Erlang `-spec`. The benchmark script is a
developer-only throughput and diagnostics smoke test; neither script is part
of the Hex package.

The supported CI baseline is Erlang/OTP 27 and 28. The package uses `jiffy`
2.0.1 for JSON encoding and decoding.
