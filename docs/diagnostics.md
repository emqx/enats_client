# Diagnostics

Diagnostics are disabled by default. Enable them with
`enable_diagnostics/2`, read a fixed-memory snapshot with `diagnostics/1`,
and turn them off with `disable_diagnostics/1`.

Snapshots contain counters and approximate P50/P90/P95/P99 microsecond
latencies for transport connect, NATS connect, total connect, publish,
request, JetStream publish and owner delivery. Message metrics use the
configured deterministic sampling interval and do not retain payloads,
subjects or credentials.

Counters include connection attempts/failures/reconnects, started and timed
out requests, slow consumers, protocol/transport errors, and messages in/out.
All counters and histograms are held in bounded in-memory maps and are reset
with `reset_diagnostics/1`.

For a local throughput A/B check, run `scripts/benchmark.escript 10000 4222`
with diagnostics disabled and add the third argument `on` to enable the
default one-in-one-hundred message sampling.
