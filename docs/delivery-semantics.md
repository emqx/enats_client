# Delivery semantics

Core NATS is at-most-once from the client's perspective. The client does not
persist offline messages and does not claim exactly-once delivery. Use
JetStream publish with a stable `msg_id` when server-side deduplication is
required.

`publish/3` reports a successful socket write, not durable persistence or
subscriber delivery. Use `flush/2` when the server-side protocol barrier is
required.

`drain/2` rejects new work, unsubscribes user subscriptions, allows in-flight
requests to finish, and waits for the final PING/PONG barrier. A finite drain
timeout fails remaining requests and closes the transport; a transport failure
during drain never starts reconnect.
