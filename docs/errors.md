# Errors

Public calls return `{error, Reason}` for invalid arguments, transport and
TLS failures, protocol errors, timeouts, server errors and JetStream
rejections. The client process is not terminated by malformed user input.

`publish` confirms only a successful socket write. `flush` confirms a NATS
PING/PONG barrier. A Core NATS publish may be uncertain after a transport
failure and is not automatically replayed.
