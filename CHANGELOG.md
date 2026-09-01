# Changelog

## 0.1.12

Add batch publishing with opt-in admission limits, optimize subscription
routing, parser ingress and disabled diagnostics, and expose reproducible
benchmark modes.

## 0.1.11

This release consolidates the authentication implementation, switches JSON
encoding to `jiffy 2.0.1`, hardens public input handling, adds bounded socket
ingress, request inbox multiplexing, runtime diagnostics and graceful drain.

The authentication helper modules were merged into `enats_auth`. The public
error taxonomy and TLS defaults changed; review the README before upgrading.
