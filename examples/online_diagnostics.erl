-module(online_diagnostics).

-export([snapshot/1]).

snapshot(Client) ->
    ok = enats_client:enable_diagnostics(Client, #{message_sample_every => 100}),
    enats_client:diagnostics(Client).
