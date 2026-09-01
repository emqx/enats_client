#!/usr/bin/env escript
%%! -pa _build/default/lib/enats_client/ebin -pa _build/default/lib/jiffy/ebin

main(Args) ->
    {Count, Port, Diagnostics} = case Args of
        [CountText, PortText, "on"] -> {list_to_integer(CountText), list_to_integer(PortText), true};
        [CountText, PortText] -> {list_to_integer(CountText), list_to_integer(PortText), false};
        [CountText] -> {list_to_integer(CountText), 4222, false};
        _ -> {10000, 4222, false}
    end,
    application:ensure_all_started(enats_client),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port}),
    maybe_enable_diagnostics(Client, Diagnostics),
    ok = enats_client:connect(Client),
    StartedAt = erlang:monotonic_time(microsecond),
    publish_many(Client, Count),
    ok = enats_client:flush(Client, 10000),
    Duration = erlang:monotonic_time(microsecond) - StartedAt,
    io:format("count=~p duration_us=~p throughput_msg_s=~.2f~n", [
        Count, Duration, Count * 1000000 / max(Duration, 1)
    ]),
    maybe_print_diagnostics(Client, Diagnostics),
    enats_client:stop(Client).

maybe_enable_diagnostics(_Client, false) -> ok;
maybe_enable_diagnostics(Client, true) ->
    enats_client:enable_diagnostics(Client, #{message_sample_every => 100}).

maybe_print_diagnostics(_Client, false) -> ok;
maybe_print_diagnostics(Client, true) ->
    {ok, Snapshot} = enats_client:diagnostics(Client),
    io:format("diagnostics=~p~n", [Snapshot]).

publish_many(_Client, 0) -> ok;
publish_many(Client, Count) ->
    ok = enats_client:publish(Client, <<"benchmark.events">>, <<"payload">>),
    publish_many(Client, Count - 1).
