#!/usr/bin/env escript
%%! -pa _build/default/lib/enats_client/ebin -pa _build/default/lib/jiffy/ebin

main(Args) ->
    {Count, Port} = case Args of
        [CountText, PortText] -> {list_to_integer(CountText), list_to_integer(PortText)};
        [CountText] -> {list_to_integer(CountText), 4222};
        _ -> {10000, 4222}
    end,
    application:ensure_all_started(enats_client),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port}),
    ok = enats_client:enable_diagnostics(Client, #{message_sample_every => 1}),
    ok = enats_client:connect(Client),
    StartedAt = erlang:monotonic_time(microsecond),
    publish_many(Client, Count),
    ok = enats_client:flush(Client, 10000),
    Duration = erlang:monotonic_time(microsecond) - StartedAt,
    {ok, Snapshot} = enats_client:diagnostics(Client),
    io:format("count=~p duration_us=~p throughput_msg_s=~.2f~n", [
        Count, Duration, Count * 1000000 / max(Duration, 1)
    ]),
    io:format("diagnostics=~p~n", [Snapshot]),
    enats_client:stop(Client).

publish_many(_Client, 0) -> ok;
publish_many(Client, Count) ->
    ok = enats_client:publish(Client, <<"benchmark.events">>, <<"payload">>),
    publish_many(Client, Count - 1).
