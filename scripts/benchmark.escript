#!/usr/bin/env escript
%%! -pa _build/default/lib/enats_client/ebin -pa _build/default/lib/jiffy/ebin

main(Args) ->
    {Mode, Count, Port, Diagnostics} = case Args of
        [ModeText, CountText, PortText, "on"] ->
            {list_to_atom(ModeText), list_to_integer(CountText), list_to_integer(PortText), true};
        [ModeText, CountText, PortText] when ModeText =:= "direct";
            ModeText =:= "batch" ->
            {list_to_atom(ModeText), list_to_integer(CountText), list_to_integer(PortText), false};
        [CountText, PortText, "on"] -> {direct, list_to_integer(CountText), list_to_integer(PortText), true};
        [CountText, PortText] -> {direct, list_to_integer(CountText), list_to_integer(PortText), false};
        [CountText] -> {direct, list_to_integer(CountText), 4222, false};
        _ -> {direct, 10000, 4222, false}
    end,
    application:ensure_all_started(enats_client),
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => Port
    }),
    maybe_enable_diagnostics(Client, Diagnostics),
    ok = enats_client:connect(Client),
    StartedAt = erlang:monotonic_time(microsecond),
    publish_many(Mode, Client, Count),
    ok = enats_client:flush(Client, 10000),
    Duration = erlang:monotonic_time(microsecond) - StartedAt,
    io:format("mode=~p count=~p duration_us=~p throughput_msg_s=~.2f~n", [
        Mode, Count, Duration, Count * 1000000 / max(Duration, 1)
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

publish_many(direct, _Client, 0) -> ok;
publish_many(direct, Client, Count) ->
    ok = enats_client:publish(Client, <<"benchmark.events">>, <<"payload">>),
    publish_many(direct, Client, Count - 1);
publish_many(batch, _Client, 0) ->
    ok;
publish_many(batch, Client, Count) ->
    BatchSize = min(Count, 256),
    Batch = [#{subject => <<"benchmark.events">>, payload => <<"payload">>} || _ <- lists:seq(1, BatchSize)],
    ok = enats_client:publish_batch(Client, Batch),
    publish_many(batch, Client, Count - BatchSize).
