#!/usr/bin/env escript
%%! -pa _build/default/lib/enats_client/ebin -pa _build/default/lib/jiffy/ebin

main(Args) ->
    {Mode, Port, Count, Size, Diagnostics} = parse_args(Args),
    application:ensure_all_started(enats_client),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port}),
    maybe_enable_diagnostics(Client, Diagnostics),
    ok = enats_client:connect(Client),
    run(Mode, Client, Count, Size),
    ok = enats_client:stop(Client).

parse_args([ModeText, PortText, CountText, SizeText, "on"]) ->
    {parse_mode(ModeText), list_to_integer(PortText), list_to_integer(CountText),
        list_to_integer(SizeText), true};
parse_args([ModeText, PortText, CountText, SizeText]) ->
    {parse_mode(ModeText), list_to_integer(PortText), list_to_integer(CountText),
        list_to_integer(SizeText), false};
parse_args([CountText, PortText, "on"]) ->
    {direct, list_to_integer(PortText), list_to_integer(CountText), 7, true};
parse_args([CountText, PortText]) ->
    {direct, list_to_integer(PortText), list_to_integer(CountText), 7, false};
parse_args([CountText]) ->
    {direct, 4222, list_to_integer(CountText), 7, false};
parse_args(_) ->
    {direct, 4222, 10000, 7, false}.

parse_mode("direct") -> direct;
parse_mode("pubsync") -> pubsync;
parse_mode("request") -> request;
parse_mode("pubsub") -> pubsub;
parse_mode("pubsubsync") -> pubsubsync;
parse_mode("batch") -> batch;
parse_mode("jetstream") -> jetstream;
parse_mode("jetstream_msgid") -> jetstream_msgid;
parse_mode(Value) -> erlang:error({invalid_mode, Value}).

maybe_enable_diagnostics(_Client, false) -> ok;
maybe_enable_diagnostics(Client, true) ->
    ok = enats_client:enable_diagnostics(Client, #{message_sample_every => 100}).

run(direct, Client, Count, Size) ->
    Payload = binary:copy(<<"x">>, Size),
    Started = now_us(),
    publish_many(Client, <<"benchmark.events">>, Payload, Count),
    ok = enats_client:flush(Client, 10000),
    report(direct, Count, now_us() - Started, []);
run(pubsync, Client, Count, Size) ->
    Payload = binary:copy(<<"x">>, Size),
    Started = now_us(),
    publish_sync(Client, <<"benchmark.events">>, Payload, Count),
    report(pubsync, Count, now_us() - Started, []);
run(batch, Client, Count, Size) ->
    Payload = binary:copy(<<"x">>, Size),
    Started = now_us(),
    publish_batches(Client, <<"benchmark.events">>, Payload, Count, benchmark_batch_size()),
    ok = enats_client:flush(Client, 10000),
    report(batch, Count, now_us() - Started, []);
run(request, Client, Count, Size) ->
    Responder = spawn(fun() -> responder_loop(Client) end),
    {ok, _} = enats_client:subscribe(Client, <<"benchmark.request">>, #{owner => Responder}),
    ok = enats_client:flush(Client, 1000),
    Payload = binary:copy(<<"x">>, Size),
    Started = now_us(),
    Latencies = request_many(Client, Payload, Count, []),
    Responder ! stop,
    report(request, Count, now_us() - Started, Latencies);
run(pubsub, Client, Count, Size) ->
    {ok, _} = enats_client:subscribe(Client, <<"benchmark.pubsub">>, #{}),
    ok = enats_client:flush(Client, 1000),
    Started = now_us(),
    publish_timed(Client, Count, Size),
    ok = enats_client:flush(Client, 10000),
    Latencies = receive_messages(Count, []),
    report(pubsub, Count, now_us() - Started, Latencies);
run(pubsubsync, Client, Count, Size) ->
    {ok, _} = enats_client:subscribe(Client, <<"benchmark.pubsubsync">>, #{}),
    ok = enats_client:flush(Client, 1000),
    Started = now_us(),
    Latencies = publish_receive_sync(Client, Count, Size, []),
    report(pubsubsync, Count, now_us() - Started, Latencies);
run(Mode, Client, Count, Size) when Mode =:= jetstream; Mode =:= jetstream_msgid ->
    Suffix = integer_to_binary(erlang:unique_integer([positive])),
    Stream = <<"BENCH_", Suffix/binary>>,
    Subject = <<"benchmark.js.", Suffix/binary>>,
    Config = jiffy:encode(#{<<"name">> => Stream, <<"subjects">> => [Subject]}),
    {ok, _} = enats_client:request(
        Client, <<"$JS.API.STREAM.CREATE.", Stream/binary>>, Config, #{timeout => 5000}
    ),
    Payload = binary:copy(<<"x">>, Size),
    _ = enats_client:jetstream_publish(Client, Subject, Payload, #{}),
    Started = now_us(),
    Latencies = jetstream_many(Mode, Client, Subject, Payload, Count, []),
    report(Mode, Count, now_us() - Started, Latencies).

publish_many(_Client, _Subject, _Payload, 0) -> ok;
publish_many(Client, Subject, Payload, Count) ->
    ok = enats_client:publish(Client, Subject, Payload),
    publish_many(Client, Subject, Payload, Count - 1).

publish_sync(_Client, _Subject, _Payload, 0) -> ok;
publish_sync(Client, Subject, Payload, Count) ->
    ok = enats_client:publish(Client, Subject, Payload),
    ok = enats_client:flush(Client, 10000),
    publish_sync(Client, Subject, Payload, Count - 1).

publish_batches(_Client, _Subject, _Payload, 0, _BatchSize) -> ok;
publish_batches(Client, Subject, Payload, Count, BatchSize) ->
    Size = min(Count, BatchSize),
    Batch = [#{subject => Subject, payload => Payload} || _ <- lists:seq(1, Size)],
    ok = enats_client:publish_batch(Client, Batch),
    publish_batches(Client, Subject, Payload, Count - Size, BatchSize).

publish_timed(_Client, 0, _Size) -> ok;
publish_timed(Client, Count, Size) ->
    Timestamp = erlang:system_time(nanosecond),
    Payload = <<Timestamp:64/unsigned-big, (binary:copy(<<"x">>, max(Size - 8, 0)))/binary>>,
    ok = enats_client:publish(Client, <<"benchmark.pubsub">>, Payload),
    publish_timed(Client, Count - 1, Size).

publish_receive_sync(_Client, 0, _Size, Acc) -> lists:sort(Acc);
publish_receive_sync(Client, Count, Size, Acc) ->
    Timestamp = erlang:system_time(nanosecond),
    Payload = <<Timestamp:64/unsigned-big, (binary:copy(<<"x">>, max(Size - 8, 0)))/binary>>,
    ok = enats_client:publish(Client, <<"benchmark.pubsubsync">>, Payload),
    ok = enats_client:flush(Client, 10000),
    receive
        {enats_client, _Client, {message, #{payload := <<Timestamp:64/unsigned-big, _/binary>>}}} ->
            LatencyUs = (erlang:system_time(nanosecond) - Timestamp) div 1000,
            publish_receive_sync(Client, Count - 1, Size, [LatencyUs | Acc])
    after 10000 ->
        erlang:error({message_timeout, Count})
    end.

request_many(_Client, _Payload, 0, Acc) -> lists:reverse(Acc);
request_many(Client, Payload, Count, Acc) ->
    Started = now_us(),
    {ok, _} = enats_client:request(Client, <<"benchmark.request">>, Payload, #{timeout => 5000}),
    request_many(Client, Payload, Count - 1, [now_us() - Started | Acc]).

responder_loop(Client) ->
    receive
        {enats_client, Client, {message, #{reply_to := ReplyTo, payload := Payload}}} ->
            ok = enats_client:publish(Client, ReplyTo, Payload),
            responder_loop(Client);
        stop -> ok
    end.

receive_messages(0, Acc) -> lists:sort(Acc);
receive_messages(Count, Acc) ->
    receive
        {enats_client, _Client, {message, #{payload := <<Timestamp:64/unsigned-big, _/binary>>}}} ->
            LatencyUs = (erlang:system_time(nanosecond) - Timestamp) div 1000,
            receive_messages(Count - 1, [LatencyUs | Acc])
    after 10000 ->
        erlang:error({message_timeout, Count})
    end.

jetstream_many(_Mode, _Client, _Subject, _Payload, 0, Acc) -> lists:sort(Acc);
jetstream_many(Mode, Client, Subject, Payload, Count, Acc) ->
    Started = now_us(),
    Options = case Mode of
        jetstream -> #{timeout => 5000};
        jetstream_msgid -> #{timeout => 5000, msg_id => integer_to_binary(Count)}
    end,
    {ok, _} = enats_client:jetstream_publish(Client, Subject, Payload, Options),
    jetstream_many(Mode, Client, Subject, Payload, Count - 1, [now_us() - Started | Acc]).

report(Mode, Count, DurationUs, []) ->
    io:format("mode=~p count=~p duration_us=~p throughput_msg_s=~.2f~n", [
        Mode, Count, DurationUs, Count * 1000000 / max(DurationUs, 1)
    ]);
report(Mode, Count, DurationUs, Latencies) ->
    Sorted = lists:sort(Latencies),
    io:format("mode=~p count=~p throughput_msg_s=~.2f p50_us=~p p90_us=~p p95_us=~p p99_us=~p~n", [
        Mode, Count, Count * 1000000 / max(DurationUs, 1), percentile(Sorted, 50),
        percentile(Sorted, 90), percentile(Sorted, 95), percentile(Sorted, 99)
    ]).

percentile(Values, Percent) ->
    Index = max(1, (length(Values) * Percent + 99) div 100),
    lists:nth(Index, Values).

now_us() -> erlang:monotonic_time(microsecond).

benchmark_batch_size() ->
    case os:getenv("ENATS_BENCHMARK_BATCH_SIZE") of
        false -> 256;
        Value ->
            case list_to_integer(Value) of
                Size when Size > 0 -> Size;
                Size -> erlang:error({invalid_batch_size, Size})
            end
    end.
