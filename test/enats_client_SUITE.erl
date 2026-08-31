-module(enats_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_frame_fragmentation/1,
    t_frame_invalid/1,
    t_frame_edges/1,
    t_ipv6_connection/1,
    t_connect_publish_subscribe_flush/1,
    t_default_owner/1,
    t_connect_idempotence/1,
    t_coalesced_flush/1,
    t_server_limits/1,
    t_flush_concurrency/1,
    t_flush_timeout_preserves_pong_order/1,
    t_flush_disconnects_all_waiters/1,
    t_headers/1,
    t_frame_variants/1,
    t_auth_helpers/1,
    t_nkey_helpers/1,
    t_secret_and_subject/1,
    t_invalid_subject/1,
    t_lazy_secret/1,
    t_invalid_options/1,
    t_canonical_no_responders/1,
    t_invalid_headers/1,
    t_user_password/1,
    t_tls/1,
    t_connection_errors/1,
    t_connection_queries/1,
    t_fake_connection_paths/1,
    t_reconnect/1,
    t_stale_connection_reconnect/1,
    t_disconnect_while_connecting/1,
    t_topology_info/1,
    t_server_failover/1,
    t_server_failover_handshake/1,
    t_nkey_nats_server/1,
    t_token_nats_server/1,
    t_jetstream_nats_server/1,
    t_nkey_seed/1,
    t_credentials_file_provider/1,
    t_request_timeout_cleanup/1,
    t_jetstream_no_responders/1,
    t_jetstream_json_unavailable/1,
    t_tls_downgrade_rejected/1,
    t_tls_first/1,
    t_request_infinity/1,
    t_invalid_public_inputs/1,
    t_diagnostics/1,
    t_drain/1,
    t_parser_limits/1,
    t_slow_consumer/1,
    t_protocol_input_safety/1,
    t_drain_clears_state/1,
    t_connect_deadline/1,
    t_owner_down/1
]).

all() ->
    [
        t_frame_fragmentation,
        t_frame_invalid,
        t_frame_edges,
        t_ipv6_connection,
        t_connect_publish_subscribe_flush,
        t_default_owner,
        t_connect_idempotence,
        t_coalesced_flush,
        t_server_limits,
        t_flush_concurrency,
        t_flush_timeout_preserves_pong_order,
        t_flush_disconnects_all_waiters,
        t_headers,
        t_frame_variants,
        t_auth_helpers,
        t_nkey_helpers,
        t_secret_and_subject,
        t_invalid_subject,
        t_lazy_secret,
        t_invalid_options,
        t_canonical_no_responders,
        t_invalid_headers,
        t_user_password,
        t_tls,
        t_connection_errors,
        t_connection_queries,
        t_fake_connection_paths,
        t_reconnect,
        t_stale_connection_reconnect,
        t_disconnect_while_connecting,
        t_topology_info,
        t_server_failover,
        t_server_failover_handshake,
        t_nkey_nats_server,
        t_token_nats_server,
        t_jetstream_nats_server,
        t_nkey_seed,
        t_credentials_file_provider,
        t_request_timeout_cleanup,
        t_jetstream_no_responders,
        t_jetstream_json_unavailable,
        t_tls_downgrade_rejected,
        t_tls_first,
        t_request_infinity,
        t_invalid_public_inputs,
        t_diagnostics,
        t_drain,
        t_parser_limits,
        t_slow_consumer,
        t_protocol_input_safety,
        t_drain_clears_state,
        t_connect_deadline,
        t_owner_down
    ].

init_per_suite(Config) ->
    application:ensure_all_started(enats_client),
    case nats_server_executable() of
        {ok, _Executable} ->
            PrivDir = proplists:get_value(priv_dir, Config),
            Port = dynamic_port(),
            PidFile = filename:join(PrivDir, "nats-base.pid"),
            Server = start_nats_process([
                "-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile
            ]),
            wait_for_port(Port),
            [{port, Port}, {base_server, Server}, {base_pid_file, PidFile} | Config];
        unavailable ->
            [{port, 4222}, {base_server, undefined}, {base_pid_file, undefined} | Config]
    end.

end_per_suite(Config) ->
    maybe_stop_nats_server(?config(base_server, Config), ?config(base_pid_file, Config)),
    ok.

t_frame_fragmentation(_Config) ->
    Data = <<"INFO {\"proto\":1,\"headers\":true}\r\nPONG\r\n">>,
    {Frames1, State1} = enats_frame:parse(binary:part(Data, 0, 9), enats_frame:initial_state()),
    ?assertEqual([], Frames1),
    {Frames2, _State2} = enats_frame:parse(binary:part(Data, 9, byte_size(Data) - 9), State1),
    ?assertEqual([{info, #{<<"proto">> => 1, <<"headers">> => true}}, pong], Frames2).

t_frame_invalid(_Config) ->
    ?assertEqual(
        [{error, {unknown_frame, <<"BOGUS">>}}],
        element(1, enats_frame:parse(<<"BOGUS\r\n">>, enats_frame:initial_state()))
    ).

t_frame_edges(_Config) ->
    {Frames1, Pending} = enats_frame:parse(<<"MSG foo 1 5\r\nhe">>, enats_frame:initial_state()),
    ?assertEqual([], Frames1),
    {Frames2, _} = enats_frame:parse(<<"llo\r\n+OK\r\n">>, Pending),
    ?assertEqual(
        [#{type => msg, subject => <<"foo">>, sid => <<"1">>, payload => <<"hello">>}, ok], Frames2
    ),
    {Frames3, _} = enats_frame:parse(
        <<"MSG foo 1 reply 5\r\nhello\r\n">>, enats_frame:initial_state()
    ),
    ?assertEqual(
        [
            #{
                type => msg,
                subject => <<"foo">>,
                sid => <<"1">>,
                reply_to => <<"reply">>,
                payload => <<"hello">>
            }
        ],
        Frames3
    ),
    {Frames4, _} = enats_frame:parse(<<"HMSG foo 1 3 3\r\nbad\r\n">>, enats_frame:initial_state()),
    ?assertEqual(
        [
            #{
                type => hmsg,
                subject => <<"foo">>,
                sid => <<"1">>,
                header_size => 3,
                headers => [],
                payload => <<>>
            }
        ],
        Frames4
    ),
    {Frames5, _} = enats_frame:parse(
        <<"HMSG foo 1 26 26\r\nNATS/1.0\r\nX-Test:value\r\n\r\n\r\n">>,
        enats_frame:initial_state()
    ),
    ?assertEqual(
        [
            #{
                type => hmsg,
                subject => <<"foo">>,
                sid => <<"1">>,
                header_size => 26,
                headers => [{<<"X-Test">>, <<"value">>}],
                payload => <<>>
            }
        ],
        Frames5
    ),
    _ = enats_frame:serialize_connect(#{<<"binary-key">> => true}),
    ok.

t_ipv6_connection(_Config) ->
    {Server, Port} = start_fake_server(ipv6),
    {ok, Client} = enats_client:start_link(#{
        servers => [{{0, 0, 0, 0, 0, 0, 0, 1}, Port}],
        owner => self()
    }),
    ok = enats_client:connect(Client),
    ?assertEqual(connected, enats_client:status(Client)),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_invalid_options(_Config) ->
    ?assertEqual(
        {error, {invalid_option, tls_handshake, typo}},
        enats_client:start_link(#{tls => true, tls_handshake => typo})
    ),
    ?assertEqual(
        {error, {invalid_option, ping_interval, bad}},
        enats_client:start_link(#{ping_interval => bad})
    ),
    ?assertEqual(
        {error, {invalid_option, max_pings_out, 0}},
        enats_client:start_link(#{max_pings_out => 0})
    ),
    ?assertEqual(
        {error, {invalid_option, socket_active_n, 32768}},
        enats_client:start_link(#{socket_active_n => 32768})
    ),
    ?assertEqual({error, {invalid_option, tls, bad}}, enats_client:start_link(#{tls => bad})),
    ?assertEqual({error, {invalid_option, port, 0}}, enats_client:start_link(#{port => 0})),
    ?assertEqual({error, {invalid_option, owner, bad}}, enats_client:start_link(#{owner => bad})),
    ?assertEqual(
        {error, {invalid_option, jitter, 2}},
        enats_client:start_link(#{reconnect => #{jitter => 2}})
    ),
    ?assertEqual(
        {error, {invalid_option, reconnect_delay_range, {200, 100}}},
        enats_client:start_link(#{reconnect => #{min_delay => 200, max_delay => 100}})
    ),
    ?assertEqual(
        {error, {invalid_option, max_parser_buffer, 0}},
        enats_client:start_link(#{max_parser_buffer => 0})
    ).

t_invalid_public_inputs(Config) ->
    ?assertEqual({error, invalid_options}, enats_client:start_link(not_a_map)),
    {ok, Client} = enats_client:start_link(#{port => ?config(port, Config), owner => self()}),
    ChildSpec = enats_client:child_spec(#{id => diagnostics_test}),
    ?assertEqual(diagnostics_test, maps:get(id, ChildSpec)),
    ?assertEqual(disconnected, enats_client:status(Client)),
    ?assertEqual(#{}, enats_client:info(Client)),
    ?assertEqual(disconnected, maps:get(status, enats_client:stats(Client))),
    ?assertEqual({error, diagnostics_disabled}, enats_client:reset_diagnostics(Client)),
    ?assertEqual({error, invalid_options}, enats_client:enable_diagnostics(Client, bad_options)),
    ok = enats_client:disable_diagnostics(Client),
    ?assertEqual({error, disconnected}, enats_client:drain(Client, 10)),
    ok = enats_client:connect(Client),
    ?assertEqual(connected, enats_client:status(Client)),
    ?assertEqual(connected, maps:get(status, enats_client:stats(Client))),
    ?assertEqual(
        {error, invalid_payload}, enats_client:publish(Client, <<"input.test">>, not_iodata)
    ),
    ?assertEqual(true, is_process_alive(Client)),
    ?assertEqual(
        {error, invalid_options},
        enats_client:jetstream_publish(Client, <<"input.test">>, <<"p">>, bad_options)
    ),
    ?assertEqual(
        {error, {invalid_option, msg_id}},
        enats_client:jetstream_publish(Client, <<"input.test">>, <<"p">>, #{msg_id => 42})
    ),
    ?assertEqual(
        {error, invalid_options}, enats_client:subscribe(Client, <<"input.test">>, not_a_map)
    ),
    ?assertEqual(true, is_process_alive(Client)),
    ?assertEqual({error, invalid_timeout}, enats_client:connect(Client, bad_timeout)),
    ?assertEqual(true, is_process_alive(Client)),
    ?assertEqual(
        {error, invalid_options},
        enats_client:request(Client, <<"input.test">>, <<"p">>, bad_options, 10)
    ),
    ?assertEqual({error, invalid_timeout}, enats_client:flush(Client, bad_timeout)),
    ok = enats_client:stop(Client).

t_diagnostics(Config) ->
    {ok, Client} = enats_client:start_link(#{port => ?config(port, Config), owner => self()}),
    ?assertEqual({error, diagnostics_disabled}, enats_client:diagnostics(Client)),
    ok = enats_client:enable_diagnostics(Client, #{message_sample_every => 1}),
    ok = enats_client:connect(Client),
    ok = enats_client:publish(Client, <<"diagnostics.test">>, <<"payload">>),
    ok = enats_client:flush(Client, 1000),
    {ok, Snapshot} = enats_client:diagnostics(Client),
    ?assertEqual(true, maps:get(enabled, Snapshot)),
    Latencies = maps:get(latencies, Snapshot),
    ?assert(maps:is_key(transport_connect_latency, Latencies)),
    ?assert(maps:is_key(nats_connect_latency, Latencies)),
    ?assert(maps:is_key(total_connect_latency, Latencies)),
    ?assert(maps:is_key(publish_latency, Latencies)),
    PublishLatency = maps:get(publish_latency, Latencies),
    ?assert(is_integer(maps:get(p50_us, PublishLatency))),
    ?assert(is_integer(maps:get(p90_us, PublishLatency))),
    ?assert(is_integer(maps:get(p95_us, PublishLatency))),
    ?assert(is_integer(maps:get(p99_us, PublishLatency))),
    ok = enats_client:reset_diagnostics(Client),
    {ok, Reset} = enats_client:diagnostics(Client),
    ?assertEqual(#{}, maps:get(latencies, Reset)),
    ok = enats_client:disable_diagnostics(Client),
    ?assertEqual({error, diagnostics_disabled}, enats_client:diagnostics(Client)),
    ok = enats_client:stop(Client).

t_drain(Config) ->
    {ok, Client} = enats_client:start_link(#{port => ?config(port, Config), owner => self()}),
    ok = enats_client:connect(Client),
    {ok, Subscription} = enats_client:subscribe(Client, <<"drain.test">>, #{}),
    ok = enats_client:drain(Client, 1000),
    ?assertEqual(disconnected, enats_client:status(Client)),
    ?assertEqual({error, disconnected}, enats_client:unsubscribe(Client, Subscription)).

t_parser_limits(_Config) ->
    LongLine = binary:copy(<<"x">>, 4097),
    ?assertError(control_line_too_long, enats_frame:parse(LongLine, enats_frame:initial_state())),
    HugeLine = binary:copy(<<"x">>, 8 * 1024 * 1024 + 1),
    ?assertError(parser_buffer_limit, enats_frame:parse(HugeLine, enats_frame:initial_state())),
    Custom = enats_frame:initial_state(#{max_control_line => 8, max_parser_buffer => 32}),
    ?assertError(control_line_too_long, enats_frame:parse(<<"123456789">>, Custom)),
    ?assertError(parser_buffer_limit, enats_frame:parse(binary:copy(<<"x">>, 33), Custom)).

t_slow_consumer(Config) ->
    SlowOwner = spawn(fun slow_owner/0),
    {ok, Client} = enats_client:start_link(#{
        port => ?config(port, Config),
        owner => self(),
        slow_consumer_limit => 0
    }),
    ok = enats_client:connect(Client),
    {ok, _Subscription} = enats_client:subscribe(Client, <<"slow.test">>, #{owner => SlowOwner}),
    ok = enats_client:publish(Client, <<"slow.test">>, <<"one">>),
    ok = enats_client:flush(Client, 1000),
    receive
        {enats_client, Client, slow_consumer, _Metadata} -> ok
    after 1000 -> ct:fail(slow_consumer_not_detected)
    end,
    ?assertEqual(0, maps:get(subscriptions, enats_client:stats(Client))),
    ok = enats_client:stop(Client),
    exit(SlowOwner, kill).

t_protocol_input_safety(Config) ->
    {ok, Client} = enats_client:start_link(#{port => ?config(port, Config), owner => self()}),
    ok = enats_client:connect(Client),
    ?assertEqual(
        {error, {invalid_option, reply_to}},
        enats_client:publish(Client, <<"safe.test">>, <<"payload">>, #{
            reply_to => <<"reply\r\nPING">>
        })
    ),
    ?assertEqual(
        {error, {invalid_option, queue_group}},
        enats_client:subscribe(Client, <<"safe.test">>, #{queue_group => <<"queue\r\nPING">>})
    ),
    ?assertEqual(true, is_process_alive(Client)),
    ok = enats_client:stop(Client).

t_drain_clears_state(Config) ->
    {ok, Client} = enats_client:start_link(#{port => ?config(port, Config), owner => self()}),
    ok = enats_client:connect(Client),
    {ok, _Subscription} = enats_client:subscribe(Client, <<"drain.reconnect">>, #{}),
    ok = enats_client:drain(Client, 1000),
    ?assertEqual(0, maps:get(subscriptions, enats_client:stats(Client))),
    ok = enats_client:connect(Client),
    ?assertEqual(0, maps:get(subscriptions, enats_client:stats(Client))),
    ok = enats_client:stop(Client).

t_connect_deadline(_Config) ->
    {FirstServer, FirstPort} = start_fake_server(silent),
    {SecondServer, SecondPort} = start_fake_server(silent),
    {ok, Client} = enats_client:start_link(#{
        servers => [{"127.0.0.1", FirstPort}, {"127.0.0.1", SecondPort}],
        owner => self()
    }),
    StartedAt = erlang:monotonic_time(millisecond),
    Result = enats_client:connect(Client, 100),
    Elapsed = erlang:monotonic_time(millisecond) - StartedAt,
    ?assertMatch({error, _}, Result),
    ?assert(Elapsed < 170),
    ?assertEqual(disconnected, enats_client:status(Client)),
    ok = enats_client:stop(Client),
    exit(FirstServer, normal),
    exit(SecondServer, normal).

t_owner_down(Config) ->
    Owner = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    {ok, Client} = enats_client:start_link(#{port => ?config(port, Config), owner => self()}),
    ok = enats_client:connect(Client),
    {ok, _Subscription} = enats_client:subscribe(Client, <<"owner.down">>, #{owner => Owner}),
    exit(Owner, kill),
    wait_for_subscription_count(Client, 0, 20),
    ok = enats_client:stop(Client).

wait_for_subscription_count(Client, Expected, Attempts) ->
    case maps:get(subscriptions, enats_client:stats(Client)) of
        Expected ->
            ok;
        _ when Attempts > 0 ->
            timer:sleep(10),
            wait_for_subscription_count(Client, Expected, Attempts - 1);
        Actual ->
            ct:fail({subscription_count_not_updated, Actual})
    end.

slow_owner() ->
    receive
        stop -> ok;
        _Message -> slow_owner()
    end.

t_canonical_no_responders(_Config) ->
    Header = <<"NATS/1.0 503 No Responders\r\nNats-Subject: $JS.API.TEST\r\n\r\n">>,
    Size = integer_to_binary(byte_size(Header)),
    Frame = iolist_to_binary([
        <<"HMSG _INBOX.test 1 ">>, Size, <<" ">>, Size, <<"\r\n">>, Header, <<"\r\n">>
    ]),
    {[#{headers := Headers, payload := <<>>}], _} = enats_frame:parse(
        Frame, enats_frame:initial_state()
    ),
    ?assertEqual(<<"503">>, proplists:get_value(<<"Status">>, Headers)),
    ?assertEqual(<<"No Responders">>, proplists:get_value(<<"Description">>, Headers)),
    ?assertEqual(<<"$JS.API.TEST">>, proplists:get_value(<<"Nats-Subject">>, Headers)).

t_invalid_headers(_Config) ->
    ?assertEqual(
        {error, {invalid_header_name, <<"bad:name">>}},
        enats_frame:validate_headers([{<<"bad:name">>, <<"value">>}])
    ),
    ?assertEqual(
        {error, {invalid_header_value, <<"x">>}},
        enats_frame:validate_headers([{<<"x">>, <<"bad\r\nvalue">>}])
    ).

t_connect_publish_subscribe_flush(Config) ->
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => ?config(port, Config), owner => self()
    }),
    ok = enats_client:connect(Client),
    ?assertEqual(connected, enats_client:status(Client)),
    {ok, Subscription} = enats_client:subscribe(Client, <<"enats.test">>, #{}),
    ok = enats_client:publish(Client, <<"enats.test">>, <<"hello">>),
    ok = enats_client:flush(Client, 1000),
    receive
        {enats_client, Client,
            {message, #{type := msg, subject := <<"enats.test">>, payload := <<"hello">>}}} ->
            ok
    after 1000 -> ct:fail(message_not_received)
    end,
    ok = enats_client:unsubscribe(Client, Subscription),
    ok = enats_client:disconnect(Client),
    ?assertEqual(disconnected, enats_client:status(Client)),
    ok = enats_client:stop(Client).

t_default_owner(Config) ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => ?config(port, Config)}),
    ok = enats_client:connect(Client),
    {ok, _Subscription} = enats_client:subscribe(Client, <<"enats.default-owner">>, #{}),
    ok = enats_client:publish(Client, <<"enats.default-owner">>, <<"hello">>),
    ok = enats_client:flush(Client, 1000),
    receive
        {enats_client, Client, {message, #{payload := <<"hello">>}}} -> ok
    after 1000 -> ct:fail(default_owner_did_not_receive_message)
    end,
    ok = enats_client:stop(Client).

t_connect_idempotence(Config) ->
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => ?config(port, Config), owner => self()
    }),
    ok = enats_client:connect(Client),
    ?assertEqual({error, already_connected}, enats_client:connect(Client)),
    ok = enats_client:stop(Client).

t_coalesced_flush(_Config) ->
    {Server, Port} = start_fake_server(coalesced_flush),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_server_limits(_Config) ->
    {Server, Port} = start_fake_server(server_limits),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    ?assertEqual(
        {error, {payload_too_large, 3}},
        enats_client:publish(Client, <<"limits">>, <<"1234">>)
    ),
    ?assertEqual(
        {error, headers_not_supported},
        enats_client:publish(Client, <<"limits">>, <<"ok">>, #{headers => [{<<"x">>, <<"y">>}]})
    ),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_flush_concurrency(_Config) ->
    {Server, Port} = start_fake_server(flush_concurrent),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    Parent = self(),
    spawn(fun() -> Parent ! {first_flush, enats_client:flush(Client, 10000)} end),
    receive
        {fake_flush_received, Server} -> ok
    after 1000 -> ct:fail(first_flush_not_received)
    end,
    spawn(fun() -> Parent ! {second_flush, enats_client:flush(Client, 10000)} end),
    receive
        {fake_second_flush_received, Server} -> ok
    after 1000 -> ct:fail(second_flush_not_received)
    end,
    Server ! release_first_flush,
    receive
        {first_flush, ok} -> ok
    after 10000 -> ct:fail(first_flush_not_completed)
    end,
    receive
        {second_flush, Result} -> ct:fail({second_flush_completed_by_first_pong, Result})
    after 50 -> ok
    end,
    Server ! release_second_flush,
    receive
        {second_flush, ok} -> ok
    after 10000 -> ct:fail(second_flush_not_completed)
    end,
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_flush_timeout_preserves_pong_order(_Config) ->
    {Server, Port} = start_fake_server(flush_timeout_order),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    Parent = self(),
    spawn(fun() -> Parent ! {first_flush, enats_client:flush(Client, 20)} end),
    receive
        {fake_flush_received, Server} -> ok
    after 1000 -> ct:fail(first_flush_not_received)
    end,
    receive
        {first_flush, {error, timeout}} -> ok
    after 1000 -> ct:fail(first_flush_did_not_time_out)
    end,
    spawn(fun() -> Parent ! {second_flush, enats_client:flush(Client, 10000)} end),
    receive
        {fake_second_flush_received, Server} -> ok
    after 1000 -> ct:fail(second_flush_not_received)
    end,
    Server ! release_timed_out_flush,
    receive
        {second_flush, Result} -> ct:fail({second_flush_completed_by_stale_pong, Result})
    after 50 -> ok
    end,
    Server ! release_second_flush,
    receive
        {second_flush, ok} -> ok
    after 10000 -> ct:fail(second_flush_not_completed)
    end,
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_flush_disconnects_all_waiters(_Config) ->
    {Server, Port} = start_fake_server(flush_disconnect),
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => Port, owner => self(), notify => false
    }),
    ok = enats_client:connect(Client),
    Parent = self(),
    spawn(fun() -> Parent ! {first_flush, enats_client:flush(Client, 10000)} end),
    receive
        {fake_flush_received, Server} -> ok
    after 1000 -> ct:fail(first_flush_not_received)
    end,
    spawn(fun() -> Parent ! {second_flush, enats_client:flush(Client, 10000)} end),
    receive
        {fake_second_flush_received, Server} -> ok
    after 1000 -> ct:fail(second_flush_not_received)
    end,
    ?assertEqual(
        lists:sort([
            {first_flush, {error, {disconnected, closed}}},
            {second_flush, {error, {disconnected, closed}}}
        ]),
        lists:sort(receive_flush_results(2, []))
    ),
    ok = enats_client:stop(Client),
    exit(Server, normal).

receive_flush_results(0, Acc) ->
    Acc;
receive_flush_results(N, Acc) ->
    receive
        {Tag, _Result} = Message when Tag =:= first_flush; Tag =:= second_flush ->
            receive_flush_results(N - 1, [Message | Acc]);
        _Other ->
            receive_flush_results(N, Acc)
    after 1000 ->
        ct:fail(flush_disconnect_results_missing)
    end.

t_headers(Config) ->
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => ?config(port, Config), owner => self()
    }),
    ok = enats_client:connect(Client),
    {ok, _Subscription} = enats_client:subscribe(Client, <<"enats.headers">>, #{}),
    ok = enats_client:publish(
        Client,
        <<"enats.headers">>,
        <<"body">>,
        #{
            reply_to => <<"reply.subject">>,
            headers => [{<<"X-Test">>, <<"yes">>}, {<<"X-Test">>, <<"again">>}]
        }
    ),
    ok = enats_client:flush(Client, 1000),
    receive
        {enats_client, Client,
            {message, #{
                type := hmsg,
                headers := Headers,
                reply_to := <<"reply.subject">>,
                payload := <<"body">>
            }}} ->
            ?assertEqual([{<<"X-Test">>, <<"yes">>}, {<<"X-Test">>, <<"again">>}], Headers)
    after 1000 -> ct:fail(header_message_not_received)
    end,
    ok = enats_client:stop(Client).

t_frame_variants(_Config) ->
    Connect = iolist_to_binary(enats_frame:serialize_connect(#{protocol => 1, headers => true})),
    ?assertMatch(<<"CONNECT ", _/binary>>, Connect),
    ?assertMatch(
        <<"PUB foo 3\r\nbar\r\n">>,
        iolist_to_binary(enats_frame:serialize({pub, <<"foo">>, <<"bar">>}))
    ),
    ?assertMatch(
        <<"PUB foo reply 3\r\nbar\r\n">>,
        iolist_to_binary(enats_frame:serialize({pub, <<"foo">>, <<"reply">>, <<"bar">>}))
    ),
    Hpub = iolist_to_binary(
        enats_frame:serialize({hpub, <<"foo">>, undefined, [{<<"X">>, <<"Y">>}], <<"bar">>})
    ),
    ?assertMatch(<<"HPUB foo ", _/binary>>, Hpub),
    HpubReply = iolist_to_binary(
        enats_frame:serialize({hpub, <<"foo">>, <<"reply">>, [{<<"X">>, <<"Y">>}], <<"bar">>})
    ),
    ?assertMatch(<<"HPUB foo reply ", _/binary>>, HpubReply),
    ?assertMatch(
        <<"SUB foo 1\r\n">>,
        iolist_to_binary(enats_frame:serialize({sub, <<"foo">>, <<"1">>, undefined}))
    ),
    ?assertMatch(
        <<"SUB foo q 1\r\n">>,
        iolist_to_binary(enats_frame:serialize({sub, <<"foo">>, <<"1">>, <<"q">>}))
    ),
    ?assertMatch(<<"UNSUB 1\r\n">>, iolist_to_binary(enats_frame:serialize({unsub, <<"1">>}))),
    Hmsg = <<"HMSG foo 1 18 21\r\nNATS/1.0\r\nX: Y\r\n\r\nbar\r\n">>,
    {Frames, _} = enats_frame:parse(Hmsg, enats_frame:initial_state()),
    ?assertEqual(
        [
            #{
                type => hmsg,
                subject => <<"foo">>,
                sid => <<"1">>,
                header_size => 18,
                headers => [{<<"X">>, <<"Y">>}],
                payload => <<"bar">>
            }
        ],
        Frames
    ).

t_auth_helpers(_Config) ->
    {ok, #{}} = enats_auth:connect_params(none, #{}, #{}),
    ?assertEqual(ok, enats_auth:validate(none)),
    ?assertEqual({error, invalid_credentials}, enats_auth:validate(#{})),
    ?assertEqual({error, invalid_secret_type}, enats_auth:resolve_secret(42)),
    ?assertEqual(none, enats_auth:describe(none)),
    ?assertEqual(user_password, enats_auth:describe(#{mechanism => user_password})),
    {ok, UserParams} = enats_auth:connect_params(
        #{
            mechanism => user_password,
            username => <<"alice">>,
            password => (fun() -> <<"secret">> end)
        },
        #{},
        #{}
    ),
    ?assertEqual(<<"alice">>, maps:get(user, UserParams)),
    ?assertEqual(<<"secret">>, maps:get(pass, UserParams)),
    {ok, TokenParams} = enats_auth:connect_params(
        #{mechanism => token, token => (fun() -> {ok, <<"token">>} end)}, #{}, #{}
    ),
    ?assertEqual(<<"token">>, maps:get(auth_token, TokenParams)),
    ?assertEqual(
        {error, invalid_secret_type},
        enats_auth:connect_params(
            #{mechanism => token, token => 42}, #{}, #{}
        )
    ),
    ?assertEqual(
        {error, nkey_nonce_missing},
        enats_auth:connect_params(
            #{mechanism => nkey, public_key => <<"key">>, sign_fun => (fun(_) -> <<"sig">> end)},
            #{},
            #{}
        )
    ),
    Seed = encode_seed(<<1:256>>),
    {ok, SeedParams} = enats_auth:connect_params(
        #{mechanism => nkey_seed, seed => (fun() -> Seed end)},
        #{nonce => <<"nonce">>},
        #{protocol => 1}
    ),
    ?assertEqual(56, byte_size(maps:get(nkey, SeedParams))),
    ?assert(is_binary(maps:get(sig, SeedParams))),
    {ok, NkeyParams} = enats_auth:connect_params(
        #{
            mechanism => nkey,
            public_key => <<"key">>,
            sign_fun => (fun(<<"nonce">>) -> <<"sig">> end)
        },
        #{nonce => <<"nonce">>},
        #{}
    ),
    ?assertEqual(<<"sig">>, maps:get(sig, NkeyParams)),
    ?assertEqual(
        {error, {invalid_nkey_signature, <<"invalid">>}},
        enats_auth:connect_params(
            #{mechanism => nkey, public_key => <<"key">>, sign_fun => (fun(_) -> {error, bad} end)},
            #{nonce => <<"nonce">>},
            #{}
        )
    ),
    ?assertEqual(
        {error, {invalid_nkey_signature, <<"invalid">>}},
        enats_auth:connect_params(
            #{mechanism => nkey, public_key => <<"key">>, sign_fun => (fun(_) -> bad end)},
            #{nonce => <<"nonce">>},
            #{}
        )
    ),
    ?assertEqual(
        {error, secret_provider_failed},
        enats_auth:connect_params(
            #{mechanism => token, token => (fun() -> erlang:error(bad) end)}, #{}, #{}
        )
    ).

t_nkey_helpers(_Config) ->
    {Public, Private} = crypto:generate_key(eddsa, ed25519),
    Encoded = enats_auth:encode_nkey_public(Public),
    ?assertEqual(56, byte_size(Encoded)),
    SignFun = enats_auth:nkey_signer(Public, Private),
    UrlSignature = SignFun(<<"nonce">>),
    StandardSignature = binary:replace(
        binary:replace(UrlSignature, <<"-">>, <<"+">>, [global]), <<"_">>, <<"/">>, [global]
    ),
    Signature = base64:decode(<<StandardSignature/binary, "==">>),
    ?assert(crypto:verify(eddsa, none, <<"nonce">>, Signature, [Public, ed25519])).

t_secret_and_subject(_Config) ->
    ?assertEqual({ok, <<"value">>}, enats_auth:resolve_secret(<<"value">>)),
    ?assertEqual({ok, <<"value">>}, enats_auth:resolve_secret(fun() -> {ok, <<"value">>} end)),
    ?assertEqual(
        {error, secret_provider_failed}, enats_auth:resolve_secret(fun() -> erlang:error(bad) end)
    ),
    ?assertEqual(
        #{password => <<"******">>, nested => [#{token => <<"******">>}]},
        enats_auth:redact(#{password => <<"secret">>, nested => [#{token => <<"token">>}]})
    ),
    ?assertEqual(value, enats_auth:redact(value)).

t_invalid_subject(_Config) ->
    {ok, Client} = enats_client:start_link(#{owner => self()}),
    ?assertEqual(
        {error, invalid_subject},
        enats_client:publish(Client, <<"bad subject">>, <<"payload">>)
    ),
    ?assertEqual({error, invalid_subject}, enats_client:publish(Client, <<>>, <<"payload">>)),
    ?assertEqual(
        {error, wildcard_subject_not_allowed},
        enats_client:publish(Client, <<"foo.*">>, <<"payload">>)
    ),
    ok = enats_client:stop(Client).

t_lazy_secret(_Config) ->
    Parent = self(),
    Provider = fun() ->
        Parent ! secret_called,
        <<"password">>
    end,
    Auth = #{mechanism => user_password, username => <<"user">>, password => Provider},
    {ok, Client} = enats_client:start_link(#{auth => Auth, owner => self()}),
    receive
        secret_called -> ct:fail(secret_evaluated_before_connect)
    after 50 -> ok
    end,
    ok = enats_client:stop(Client).

t_user_password(Config) ->
    case os:getenv("ENATS_AUTH_PORT") of
        false ->
            case nats_server_executable() of
                unavailable ->
                    {skip, "nats-server executable is unavailable"};
                {ok, _} ->
                    Port = dynamic_port(),
                    PidFile = filename:join(?config(priv_dir, Config), "nats-userpass.pid"),
                    Server = start_nats_process([
                        "-a",
                        "127.0.0.1",
                        "-p",
                        integer_to_list(Port),
                        "-P",
                        PidFile,
                        "--user",
                        "alice",
                        "--pass",
                        "secret"
                    ]),
                    wait_for_port(Port),
                    try
                        user_password_case(Port)
                    after
                        stop_nats_server(Server, PidFile)
                    end
            end;
        Port0 ->
            user_password_case(list_to_integer(Port0))
    end.

t_tls(Config) ->
    case os:getenv("ENATS_TLS_PORT") of
        false ->
            case nats_server_executable() of
                unavailable ->
                    {skip, "nats-server executable is unavailable"};
                {ok, _} ->
                    PrivDir = ?config(priv_dir, Config),
                    Port = dynamic_port(),
                    PidFile = filename:join(PrivDir, "nats-tls.pid"),
                    CaFile = filename:join(PrivDir, "nats-test-ca.crt"),
                    CaKeyFile = filename:join(PrivDir, "nats-test-ca.key"),
                    CertFile = filename:join(PrivDir, "nats-tls.crt"),
                    KeyFile = filename:join(PrivDir, "nats-tls.key"),
                    ExtFile = filename:join(PrivDir, "nats-tls.ext"),
                    generate_test_ca(CaFile, CaKeyFile),
                    generate_signed_certificate(CaFile, CaKeyFile, CertFile, KeyFile, ExtFile),
                    Server = start_nats_process([
                        "-a",
                        "127.0.0.1",
                        "-p",
                        integer_to_list(Port),
                        "-P",
                        PidFile,
                        "--tls",
                        "--tlscert",
                        CertFile,
                        "--tlskey",
                        KeyFile
                    ]),
                    wait_for_port(Port),
                    try
                        {ok, SecureClient} = enats_client:start_link(#{
                            host => "127.0.0.1",
                            port => Port,
                            tls => true,
                            owner => self()
                        }),
                        ?assertMatch({error, _}, enats_client:connect(SecureClient)),
                        ok = enats_client:stop(SecureClient),
                        verify_peer_tls_case(Port, CaFile, CertFile),
                        tls_case(Port)
                    after
                        stop_nats_server(Server, PidFile)
                    end
            end;
        Port0 ->
            tls_case(list_to_integer(Port0))
    end.

user_password_case(Port) ->
    Parent = self(),
    Provider = fun() ->
        Parent ! password_called,
        <<"secret">>
    end,
    Auth = #{mechanism => user_password, username => <<"alice">>, password => Provider},
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => Port, auth => Auth, owner => self()
    }),
    ok = enats_client:connect(Client),
    receive
        password_called -> ok
    after 1000 -> ct:fail(secret_provider_not_called)
    end,
    ok = enats_client:publish(Client, <<"enats.auth">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client).

tls_case(Port) ->
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => Port,
        tls => true,
        ssl_opts => [{verify, verify_none}],
        owner => self()
    }),
    ok = enats_client:connect(Client),
    ok = enats_client:publish(Client, <<"enats.tls">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client).

t_tls_first(Config) ->
    case nats_server_executable() of
        unavailable ->
            {skip, "nats-server executable is unavailable"};
        {ok, _} ->
            PrivDir = ?config(priv_dir, Config),
            Port = dynamic_port(),
            PidFile = filename:join(PrivDir, "nats-tls-first.pid"),
            CertFile = filename:join(PrivDir, "nats-tls-first.crt"),
            KeyFile = filename:join(PrivDir, "nats-tls-first.key"),
            ConfigFile = filename:join(PrivDir, "nats-tls-first.conf"),
            generate_test_certificate(CertFile, KeyFile),
            ConfigText = iolist_to_binary([
                "port: ",
                integer_to_list(Port),
                "\n",
                "tls {\n",
                "  cert_file: \"",
                CertFile,
                "\"\n",
                "  key_file: \"",
                KeyFile,
                "\"\n",
                "  handshake_first: true\n",
                "}\n"
            ]),
            ok = file:write_file(ConfigFile, ConfigText),
            Server = start_nats_server(ConfigFile, PidFile),
            wait_for_port(Port),
            try
                tls_first_case(Port)
            after
                stop_nats_server(Server, PidFile)
            end
    end.

tls_first_case(Port) ->
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => Port,
        tls => true,
        tls_handshake => first,
        ssl_opts => [{verify, verify_none}],
        owner => self()
    }),
    ok = enats_client:connect(Client),
    {ok, Subscription} = enats_client:subscribe(Client, <<"enats.tls.first">>, #{}),
    ok = enats_client:publish(Client, <<"enats.tls.first">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    receive
        {enats_client, Client, {message, #{subject := <<"enats.tls.first">>, payload := <<"ok">>}}} ->
            ok
    after 1000 -> ct:fail(tls_first_message_not_received)
    end,
    ok = enats_client:unsubscribe(Client, Subscription),
    ok = enats_client:stop(Client).

generate_test_certificate(CertFile, KeyFile) ->
    Command = lists:flatten(
        io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -keyout ~ts -out ~ts -subj /CN=localhost -days 1 >/dev/null 2>&1",
            [KeyFile, CertFile]
        )
    ),
    _ = os:cmd(Command),
    {ok, _} = file:read_file(CertFile),
    {ok, _} = file:read_file(KeyFile),
    ok.

generate_test_ca(CaFile, CaKeyFile) ->
    Command = lists:flatten(
        io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -keyout ~ts -out ~ts -subj /CN=enats-test-ca -days 1 >/dev/null 2>&1",
            [CaKeyFile, CaFile]
        )
    ),
    _ = os:cmd(Command),
    {ok, _} = file:read_file(CaFile),
    {ok, _} = file:read_file(CaKeyFile),
    ok.

generate_signed_certificate(CaFile, CaKeyFile, CertFile, KeyFile, ExtFile) ->
    ok = file:write_file(ExtFile, <<"subjectAltName=DNS:localhost\n">>),
    CsrCommand = lists:flatten(
        io_lib:format(
            "openssl req -newkey rsa:2048 -nodes -keyout ~ts -out ~ts -subj /CN=localhost >/dev/null 2>&1",
            [KeyFile, CertFile ++ ".csr"]
        )
    ),
    _ = os:cmd(CsrCommand),
    SignCommand = lists:flatten(
        io_lib:format(
            "openssl x509 -req -in ~ts -CA ~ts -CAkey ~ts -CAcreateserial -out ~ts -days 1 -sha256 -extfile ~ts >/dev/null 2>&1",
            [CertFile ++ ".csr", CaFile, CaKeyFile, CertFile, ExtFile]
        )
    ),
    _ = os:cmd(SignCommand),
    {ok, _} = file:read_file(CertFile),
    {ok, _} = file:read_file(KeyFile),
    ok.

verify_peer_tls_case(Port, CaFile, CertFile) ->
    {ok, Client} = enats_client:start_link(#{
        host => "localhost",
        port => Port,
        tls => true,
        ssl_opts => [{verify, verify_peer}, {cacertfile, CaFile}],
        owner => self()
    }),
    ok = enats_client:connect(Client),
    ok = enats_client:stop(Client),
    {ok, WrongCaClient} = enats_client:start_link(#{
        host => "localhost",
        port => Port,
        tls => true,
        ssl_opts => [{verify, verify_peer}, {cacertfile, CertFile}],
        owner => self()
    }),
    ?assertMatch({error, _}, enats_client:connect(WrongCaClient)),
    ok = enats_client:stop(WrongCaClient),
    {ok, MismatchClient} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => Port,
        tls => true,
        ssl_opts => [{verify, verify_peer}, {cacertfile, CaFile}],
        owner => self()
    }),
    ?assertMatch({error, _}, enats_client:connect(MismatchClient)),
    ok = enats_client:stop(MismatchClient),
    ok.

t_connection_errors(_Config) ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => 1, owner => self()}),
    ?assertEqual(disconnected, enats_client:status(Client)),
    ?assertEqual(#{}, enats_client:info(Client)),
    ?assertMatch({error, _}, enats_client:connect(Client)),
    ok = enats_client:disconnect(Client),
    ok = enats_client:stop(Client),
    ok.

t_connection_queries(Config) ->
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => ?config(port, Config), owner => self()
    }),
    ?assertEqual(
        {error, disconnected}, enats_client:publish(Client, <<"valid.subject">>, <<"payload">>)
    ),
    ok = enats_client:connect(Client),
    Info = enats_client:info(Client),
    ?assertEqual(?config(port, Config), maps:get(port, Info)),
    ?assertEqual(
        {error, {invalid_subject, invalid_subject}},
        enats_connection:publish(Client, <<"bad subject">>, <<"payload">>, #{})
    ),
    ?assertEqual(
        {error, invalid_subject},
        enats_client:subscribe(Client, <<"bad subject">>, #{})
    ),
    ?assertEqual(
        {error, {no_responders, <<"503">>}},
        enats_client:request(Client, <<"no.reply">>, <<"payload">>, #{timeout => 20})
    ),
    ?assertMatch(
        {error, _}, enats_client:jetstream_publish(Client, <<"subject">>, <<"payload">>, #{})
    ),
    ?assertEqual({error, not_found}, enats_client:unsubscribe(Client, make_ref())),
    ok = enats_client:disconnect(Client),
    ok = enats_client:stop(Client),
    {Server, Port} = start_fake_server(flush_timeout),
    {ok, FlushClient} = enats_client:start_link(#{
        host => "127.0.0.1", port => Port, owner => self()
    }),
    ok = enats_client:connect(FlushClient),
    ?assertEqual({error, timeout}, enats_client:flush(FlushClient, 20)),
    ok = enats_client:stop(FlushClient),
    exit(Server, normal).

t_fake_connection_paths(_Config) ->
    {ErrorServer, ErrorPort} = start_fake_server(server_error),
    {ok, ErrorClient} = enats_client:start_link(#{
        host => "127.0.0.1", port => ErrorPort, owner => self()
    }),
    ?assertEqual({error, {server_error, <<"\"bad\"">>}}, enats_client:connect(ErrorClient)),
    ok = enats_client:stop(ErrorClient),
    exit(ErrorServer, normal),
    {CloseServer, ClosePort} = start_fake_server(close_without_pong),
    {ok, CloseClient} = enats_client:start_link(#{
        host => "127.0.0.1", port => ClosePort, owner => self()
    }),
    ?assertMatch({error, _}, enats_client:connect(CloseClient)),
    ok = enats_client:stop(CloseClient),
    exit(CloseServer, normal).

t_reconnect(_Config) ->
    {Server, Port} = start_fake_server(reconnect),
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => Port,
        reconnect => true,
        reconnect_delay => 20,
        owner => self()
    }),
    ok = enats_client:connect(Client),
    {ok, _} = enats_client:subscribe(Client, <<"reconnect.test">>, #{}),
    receive
        {enats_client, Client, disconnected, closed} -> ok
    after 2000 -> ct:fail(reconnect_disconnect_not_observed)
    end,
    ?assertEqual(reconnecting, enats_client:status(Client)),
    receive
        {enats_client, Client, connected, _Info} -> ok
    after 2000 -> ct:fail(reconnect_not_observed)
    end,
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_stale_connection_reconnect(_Config) ->
    {Server, Port} = start_fake_server(stale_reconnect),
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => Port,
        reconnect => true,
        reconnect_delay => 10,
        ping_interval => 20,
        max_pings_out => 1,
        owner => self()
    }),
    ok = enats_client:connect(Client),
    receive
        {fake_stale_ping, Server} -> ok
    after 1000 ->
        ct:fail(stale_ping_not_seen)
    end,
    receive
        {fake_server_reconnected, Server} -> ok
    after 2000 ->
        ct:fail(stale_connection_not_reconnected)
    end,
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_disconnect_while_connecting(_Config) ->
    {InfoServer, InfoPort} = start_fake_server(silent),
    {ok, InfoClient} = enats_client:start_link(#{
        host => "127.0.0.1", port => InfoPort, owner => self()
    }),
    Parent = self(),
    _ConnectCaller = spawn(fun() -> Parent ! {connect_result, enats_client:connect(InfoClient)} end),
    receive
        {fake_server_accepted, InfoServer, silent} -> ok
    after 1000 -> ct:fail(fake_info_server_not_accepted)
    end,
    ?assertEqual(connecting, enats_client:status(InfoClient)),
    ?assertEqual({error, connecting}, enats_client:info(InfoClient)),
    ok = enats_client:disconnect(InfoClient),
    receive
        {connect_result, {error, disconnected}} -> ok
    after 1000 -> ct:fail(connect_call_not_replied)
    end,
    ok = enats_client:stop(InfoClient),
    exit(InfoServer, normal),
    {PongServer, PongPort} = start_fake_server(no_pong),
    {ok, PongClient} = enats_client:start_link(#{
        host => "127.0.0.1", port => PongPort, owner => self()
    }),
    _PongCaller = spawn(fun() ->
        Parent ! {pong_connect_result, enats_client:connect(PongClient)}
    end),
    receive
        {fake_server_accepted, PongServer, no_pong} -> ok
    after 1000 -> ct:fail(fake_pong_server_not_accepted)
    end,
    ?assertEqual(connecting, enats_client:status(PongClient)),
    ?assertEqual({error, connecting}, enats_client:info(PongClient)),
    ok = enats_client:disconnect(PongClient),
    receive
        {pong_connect_result, {error, disconnected}} -> ok
    after 1000 -> ct:fail(pong_call_not_replied)
    end,
    ok = enats_client:stop(PongClient),
    exit(PongServer, normal).

t_topology_info(_Config) ->
    {Server, Port} = start_fake_server(topology),
    {ok, Client} = enats_client:start_link(#{servers => [{"127.0.0.1", Port}], owner => self()}),
    ok = enats_client:connect(Client),
    Info = enats_client:info(Client),
    ?assertEqual([<<"nats://127.0.0.1:14222">>, <<"bad">>], maps:get(connect_urls, Info)),
    ?assertEqual([{<<"untrusted">>, <<"untrusted">>}], maps:get(extra, Info)),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_server_failover(Config) ->
    {ok, Client} = enats_client:start_link(#{
        servers => [{"127.0.0.1", 1}, {"127.0.0.1", ?config(port, Config)}], owner => self()
    }),
    ok = enats_client:connect(Client),
    ?assertEqual(connected, enats_client:status(Client)),
    ok = enats_client:stop(Client).

t_server_failover_handshake(_Config) ->
    {SilentServer, SilentPort} = start_fake_server(silent),
    {HealthyServer, HealthyPort} = start_fake_server(server_limits),
    {ok, Client} = enats_client:start_link(#{
        servers => [{"127.0.0.1", SilentPort}, {"127.0.0.1", HealthyPort}],
        connect_timeout => 100,
        owner => self()
    }),
    try
        ok = enats_client:connect(Client, 500),
        ?assertEqual(connected, enats_client:status(Client))
    after
        ok = enats_client:stop(Client),
        exit(SilentServer, normal),
        exit(HealthyServer, normal)
    end.

t_nkey_nats_server(Config) ->
    case nats_server_executable() of
        unavailable -> {skip, "nats-server executable is unavailable"};
        {ok, _} -> t_nkey_nats_server_impl(Config)
    end.

t_nkey_nats_server_impl(Config) ->
    {PublicKey, PrivateKey} = crypto:generate_key(eddsa, ed25519),
    PublicNKey = enats_auth:encode_nkey_public(PublicKey),
    Port = dynamic_port(),
    ConfigFile = filename:join(?config(priv_dir, Config), "nats-nkey.conf"),
    PidFile = filename:join(?config(priv_dir, Config), "nats-nkey.pid"),
    ConfigText = iolist_to_binary([
        "port: ",
        integer_to_list(Port),
        "\n",
        "authorization {\n  users = [{nkey: \"",
        PublicNKey,
        "\"}]\n}\n"
    ]),
    ok = file:write_file(ConfigFile, ConfigText),
    NatsServer = start_nats_server(ConfigFile, PidFile),
    wait_for_port(Port),
    Auth = #{
        mechanism => nkey,
        public_key => PublicNKey,
        sign_fun => enats_auth:nkey_signer(PublicKey, PrivateKey)
    },
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => Port,
        auth => Auth,
        owner => self()
    }),
    case enats_client:connect(Client) of
        ok ->
            ok;
        Error ->
            io:format("NKey connect failed: ~p~nServer logs: ~p~n", [Error, drain_nats(NatsServer)]),
            ct:fail({nkey_connect_failed, Error})
    end,
    ok = enats_client:publish(Client, <<"enats.nkey">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client),
    stop_nats_server(NatsServer, PidFile).

t_token_nats_server(Config) ->
    case nats_server_executable() of
        unavailable -> {skip, "nats-server executable is unavailable"};
        {ok, _} -> t_token_nats_server_impl(Config)
    end.

t_token_nats_server_impl(Config) ->
    Port = dynamic_port(),
    PidFile = filename:join(?config(priv_dir, Config), "nats-token.pid"),
    NatsServer = start_nats_process([
        "-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile, "--auth", "token"
    ]),
    wait_for_port(Port),
    Auth = #{mechanism => token, token => (fun() -> {ok, <<"token">>} end)},
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1",
        port => Port,
        auth => Auth,
        owner => self()
    }),
    ok = enats_client:connect(Client),
    ok = enats_client:publish(Client, <<"enats.token">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client),
    stop_nats_server(NatsServer, PidFile).

t_jetstream_nats_server(Config) ->
    case nats_server_executable() of
        unavailable ->
            {skip, "nats-server executable is unavailable"};
        {ok, _} ->
            Port = dynamic_port(),
            PidFile = filename:join(?config(priv_dir, Config), "nats-jetstream.pid"),
            Server = start_nats_process([
                "-a",
                "127.0.0.1",
                "-p",
                integer_to_list(Port),
                "-P",
                PidFile,
                "-js"
            ]),
            wait_for_port(Port),
            try
                {ok, Client} = enats_client:start_link(#{
                    host => "127.0.0.1", port => Port, owner => self()
                }),
                ok = enats_client:connect(Client),
                StreamConfig = jiffy:encode(#{
                    <<"name">> => <<"ORDERS">>,
                    <<"subjects">> => [<<"orders.>">>]
                }),
                {ok, #{payload := _CreateAck}} = enats_client:request(
                    Client, <<"$JS.API.STREAM.CREATE.ORDERS">>, StreamConfig, #{timeout => 2000}
                ),
                {ok, Ack1} = enats_client:jetstream_publish(
                    Client,
                    <<"orders.test">>,
                    <<"hello">>,
                    #{msg_id => <<"id-1">>, timeout => 2000}
                ),
                {ok, Ack2} = enats_client:jetstream_publish(
                    Client,
                    <<"orders.test">>,
                    <<"hello">>,
                    #{msg_id => <<"id-1">>, timeout => 2000}
                ),
                ?assertEqual(maps:get(sequence, Ack1), maps:get(sequence, Ack2)),
                ?assertEqual(true, maps:get(duplicate, Ack2)),
                ok = enats_client:stop(Client)
            after
                stop_nats_server(Server, PidFile)
            end
    end.

t_nkey_seed(_Config) ->
    Seed = <<"SUAACAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4DUPB6IC5CQ">>,
    {ok, PublicKey, SignFun} = enats_auth:from_seed(Seed),
    ?assertEqual(<<"UB43KVROR7TFJ6KAPCYRF2FJROTZAH4FHLTJLPWX4DRZCC5NASLGJBFE">>, PublicKey),
    ?assertEqual(56, byte_size(PublicKey)),
    ?assertEqual(86, byte_size(SignFun(<<"nonce">>))),
    {ok, #{jwt := <<"jwt">>, nkey := <<"PUB">>, sig := <<"SIG">>}} = enats_auth:connect_params(
        #{
            mechanism => jwt,
            jwt => fun() -> <<"jwt">> end,
            public_key => <<"PUB">>,
            sign_fun => fun(<<"nonce">>) -> <<"SIG">> end
        },
        #{nonce => <<"nonce">>},
        #{}
    ),
    Creds = iolist_to_binary([
        "-----BEGIN NATS USER JWT-----\njwt\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\n",
        Seed,
        "\n------END USER NKEY SEED------\n"
    ]),
    {ok, _CredentialsAuth} = enats_auth:credentials(Creds),
    CredsAuth = #{mechanism => credentials, provider => fun() -> {ok, Creds} end},
    {ok, CredsParams} = enats_auth:connect_params(CredsAuth, #{nonce => <<"nonce">>}, #{}),
    ?assertEqual(<<"jwt">>, maps:get(jwt, CredsParams)),
    ?assertEqual({error, invalid_nkey_seed}, enats_auth:from_seed(<<"bad">>)).

t_credentials_file_provider(Config) ->
    Seed = encode_seed(<<1:256>>),
    Filename = filename:join(?config(priv_dir, Config), "credentials-provider.creds"),
    Contents1 = credentials_contents(<<"jwt-1">>, Seed),
    Contents2 = credentials_contents(<<"jwt-2">>, Seed),
    ok = file:write_file(Filename, Contents1),
    try
        ?assertEqual(ok, enats_auth:validate_credentials_file(Filename)),
        {ok, Auth} = enats_auth:credentials_file(Filename),
        Provider = maps:get(provider, Auth),
        {env, Env} = erlang:fun_info(Provider, env),
        ?assert(lists:member(Filename, Env)),
        ?assertNot(lists:member(Contents1, Env)),
        {ok, Params1} = enats_auth:connect_params(Auth, #{nonce => <<"nonce">>}, #{}),
        ?assertEqual(<<"jwt-1">>, maps:get(jwt, Params1)),
        ok = file:write_file(Filename, Contents2),
        {ok, Params2} = enats_auth:connect_params(Auth, #{nonce => <<"nonce">>}, #{}),
        ?assertEqual(<<"jwt-2">>, maps:get(jwt, Params2))
    after
        ok = file:delete(Filename)
    end.

credentials_contents(JWT, Seed) ->
    iolist_to_binary([
        "-----BEGIN NATS USER JWT-----\n",
        JWT,
        "\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\n",
        Seed,
        "\n------END USER NKEY SEED------\n"
    ]).

t_request_timeout_cleanup(_Config) ->
    {Server, Port} = start_fake_server(request_timeout),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    Parent = self(),
    spawn(fun() ->
        Parent !
            {request_result,
                enats_client:request(Client, <<"$JS.API.TEST">>, <<"{}">>, #{timeout => 20})}
    end),
    receive
        {fake_request_sub, Server} -> ok
    after 1000 -> ct:fail(request_sub_not_seen)
    end,
    receive
        {request_result, {error, timeout}} -> ok
    after 1000 -> ct:fail(request_timeout_not_seen)
    end,
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_request_infinity(_Config) ->
    {Server, Port} = start_fake_server(request_infinity),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    Parent = self(),
    spawn(fun() ->
        Parent !
            {request_result,
                enats_client:request(Client, <<"$JS.API.TEST">>, <<"{}">>, #{timeout => infinity})}
    end),
    receive
        {fake_request_infinity, Server} -> ok
    after 1000 -> ct:fail(request_infinity_not_seen)
    end,
    receive
        {request_result, {ok, #{payload := <<"{}">>}}} -> ok;
        {request_result, OtherResult} -> ct:fail({unexpected_request_result, OtherResult})
    after 1000 -> ct:fail(request_infinity_not_returned)
    end,
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_jetstream_no_responders(_Config) ->
    {Server, Port} = start_fake_server(jetstream_no_responders),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:enable_diagnostics(Client, #{message_sample_every => 1}),
    ok = enats_client:connect(Client),
    ?assertEqual(
        {error, {jetstream, unavailable, <<"503">>}},
        enats_client:jetstream_publish(Client, <<"orders.test">>, <<"payload">>, #{timeout => 1000})
    ),
    {ok, Snapshot} = enats_client:diagnostics(Client),
    ?assert(maps:is_key(jetstream_publish_latency, maps:get(latencies, Snapshot))),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_tls_downgrade_rejected(_Config) ->
    {Server, Port} = start_fake_server(tls_unavailable),
    {ok, Client} = enats_client:start_link(#{
        host => "127.0.0.1", port => Port, tls => true, owner => self()
    }),
    ?assertEqual({error, {tls_upgrade_failed, tls_not_available}}, enats_client:connect(Client)),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_jetstream_json_unavailable(_Config) ->
    {Server, Port} = start_fake_server(jetstream_json_unavailable),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    ?assertMatch(
        {error, {jetstream, unavailable, 503}},
        enats_client:jetstream_publish(Client, <<"orders.test">>, <<"payload">>, #{timeout => 1000})
    ),
    ok = enats_client:stop(Client),
    exit(Server, normal).

encode_seed(PrivateSeed) ->
    Prefix = <<(16#90 bor (16#A0 bsr 5)), ((16#A0 band 31) bsl 3), PrivateSeed/binary>>,
    encode_base32(<<Prefix/binary, (test_crc16(Prefix)):16/little>>).

test_crc16(Bin) -> test_crc16(Bin, 0).
test_crc16(<<>>, Crc) ->
    Crc;
test_crc16(<<Byte, Rest/binary>>, Crc0) ->
    Crc1 = Crc0 bxor (Byte bsl 8),
    test_crc16(Rest, test_crc_byte(Crc1, 8)).

test_crc_byte(Crc, 0) ->
    Crc band 16#FFFF;
test_crc_byte(Crc, N) when Crc band 16#8000 =/= 0 ->
    test_crc_byte(((Crc bsl 1) bxor 16#1021) band 16#FFFF, N - 1);
test_crc_byte(Crc, N) ->
    test_crc_byte((Crc bsl 1) band 16#FFFF, N - 1).

encode_base32(Bits) ->
    encode_base32(Bits, []).

encode_base32(<<Value:5, Rest/bitstring>>, Acc) ->
    encode_base32(Rest, [lists:nth(Value + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]);
encode_base32(Bits, Acc) when bit_size(Bits) > 0 ->
    Size = bit_size(Bits),
    <<Value:Size>> = Bits,
    Padded = Value bsl (5 - Size),
    encode_base32(<<>>, [lists:nth(Padded + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]);
encode_base32(<<>>, Acc) ->
    list_to_binary(lists:reverse(Acc)).

start_nats_server(ConfigFile, PidFile) ->
    start_nats_process(["-DV", "-P", PidFile, "-c", ConfigFile]).

start_nats_process(Args) ->
    {ok, Executable} = nats_server_executable(),
    Port = open_port(
        {spawn_executable, Executable},
        [{args, Args}, exit_status, use_stdio, stderr_to_stdout]
    ),
    timer:sleep(200),
    Port.

drain_nats(Port) ->
    drain_nats(Port, []).

drain_nats(Port, Acc) ->
    receive
        {Port, {data, Data}} -> drain_nats(Port, [Data | Acc])
    after 0 -> lists:reverse(Acc)
    end.

dynamic_port() ->
    {ok, Socket} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Address, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Port.

wait_for_port(Port) ->
    wait_for_port(Port, 50).

wait_for_port(_Port, 0) ->
    ct:fail(nats_server_not_ready);
wait_for_port(Port, Attempts) ->
    case gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 50) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, _} ->
            timer:sleep(20),
            wait_for_port(Port, Attempts - 1)
    end.

stop_nats_server(Port, PidFile) ->
    case file:read_file(PidFile) of
        {ok, PidBin} ->
            Pid = string:trim(binary_to_list(PidBin)),
            _ = os:cmd("kill -TERM " ++ Pid),
            timer:sleep(100);
        {error, _} ->
            ok
    end,
    catch port_close(Port),
    _ = file:delete(PidFile),
    ok.

maybe_stop_nats_server(undefined, undefined) -> ok;
maybe_stop_nats_server(Port, PidFile) -> stop_nats_server(Port, PidFile).

nats_server_executable() ->
    case os:getenv("ENATS_USE_NATS_SERVICE") of
        "true" ->
            unavailable;
        _ ->
            case os:find_executable("nats-server") of
                false -> unavailable;
                Executable -> {ok, Executable}
            end
    end.

start_fake_server(Mode) ->
    Parent = self(),
    Pid = spawn_link(fun() -> fake_server(Parent, Mode) end),
    receive
        {fake_server_ready, Pid, Port} -> {Pid, Port}
    after 1000 -> ct:fail(fake_server_not_ready)
    end.

fake_server(Parent, Mode) ->
    ListenOpts =
        case Mode of
            ipv6 -> [inet6, binary, {active, false}, {reuseaddr, true}];
            _ -> [binary, {active, false}, {reuseaddr, true}]
        end,
    {ok, Listener} = gen_tcp:listen(0, ListenOpts),
    {ok, {_Address, Port}} = inet:sockname(Listener),
    Parent ! {fake_server_ready, self(), Port},
    case Mode of
        reconnect ->
            fake_reconnect(Listener, Parent);
        stale_reconnect ->
            fake_stale_reconnect(Listener, Parent);
        _ ->
            {ok, Socket} = gen_tcp:accept(Listener),
            Parent ! {fake_server_accepted, self(), Mode},
            case Mode of
                silent -> ok;
                topology -> ok = gen_tcp:send(Socket, fake_topology_info());
                server_limits -> ok = gen_tcp:send(Socket, fake_limits_info());
                _ -> ok = gen_tcp:send(Socket, fake_info())
            end,
            InitialData =
                case gen_tcp:recv(Socket, 0, 1000) of
                    {ok, Data} -> Data;
                    {error, closed} -> <<>>
                end,
            case Mode of
                server_error ->
                    ok = gen_tcp:send(Socket, <<"-ERR \"bad\"\r\n">>);
                close_without_pong ->
                    ok;
                silent ->
                    timer:sleep(500);
                no_pong ->
                    timer:sleep(500);
                topology ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>);
                server_limits ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    timer:sleep(500);
                coalesced_flush ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\nPING\r\n">>),
                    {ok, _ClientData} = recv_until(Socket, <<"PING\r\n">>, <<>>),
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    timer:sleep(500);
                flush_concurrent ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, _FirstFlushData} = recv_until(Socket, <<"PING\r\n">>, <<>>),
                    Parent ! {fake_flush_received, self()},
                    {ok, _SecondFlushData} = recv_until(Socket, <<"PING\r\n">>, <<>>),
                    Parent ! {fake_second_flush_received, self()},
                    receive
                        release_first_flush -> ok
                    end,
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    receive
                        release_second_flush -> ok
                    end,
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>);
                flush_timeout_order ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, _FirstFlushData} = recv_until(Socket, <<"PING\r\n">>, <<>>),
                    Parent ! {fake_flush_received, self()},
                    {ok, _SecondFlushData} = recv_until(Socket, <<"PING\r\n">>, <<>>),
                    Parent ! {fake_second_flush_received, self()},
                    receive
                        release_timed_out_flush -> ok
                    end,
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    receive
                        release_second_flush -> ok
                    end,
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>);
                flush_disconnect ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, _FirstFlushData} = recv_until(Socket, <<"PING\r\n">>, <<>>),
                    Parent ! {fake_flush_received, self()},
                    {ok, _SecondFlushData} = recv_until(Socket, <<"PING\r\n">>, <<>>),
                    Parent ! {fake_second_flush_received, self()};
                flush_timeout ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, _Ignored} = gen_tcp:recv(Socket, 0, 1000),
                    timer:sleep(500);
                request_timeout ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, _Sub} = recv_until(Socket, <<"SUB ">>, InitialData),
                    Parent ! {fake_request_sub, self()},
                    {ok, _Pub} = recv_until(Socket, <<"PUB ">>, InitialData),
                    timer:sleep(100);
                request_infinity ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, SubData0} = recv_until(Socket, <<"SUB ">>, InitialData),
                    {ok, SubData} = recv_until(Socket, <<"\r\n">>, SubData0),
                    [_, _Inbox, Sid | _] = binary:split(find_sub_line(SubData), <<" ">>, [global]),
                    {ok, PubData0} = recv_until(Socket, <<"PUB ">>, SubData),
                    {ok, PubData} = recv_until(Socket, <<"\r\n">>, PubData0),
                    [_, _RequestSubject, ReplyTo | _] = binary:split(
                        find_pub_line(PubData), <<" ">>, [global]
                    ),
                    Parent ! {fake_request_infinity, self()},
                    Payload = <<"{}">>,
                    ok = gen_tcp:send(Socket, [
                        <<"MSG ">>,
                        ReplyTo,
                        <<" ">>,
                        Sid,
                        <<" ">>,
                        integer_to_binary(byte_size(Payload)),
                        <<"\r\n">>,
                        Payload,
                        <<"\r\n">>
                    ]),
                    timer:sleep(100);
                jetstream_no_responders ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, SubData0} = recv_until(Socket, <<"SUB ">>, InitialData),
                    {ok, SubData} = recv_until(Socket, <<"\r\n">>, SubData0),
                    [_, _Inbox, Sid | _] = binary:split(find_sub_line(SubData), <<" ">>, [global]),
                    {ok, PubData0} = recv_until(Socket, <<"PUB ">>, SubData),
                    {ok, PubData} = recv_until(Socket, <<"\r\n">>, PubData0),
                    [_, _RequestSubject, ReplyTo | _] = binary:split(
                        find_pub_line(PubData), <<" ">>, [global]
                    ),
                    Header = <<"NATS/1.0\r\nStatus: 503\r\n\r\n">>,
                    HeaderSize = byte_size(Header),
                    ok = gen_tcp:send(Socket, [
                        <<"HMSG ">>,
                        ReplyTo,
                        <<" ">>,
                        Sid,
                        <<" ">>,
                        integer_to_binary(HeaderSize),
                        <<" ">>,
                        integer_to_binary(HeaderSize),
                        <<"\r\n">>,
                        Header,
                        <<"\r\n">>
                    ]),
                    timer:sleep(100);
                tls_unavailable ->
                    timer:sleep(100);
                ipv6 ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    timer:sleep(50);
                jetstream_json_unavailable ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, SubData0} = recv_until(Socket, <<"SUB ">>, InitialData),
                    {ok, SubData} = recv_until(Socket, <<"\r\n">>, SubData0),
                    [_, _Inbox, Sid | _] = binary:split(find_sub_line(SubData), <<" ">>, [global]),
                    {ok, PubData0} = recv_until(Socket, <<"PUB ">>, SubData),
                    {ok, PubData} = recv_until(Socket, <<"\r\n">>, PubData0),
                    [_, _RequestSubject, ReplyTo | _] = binary:split(
                        find_pub_line(PubData), <<" ">>, [global]
                    ),
                    Payload = <<"{\"error\":{\"code\":503,\"description\":\"temporary\"}}">>,
                    ok = gen_tcp:send(Socket, [
                        <<"MSG ">>,
                        ReplyTo,
                        <<" ">>,
                        Sid,
                        <<" ">>,
                        integer_to_binary(byte_size(Payload)),
                        <<"\r\n">>,
                        Payload,
                        <<"\r\n">>
                    ]),
                    timer:sleep(100)
            end,
            gen_tcp:close(Socket),
            gen_tcp:close(Listener)
    end.

find_sub_line(Data) ->
    [Line | _] = [
        L
     || L <- binary:split(Data, <<"\r\n">>, [global]), binary:match(L, <<"SUB ">>) =/= nomatch
    ],
    Line.

find_pub_line(Data) ->
    [Line | _] = [
        L
     || L <- binary:split(Data, <<"\r\n">>, [global]), binary:match(L, <<"PUB ">>) =/= nomatch
    ],
    Line.

fake_reconnect(Listener, Parent) ->
    {ok, First} = gen_tcp:accept(Listener),
    ok = gen_tcp:send(First, fake_info()),
    {ok, _FirstData} = gen_tcp:recv(First, 0, 1000),
    ok = gen_tcp:send(First, <<"PONG\r\n">>),
    ok = gen_tcp:send(First, <<"PING\r\n">>),
    {ok, _PongData} = recv_until(First, <<"PONG\r\n">>, <<>>),
    ok = gen_tcp:send(First, <<"PONG\r\n">>),
    timer:sleep(50),
    gen_tcp:close(First),
    {ok, Second} = gen_tcp:accept(Listener),
    ok = gen_tcp:send(Second, fake_info()),
    {ok, _SecondData} = recv_until(Second, <<"SUB reconnect.test">>, <<>>),
    ok = gen_tcp:send(Second, <<"PONG\r\n">>),
    Parent ! {fake_server_reconnected, self()},
    timer:sleep(100),
    gen_tcp:close(Second),
    gen_tcp:close(Listener).

fake_stale_reconnect(Listener, Parent) ->
    {ok, First} = gen_tcp:accept(Listener),
    ok = gen_tcp:send(First, fake_info()),
    {ok, InitialData} = gen_tcp:recv(First, 0, 1000),
    ok = gen_tcp:send(First, <<"PONG\r\n">>),
    {ok, _} = recv_until_nth(First, <<"PING\r\n">>, 2, InitialData),
    Parent ! {fake_stale_ping, self()},
    wait_socket_closed(First),
    {ok, Second} = gen_tcp:accept(Listener),
    ok = gen_tcp:send(Second, fake_info()),
    {ok, _SecondData} = gen_tcp:recv(Second, 0, 1000),
    ok = gen_tcp:send(Second, <<"PONG\r\n">>),
    Parent ! {fake_server_reconnected, self()},
    timer:sleep(100),
    gen_tcp:close(Second),
    gen_tcp:close(Listener).

fake_info() ->
    <<"INFO {\"proto\":1,\"headers\":true,\"max_payload\":1048576}\r\n">>.

fake_topology_info() ->
    <<"INFO {\"proto\":1,\"headers\":true,\"connect_urls\":[\"nats://127.0.0.1:14222\",\"bad\"],\"untrusted\":\"untrusted\"}\r\n">>.

fake_limits_info() ->
    <<"INFO {\"proto\":1,\"headers\":false,\"max_payload\":3}\r\n">>.

recv_until(Socket, Needle, Acc) ->
    case binary:match(Acc, Needle) of
        nomatch ->
            case gen_tcp:recv(Socket, 0, 1000) of
                {ok, Data} -> recv_until(Socket, Needle, <<Acc/binary, Data/binary>>);
                Error -> Error
            end;
        _ ->
            {ok, Acc}
    end.

recv_until_nth(Socket, Needle, N, Acc) ->
    case length(binary:matches(Acc, Needle)) >= N of
        true ->
            {ok, Acc};
        false ->
            case gen_tcp:recv(Socket, 0, 1000) of
                {ok, Data} -> recv_until_nth(Socket, Needle, N, <<Acc/binary, Data/binary>>);
                Error -> Error
            end
    end.

wait_socket_closed(Socket) ->
    case gen_tcp:recv(Socket, 0, 100) of
        {error, closed} -> ok;
        {ok, _Data} -> wait_socket_closed(Socket);
        {error, timeout} -> wait_socket_closed(Socket);
        Error -> ct:fail({unexpected_socket_result, Error})
    end.
