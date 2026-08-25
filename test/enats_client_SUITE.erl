-module(enats_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_frame_fragmentation/1, t_frame_invalid/1, t_frame_edges/1, t_connect_publish_subscribe_flush/1,
    t_default_owner/1, t_connect_idempotence/1, t_coalesced_flush/1, t_server_limits/1,
    t_flush_concurrency/1,
    t_headers/1, t_frame_variants/1, t_auth_helpers/1, t_nkey_helpers/1,
    t_secret_and_subject/1, t_invalid_subject/1, t_lazy_secret/1,
    t_user_password/1, t_tls/1, t_connection_errors/1, t_connection_queries/1,
    t_fake_connection_paths/1,
    t_reconnect/1, t_disconnect_while_connecting/1, t_topology_info/1, t_server_failover/1,
    t_nkey_nats_server/1, t_token_nats_server/1, t_jetstream_nats_server/1, t_nkey_seed/1]).

all() ->
    [t_frame_fragmentation, t_frame_invalid, t_frame_edges, t_connect_publish_subscribe_flush,
        t_default_owner, t_connect_idempotence, t_coalesced_flush, t_server_limits, t_flush_concurrency, t_headers,
        t_frame_variants, t_auth_helpers, t_nkey_helpers, t_secret_and_subject,
        t_invalid_subject, t_lazy_secret, t_user_password, t_tls, t_connection_errors,
        t_connection_queries,
        t_fake_connection_paths, t_reconnect, t_disconnect_while_connecting, t_topology_info,
        t_server_failover,
        t_nkey_nats_server, t_token_nats_server, t_jetstream_nats_server, t_nkey_seed].

init_per_suite(Config) ->
    application:ensure_all_started(enats_client),
    case nats_server_executable() of
        {ok, _Executable} ->
            PrivDir = proplists:get_value(priv_dir, Config),
            Port = dynamic_port(),
            PidFile = filename:join(PrivDir, "nats-base.pid"),
            Server = start_nats_process(["-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile]),
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
    ?assertEqual([{error, {unknown_frame, <<"BOGUS">>}}],
        element(1, enats_frame:parse(<<"BOGUS\r\n">>, enats_frame:initial_state()))).

t_frame_edges(_Config) ->
    {Frames1, Pending} = enats_frame:parse(<<"MSG foo 1 5\r\nhe">>, enats_frame:initial_state()),
    ?assertEqual([], Frames1),
    {Frames2, _} = enats_frame:parse(<<"llo\r\n+OK\r\n">>, Pending),
    ?assertEqual([#{type => msg, subject => <<"foo">>, sid => <<"1">>, payload => <<"hello">>}, ok], Frames2),
    {Frames3, _} = enats_frame:parse(<<"MSG foo 1 reply 5\r\nhello\r\n">>, enats_frame:initial_state()),
    ?assertEqual([#{type => msg, subject => <<"foo">>, sid => <<"1">>, reply_to => <<"reply">>, payload => <<"hello">>}], Frames3),
    {Frames4, _} = enats_frame:parse(<<"HMSG foo 1 3 3\r\nbad\r\n">>, enats_frame:initial_state()),
    ?assertEqual([#{type => hmsg, subject => <<"foo">>, sid => <<"1">>, header_size => 3,
        headers => [], payload => <<>>}], Frames4),
    _ = enats_frame:serialize_connect(#{<<"binary-key">> => true}),
    ok.

t_connect_publish_subscribe_flush(Config) ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => ?config(port, Config), owner => self()}),
    ok = enats_client:connect(Client),
    ?assertEqual(connected, enats_client:status(Client)),
    {ok, Subscription} = enats_client:subscribe(Client, <<"enats.test">>, #{}),
    ok = enats_client:publish(Client, <<"enats.test">>, <<"hello">>),
    ok = enats_client:flush(Client, 1000),
    receive
        {enats_client, Client, {message, #{type := msg, subject := <<"enats.test">>, payload := <<"hello">>}}} -> ok
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
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => ?config(port, Config), owner => self()}),
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
    ?assertEqual({error, {payload_too_large, 3}},
        enats_client:publish(Client, <<"limits">>, <<"1234">>)),
    ?assertEqual({error, headers_not_supported},
        enats_client:publish(Client, <<"limits">>, <<"ok">>, #{headers => [{<<"x">>, <<"y">>}]})),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_flush_concurrency(_Config) ->
    {Server, Port} = start_fake_server(flush_concurrent),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(Client),
    Parent = self(),
    spawn(fun() -> Parent ! {first_flush, enats_client:flush(Client, 10000)} end),
    receive {fake_flush_received, Server} -> ok after 1000 -> ct:fail(first_flush_not_received) end,
    timer:sleep(50),
    ?assertEqual({error, flush_in_progress}, enats_client:flush(Client, 10000)),
    Server ! release_flush,
    receive {first_flush, ok} -> ok after 10000 -> ct:fail(first_flush_not_completed) end,
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_headers(Config) ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => ?config(port, Config), owner => self()}),
    ok = enats_client:connect(Client),
    {ok, _Subscription} = enats_client:subscribe(Client, <<"enats.headers">>, #{}),
    ok = enats_client:publish(Client, <<"enats.headers">>, <<"body">>,
        #{reply_to => <<"reply.subject">>, headers => [{<<"X-Test">>, <<"yes">>}, {<<"X-Test">>, <<"again">>}]}),
    ok = enats_client:flush(Client, 1000),
    receive
        {enats_client, Client, {message, #{type := hmsg, headers := Headers,
            reply_to := <<"reply.subject">>, payload := <<"body">>}}} ->
            ?assertEqual([{<<"X-Test">>, <<"yes">>}, {<<"X-Test">>, <<"again">>}], Headers)
    after 1000 -> ct:fail(header_message_not_received)
    end,
    ok = enats_client:stop(Client).

t_frame_variants(_Config) ->
    Connect = iolist_to_binary(enats_frame:serialize_connect(#{protocol => 1, headers => true})),
    ?assertMatch(<<"CONNECT ", _/binary>>, Connect),
    ?assertMatch(<<"PUB foo 3\r\nbar\r\n">>, iolist_to_binary(enats_frame:serialize({pub, <<"foo">>, <<"bar">>}))),
    ?assertMatch(<<"PUB foo reply 3\r\nbar\r\n">>,
        iolist_to_binary(enats_frame:serialize({pub, <<"foo">>, <<"reply">>, <<"bar">>}))),
    Hpub = iolist_to_binary(enats_frame:serialize({hpub, <<"foo">>, undefined,
        [{<<"X">>, <<"Y">>}], <<"bar">>})),
    ?assertMatch(<<"HPUB foo ", _/binary>>, Hpub),
    HpubReply = iolist_to_binary(enats_frame:serialize({hpub, <<"foo">>, <<"reply">>,
        [{<<"X">>, <<"Y">>}], <<"bar">>})),
    ?assertMatch(<<"HPUB foo reply ", _/binary>>, HpubReply),
    ?assertMatch(<<"SUB foo 1\r\n">>, iolist_to_binary(enats_frame:serialize({sub, <<"foo">>, <<"1">>, undefined}))),
    ?assertMatch(<<"SUB foo q 1\r\n">>, iolist_to_binary(enats_frame:serialize({sub, <<"foo">>, <<"1">>, <<"q">>}))),
    ?assertMatch(<<"UNSUB 1\r\n">>, iolist_to_binary(enats_frame:serialize({unsub, <<"1">>}))),
    Hmsg = <<"HMSG foo 1 18 21\r\nNATS/1.0\r\nX: Y\r\n\r\nbar\r\n">>,
    {Frames, _} = enats_frame:parse(Hmsg, enats_frame:initial_state()),
    ?assertEqual([#{type => hmsg, subject => <<"foo">>, sid => <<"1">>,
        header_size => 18, headers => [{<<"X">>, <<"Y">>}], payload => <<"bar">>}], Frames).

t_auth_helpers(_Config) ->
    {ok, #{}} = enats_auth:connect_params(none, #{}, #{}),
    ?assertEqual(none, enats_auth:describe(none)),
    ?assertEqual(user_password, enats_auth:describe(#{mechanism => user_password})),
    {ok, UserParams} = enats_auth:connect_params(
        #{mechanism => user_password, username => <<"alice">>, password => (fun() -> <<"secret">> end)},
        #{}, #{}),
    ?assertEqual(<<"alice">>, maps:get(user, UserParams)),
    ?assertEqual(<<"secret">>, maps:get(pass, UserParams)),
    {ok, TokenParams} = enats_auth:connect_params(
        #{mechanism => token, token => (fun() -> {ok, <<"token">>} end)}, #{}, #{}),
    ?assertEqual(<<"token">>, maps:get(auth_token, TokenParams)),
    ?assertEqual({error, invalid_secret_type}, enats_auth:connect_params(
        #{mechanism => token, token => 42}, #{}, #{})),
    ?assertEqual({error, nkey_nonce_missing}, enats_auth:connect_params(
        #{mechanism => nkey, public_key => <<"key">>, sign_fun => (fun(_) -> <<"sig">> end)}, #{}, #{})),
    {ok, NkeyParams} = enats_auth:connect_params(
        #{mechanism => nkey, public_key => <<"key">>, sign_fun => (fun(<<"nonce">>) -> <<"sig">> end)},
        #{nonce => <<"nonce">>}, #{}),
    ?assertEqual(<<"sig">>, maps:get(sig, NkeyParams)),
    ?assertEqual({error, {nkey_sign_failed, bad}}, enats_auth:connect_params(
        #{mechanism => nkey, public_key => <<"key">>, sign_fun => (fun(_) -> {error, bad} end)},
        #{nonce => <<"nonce">>}, #{})),
    ?assertEqual({error, {invalid_nkey_signature, bad}}, enats_auth:connect_params(
        #{mechanism => nkey, public_key => <<"key">>, sign_fun => (fun(_) -> bad end)},
        #{nonce => <<"nonce">>}, #{})),
    ?assertMatch({error, {secret_provider_failed, error, bad}}, enats_auth:connect_params(
        #{mechanism => token, token => (fun() -> erlang:error(bad) end)}, #{}, #{})).

t_nkey_helpers(_Config) ->
    {Public, Private} = crypto:generate_key(eddsa, ed25519),
    Encoded = enats_nkey:encode_public(Public),
    ?assertEqual(56, byte_size(Encoded)),
    SignFun = enats_nkey:sign_fun(Public, Private),
    UrlSignature = SignFun(<<"nonce">>),
    StandardSignature = binary:replace(binary:replace(UrlSignature, <<"-">>, <<"+">>, [global]), <<"_">>, <<"/">>, [global]),
    Signature = base64:decode(<<StandardSignature/binary, "==">>),
    ?assert(crypto:verify(eddsa, none, <<"nonce">>, Signature, [Public, ed25519])).

t_secret_and_subject(_Config) ->
    ?assertEqual({ok, <<"value">>}, enats_secret:resolve(<<"value">>)),
    ?assertEqual({ok, <<"value">>}, enats_secret:resolve(fun() -> {ok, <<"value">>} end)),
    ?assertMatch({error, {secret_provider_failed, error, bad}}, enats_secret:resolve(fun() -> erlang:error(bad) end)),
    ?assertEqual(#{password => <<"******">>, nested => [#{token => <<"******">>}]},
        enats_secret:redact(#{password => <<"secret">>, nested => [#{token => <<"token">>}]})),
    ?assertEqual(value, enats_secret:redact(value)),
    ?assertEqual(ok, enats_subject:validate(<<"foo.bar">>)),
    ?assertEqual({error, invalid_subject}, enats_subject:validate(<<>>)),
    ?assertEqual({error, invalid_subject}, enats_subject:validate(<<"foo bar">>)),
    ?assertEqual(ok, enats_subject:validate_publish(<<"foo.bar">>)),
    ?assertEqual({error, wildcard_subject_not_allowed}, enats_subject:validate_publish(<<"foo.*">>)).

t_invalid_subject(_Config) ->
    {ok, Client} = enats_client:start_link(#{owner => self()}),
    ?assertEqual({error, {invalid_subject, invalid_subject}},
        enats_client:publish(Client, <<"bad subject">>, <<"payload">>)),
    ok = enats_client:stop(Client).

t_lazy_secret(_Config) ->
    Parent = self(),
    Provider = fun() -> Parent ! secret_called, <<"password">> end,
    Auth = #{mechanism => user_password, username => <<"user">>, password => Provider},
    {ok, Client} = enats_client:start_link(#{auth => Auth, owner => self()}),
    receive secret_called -> ct:fail(secret_evaluated_before_connect) after 50 -> ok end,
    ok = enats_client:stop(Client).

t_user_password(Config) ->
    case os:getenv("ENATS_AUTH_PORT") of
        false ->
            case nats_server_executable() of
                unavailable -> {skip, "nats-server executable is unavailable"};
                {ok, _} ->
                    Port = dynamic_port(),
                    PidFile = filename:join(?config(priv_dir, Config), "nats-userpass.pid"),
                    Server = start_nats_process(["-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile,
                        "--user", "alice", "--pass", "secret"]),
                    wait_for_port(Port),
                    try user_password_case(Port) after stop_nats_server(Server, PidFile) end
            end;
        Port0 ->
            user_password_case(list_to_integer(Port0))
    end.

t_tls(Config) ->
    case os:getenv("ENATS_TLS_PORT") of
        false ->
            case nats_server_executable() of
                unavailable -> {skip, "nats-server executable is unavailable"};
                {ok, _} ->
                    PrivDir = ?config(priv_dir, Config),
                    Port = dynamic_port(),
                    PidFile = filename:join(PrivDir, "nats-tls.pid"),
                    CertFile = filename:join(PrivDir, "nats-tls.crt"),
                    KeyFile = filename:join(PrivDir, "nats-tls.key"),
                    generate_test_certificate(CertFile, KeyFile),
                    Server = start_nats_process(["-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile,
                        "--tls", "--tlscert", CertFile, "--tlskey", KeyFile]),
                    wait_for_port(Port),
                    try tls_case(Port) after stop_nats_server(Server, PidFile) end
            end;
        Port0 ->
            tls_case(list_to_integer(Port0))
    end.

user_password_case(Port) ->
    Parent = self(),
    Provider = fun() -> Parent ! password_called, <<"secret">> end,
    Auth = #{mechanism => user_password, username => <<"alice">>, password => Provider},
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, auth => Auth, owner => self()}),
    ok = enats_client:connect(Client),
    receive password_called -> ok after 1000 -> ct:fail(secret_provider_not_called) end,
    ok = enats_client:publish(Client, <<"enats.auth">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client).

tls_case(Port) ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port,
        tls => true, ssl_opts => [{verify, verify_none}], owner => self()}),
    ok = enats_client:connect(Client),
    ok = enats_client:publish(Client, <<"enats.tls">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client).

generate_test_certificate(CertFile, KeyFile) ->
    Command = lists:flatten(io_lib:format(
        "openssl req -x509 -newkey rsa:2048 -nodes -keyout ~ts -out ~ts -subj /CN=localhost -days 1 >/dev/null 2>&1",
        [KeyFile, CertFile])),
    _ = os:cmd(Command),
    {ok, _} = file:read_file(CertFile),
    {ok, _} = file:read_file(KeyFile),
    ok.

t_connection_errors(_Config) ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => 1, owner => self()}),
    ?assertEqual(disconnected, enats_client:status(Client)),
    ?assertEqual(#{}, enats_client:info(Client)),
    ?assertMatch({error, _}, enats_client:connect(Client)),
    ok = enats_client:disconnect(Client),
    ok = enats_client:stop(Client),
    ok = enats_client_app:stop(ok).

t_connection_queries(Config) ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => ?config(port, Config), owner => self()}),
    ?assertEqual({error, disconnected}, enats_client:publish(Client, <<"valid.subject">>, <<"payload">>)),
    ok = enats_client:connect(Client),
    Info = enats_client:info(Client),
    ?assertEqual(?config(port, Config), maps:get(port, Info)),
    ?assertEqual({error, {invalid_subject, invalid_subject}},
        enats_connection:publish(Client, <<"bad subject">>, <<"payload">>, #{})),
    ?assertEqual({error, {invalid_subject, invalid_subject}},
        enats_client:subscribe(Client, <<"bad subject">>, #{})),
    ?assertEqual({error, timeout},
        enats_client:request(Client, <<"no.reply">>, <<"payload">>, #{timeout => 20})),
    ?assertMatch({error, _}, enats_js:publish(self(), <<"subject">>, <<"payload">>, #{})),
    ?assertEqual({error, not_found}, enats_client:unsubscribe(Client, make_ref())),
    ok = enats_client:disconnect(Client),
    ok = enats_client:stop(Client),
    {Server, Port} = start_fake_server(flush_timeout),
    {ok, FlushClient} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
    ok = enats_client:connect(FlushClient),
    ?assertEqual({error, timeout}, enats_client:flush(FlushClient, 20)),
    ok = enats_client:stop(FlushClient),
    exit(Server, normal).

t_fake_connection_paths(_Config) ->
    {ErrorServer, ErrorPort} = start_fake_server(server_error),
    {ok, ErrorClient} = enats_client:start_link(#{host => "127.0.0.1", port => ErrorPort, owner => self()}),
    ?assertEqual({error, {server_error, <<"\"bad\"">>}}, enats_client:connect(ErrorClient)),
    ok = enats_client:stop(ErrorClient),
    exit(ErrorServer, normal),
    {CloseServer, ClosePort} = start_fake_server(close_without_pong),
    {ok, CloseClient} = enats_client:start_link(#{host => "127.0.0.1", port => ClosePort, owner => self()}),
    ?assertEqual({error, closed}, enats_client:connect(CloseClient)),
    ok = enats_client:stop(CloseClient),
    exit(CloseServer, normal).

t_reconnect(_Config) ->
    {Server, Port} = start_fake_server(reconnect),
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port,
        reconnect => true, reconnect_delay => 20, owner => self()}),
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

t_disconnect_while_connecting(_Config) ->
    {InfoServer, InfoPort} = start_fake_server(silent),
    {ok, InfoClient} = enats_client:start_link(#{host => "127.0.0.1", port => InfoPort, owner => self()}),
    Parent = self(),
    _ConnectCaller = spawn(fun() -> Parent ! {connect_result, enats_client:connect(InfoClient)} end),
    receive {fake_server_accepted, InfoServer, silent} -> ok after 1000 -> ct:fail(fake_info_server_not_accepted) end,
    ?assertEqual(connecting, enats_client:status(InfoClient)),
    ?assertEqual({error, connecting}, enats_client:info(InfoClient)),
    ok = enats_client:disconnect(InfoClient),
    receive {connect_result, {error, disconnected}} -> ok after 1000 -> ct:fail(connect_call_not_replied) end,
    ok = enats_client:stop(InfoClient),
    exit(InfoServer, normal),
    {PongServer, PongPort} = start_fake_server(no_pong),
    {ok, PongClient} = enats_client:start_link(#{host => "127.0.0.1", port => PongPort, owner => self()}),
    _PongCaller = spawn(fun() -> Parent ! {pong_connect_result, enats_client:connect(PongClient)} end),
    receive {fake_server_accepted, PongServer, no_pong} -> ok after 1000 -> ct:fail(fake_pong_server_not_accepted) end,
    ?assertEqual(connecting, enats_client:status(PongClient)),
    ?assertEqual({error, connecting}, enats_client:info(PongClient)),
    ok = enats_client:disconnect(PongClient),
    receive {pong_connect_result, {error, disconnected}} -> ok after 1000 -> ct:fail(pong_call_not_replied) end,
    ok = enats_client:stop(PongClient),
    exit(PongServer, normal).

t_topology_info(_Config) ->
    {Server, Port} = start_fake_server(topology),
    {ok, Client} = enats_client:start_link(#{servers => [{"127.0.0.1", Port}], owner => self()}),
    ok = enats_client:connect(Client),
    Info = enats_client:info(Client),
    ?assertEqual([<<"nats://127.0.0.1:14222">>, <<"bad">>], maps:get(connect_urls, Info)),
    ?assertEqual(<<"untrusted">>, maps:get(<<"untrusted">>, Info)),
    ?assertEqual(false, lists:keymember(untrusted, 1, maps:to_list(Info))),
    ok = enats_client:stop(Client),
    exit(Server, normal).

t_server_failover(Config) ->
    {ok, Client} = enats_client:start_link(#{servers => [{"127.0.0.1", 1}, {"127.0.0.1", ?config(port, Config)}], owner => self()}),
    ok = enats_client:connect(Client),
    ?assertEqual(connected, enats_client:status(Client)),
    ok = enats_client:stop(Client).

t_nkey_nats_server(Config) ->
    case nats_server_executable() of
        unavailable -> {skip, "nats-server executable is unavailable"};
        {ok, _} -> t_nkey_nats_server_impl(Config)
    end.

t_nkey_nats_server_impl(Config) ->
    {PublicKey, PrivateKey} = crypto:generate_key(eddsa, ed25519),
    PublicNKey = enats_nkey:encode_public(PublicKey),
    Port = dynamic_port(),
    ConfigFile = filename:join(?config(priv_dir, Config), "nats-nkey.conf"),
    PidFile = filename:join(?config(priv_dir, Config), "nats-nkey.pid"),
    ConfigText = iolist_to_binary([
        "port: ", integer_to_list(Port), "\n",
        "authorization {\n  users = [{nkey: \"", PublicNKey, "\"}]\n}\n"
    ]),
    ok = file:write_file(ConfigFile, ConfigText),
    NatsServer = start_nats_server(ConfigFile, PidFile),
    wait_for_port(Port),
    Auth = #{mechanism => nkey, public_key => PublicNKey,
        sign_fun => enats_nkey:sign_fun(PublicKey, PrivateKey)},
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port,
        auth => Auth, owner => self()}),
    case enats_client:connect(Client) of
        ok -> ok;
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
    NatsServer = start_nats_process(["-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile, "--auth", "token"]),
    wait_for_port(Port),
    Auth = #{mechanism => token, token => (fun() -> {ok, <<"token">>} end)},
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port,
        auth => Auth, owner => self()}),
    ok = enats_client:connect(Client),
    ok = enats_client:publish(Client, <<"enats.token">>, <<"ok">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:stop(Client),
    stop_nats_server(NatsServer, PidFile).

t_jetstream_nats_server(Config) ->
    case nats_server_executable() of
        unavailable -> {skip, "nats-server executable is unavailable"};
        {ok, _} ->
            Port = dynamic_port(),
            PidFile = filename:join(?config(priv_dir, Config), "nats-jetstream.pid"),
            Server = start_nats_process(["-a", "127.0.0.1", "-p", integer_to_list(Port),
                "-P", PidFile, "-js"]),
            wait_for_port(Port),
            try
                {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => Port, owner => self()}),
                ok = enats_client:connect(Client),
                StreamConfig = jsx:encode(#{<<"name">> => <<"ORDERS">>,
                    <<"subjects">> => [<<"orders.>">>]}),
                {ok, #{payload := _CreateAck}} = enats_client:request(
                    Client, <<"$JS.API.STREAM.CREATE.ORDERS">>, StreamConfig, #{timeout => 2000}
                ),
                {ok, Ack1} = enats_client:jetstream_publish(
                    Client, <<"orders.test">>, <<"hello">>,
                    #{msg_id => <<"id-1">>, timeout => 2000}
                ),
                {ok, Ack2} = enats_client:jetstream_publish(
                    Client, <<"orders.test">>, <<"hello">>,
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
    Seed = encode_seed(<<1:256>>),
    {ok, PublicKey, SignFun} = enats_nkey:from_seed(Seed),
    ?assertEqual(56, byte_size(PublicKey)),
    ?assertEqual(86, byte_size(SignFun(<<"nonce">>))),
    {ok, #{jwt := <<"jwt">>, nkey := <<"PUB">>, sig := <<"SIG">>}} = enats_auth:connect_params(
        #{mechanism => jwt, jwt => fun() -> <<"jwt">> end, public_key => <<"PUB">>,
            sign_fun => fun(<<"nonce">>) -> <<"SIG">> end},
        #{nonce => <<"nonce">>},
        #{}
    ),
    ?assertEqual({error, invalid_nkey_seed}, enats_nkey:from_seed(<<"bad">>)).

encode_seed(PrivateSeed) ->
    encode_base32(<<16#90, 16#A0, PrivateSeed/binary, 0:16/little>>).

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
    Port = open_port({spawn_executable, Executable},
        [{args, Args}, exit_status, use_stdio, stderr_to_stdout]),
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
        {ok, Socket} -> gen_tcp:close(Socket), ok;
        {error, _} -> timer:sleep(20), wait_for_port(Port, Attempts - 1)
    end.

stop_nats_server(Port, PidFile) ->
    case file:read_file(PidFile) of
        {ok, PidBin} ->
            Pid = string:trim(binary_to_list(PidBin)),
            _ = os:cmd("kill -TERM " ++ Pid),
            timer:sleep(100);
        {error, _} -> ok
    end,
    catch port_close(Port),
    _ = file:delete(PidFile),
    ok.

maybe_stop_nats_server(undefined, undefined) -> ok;
maybe_stop_nats_server(Port, PidFile) -> stop_nats_server(Port, PidFile).

nats_server_executable() ->
    case os:getenv("ENATS_USE_NATS_SERVICE") of
        "true" -> unavailable;
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
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Address, Port}} = inet:sockname(Listener),
    Parent ! {fake_server_ready, self(), Port},
    case Mode of
        reconnect -> fake_reconnect(Listener, Parent);
        _ ->
            {ok, Socket} = gen_tcp:accept(Listener),
            Parent ! {fake_server_accepted, self(), Mode},
            case Mode of
                silent -> ok;
                topology -> ok = gen_tcp:send(Socket, fake_topology_info());
                server_limits -> ok = gen_tcp:send(Socket, fake_limits_info());
                _ -> ok = gen_tcp:send(Socket, fake_info())
            end,
            _ = gen_tcp:recv(Socket, 0, 1000),
            case Mode of
                server_error -> ok = gen_tcp:send(Socket, <<"-ERR \"bad\"\r\n">>);
                close_without_pong -> ok;
                silent -> timer:sleep(500);
                no_pong -> timer:sleep(500);
                topology -> ok = gen_tcp:send(Socket, <<"PONG\r\n">>);
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
                    {ok, _FlushData} = gen_tcp:recv(Socket, 0, 1000),
                    Parent ! {fake_flush_received, self()},
                    receive release_flush -> ok end,
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>);
                flush_timeout ->
                    ok = gen_tcp:send(Socket, <<"PONG\r\n">>),
                    {ok, _Ignored} = gen_tcp:recv(Socket, 0, 1000),
                    timer:sleep(500)
            end,
            gen_tcp:close(Socket),
            gen_tcp:close(Listener)
    end.

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
        _ -> {ok, Acc}
    end.
