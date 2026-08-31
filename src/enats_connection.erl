-module(enats_connection).
-behaviour(gen_statem).

-export([start_link/1, connect/1, connect/2, disconnect/1, stop/1, status/1, info/1,
    publish/4, request/4, subscribe/3, unsubscribe/2, flush/2]).
-export([init/1, callback_mode/0, terminate/3, disconnected/3, waiting_info/3,
    waiting_pong/3, connected/3, reconnecting/3]).

-define(TIMEOUT, 5000).

start_link(Options) ->
    case validate_options(Options) of
        ok -> gen_statem:start_link(?MODULE, Options, []);
        Error -> Error
    end.
connect(Pid) -> connect(Pid, ?TIMEOUT).
connect(Pid, Timeout) -> safe_call(Pid, {connect, Timeout}, Timeout).
disconnect(Pid) -> gen_statem:call(Pid, disconnect, ?TIMEOUT).
stop(Pid) -> gen_statem:stop(Pid).
status(Pid) -> gen_statem:call(Pid, status, ?TIMEOUT).
info(Pid) -> gen_statem:call(Pid, info, ?TIMEOUT).
publish(Pid, Subject, Payload, Options) ->
    safe_call(Pid, {publish, Subject, Payload, Options}, maps:get(timeout, Options, ?TIMEOUT)).
request(Pid, Subject, Payload, Options) ->
    safe_call(Pid, {request, Subject, Payload, Options}, maps:get(timeout, Options, ?TIMEOUT)).
subscribe(Pid, Subject, Options) -> gen_statem:call(Pid, {subscribe, Subject, Options}, ?TIMEOUT).
unsubscribe(Pid, Ref) -> gen_statem:call(Pid, {unsubscribe, Ref}, ?TIMEOUT).
flush(Pid, Timeout) -> safe_call(Pid, {flush, Timeout}, Timeout).

safe_call(Pid, Request, Timeout) ->
    CallTimeout = case Timeout of infinity -> infinity; Value -> Value + 1000 end,
    try gen_statem:call(Pid, Request, CallTimeout) of
        Result -> Result
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _} -> {error, disconnected};
        exit:Reason -> {error, {client_exit, Reason}}
    end.

callback_mode() -> state_functions.

init(Options0) ->
    process_flag(trap_exit, true),
    Options = normalize_options(Options0),
    {ok, disconnected, #{
        options => Options,
        socket => undefined,
        parse_state => enats_frame:initial_state(),
        server_info => #{},
        connect_from => undefined,
        connect_attempts => 0,
        pending_connect_timeout => undefined,
        flushes => queue:new(),
        subscriptions => #{},
        requests => #{},
        next_sid => 1,
        servers => maps:get(servers, Options),
        server_index => 1,
        current_server => undefined,
        heartbeat_timer => undefined,
        pings_out => 0
    }}.

disconnected({call, From}, connect, State) ->
    connect_call(From, maps:get(connect_timeout, maps:get(options, State)), State);
disconnected({call, From}, {connect, Timeout}, State) ->
    connect_call(From, Timeout, State);
disconnected({call, From}, status, _State) -> reply(From, disconnected);
disconnected({call, From}, info, State) -> reply(From, maps:get(server_info, State));
disconnected({call, From}, disconnect, _State) -> reply(From, ok);
disconnected({call, From}, _Request, _State) -> reply(From, {error, disconnected});
disconnected(_, _, State) -> keep(State).

connect_call(From, Timeout, State) ->
    Options = maps:get(options, State),
    Attempts = length(maps:get(servers, State)),
    State0 = State#{connect_from => From, connect_attempts => Attempts,
        pending_connect_timeout => Timeout},
    case open_socket(State0, Attempts, Options, Timeout) of
        {ok, Socket, State1} ->
            {next_state, waiting_info, State1#{socket => Socket,
                connect_attempts => max(Attempts - 1, 0),
                parse_state => enats_frame:initial_state()}, [{state_timeout, Timeout, connect_timeout}]};
        {error, Reason, State1} ->
            {keep_state, clear_pending_connect(State1), reply_action(From, {error, Reason})}
    end.

waiting_info(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) -> process_data(Data, waiting_info, State);
waiting_info(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) -> process_data(Data, waiting_info, State);
waiting_info(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) -> connect_failed(waiting_info, closed, State);
waiting_info(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) -> connect_failed(waiting_info, closed, State);
waiting_info(state_timeout, connect_timeout, State) -> connect_failed(waiting_info, timeout, State);
waiting_info({call, From}, status, _State) -> reply(From, connecting);
waiting_info({call, From}, disconnect, State) ->
    reply_connect(State, {error, disconnected}),
    reply_requests(State, {error, disconnected}),
    close(State),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
waiting_info({call, From}, _Request, _State) -> reply(From, {error, connecting});
waiting_info(_, _, State) -> keep(State).

waiting_pong(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) -> process_data(Data, waiting_pong, State);
waiting_pong(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) -> process_data(Data, waiting_pong, State);
waiting_pong(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) -> connect_failed(waiting_pong, closed, State);
waiting_pong(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) -> connect_failed(waiting_pong, closed, State);
waiting_pong(state_timeout, connect_timeout, State) -> connect_failed(waiting_pong, timeout, State);
waiting_pong({call, From}, status, _State) -> reply(From, connecting);
waiting_pong({call, From}, disconnect, State) ->
    reply_connect(State, {error, disconnected}),
    reply_requests(State, {error, disconnected}),
    close(State),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
waiting_pong({call, From}, _Request, _State) -> reply(From, {error, connecting});
waiting_pong(_, _, State) -> keep(State).

connected({call, From}, status, _State) -> reply(From, connected);
connected({call, From}, info, State) -> reply(From, maps:get(server_info, State));
connected({call, From}, connect, _State) -> reply(From, {error, already_connected});
connected({call, From}, {connect, _Timeout}, _State) -> reply(From, {error, already_connected});
connected({call, From}, {publish, Subject, Payload0, Options}, State) ->
    case enats_subject:validate_publish(Subject) of
        ok ->
            Payload = iolist_to_binary(Payload0),
            case validate_publish_options(Payload, Options, State) of
                ok ->
                    case send_frame(publish_frame(Subject, Payload, Options), State) of
                        ok -> reply(From, ok);
                        {error, Reason} -> lost_with_reply(From, Reason, State)
                    end;
                {error, Reason} -> reply(From, {error, Reason})
            end;
        {error, Reason} -> reply(From, {error, {invalid_subject, Reason}})
    end;
connected({call, From}, {request, Subject, Payload0, Options}, State) ->
    case enats_subject:validate_publish(Subject) of
        ok ->
            Payload = iolist_to_binary(Payload0),
            case validate_publish_options(Payload, Options, State) of
                ok ->
                    Sid = integer_to_binary(maps:get(next_sid, State)),
                    Inbox = request_inbox(),
                    case send_frame({sub, Inbox, Sid, undefined}, State) of
                        ok ->
                            case send_frame(publish_frame(Subject, Payload, Options#{reply_to => Inbox}), State) of
                                ok ->
                                    Timer = request_timer(maps:get(timeout, Options, ?TIMEOUT), Sid),
                                    Requests = maps:put(Sid, #{from => From, timer => Timer}, maps:get(requests, State)),
                                    {keep_state, State#{requests => Requests, next_sid => maps:get(next_sid, State) + 1}, []};
                                {error, Reason} -> lost_with_reply(From, Reason, State)
                            end;
                        {error, Reason} -> lost_with_reply(From, Reason, State)
                    end;
                {error, Reason} -> reply(From, {error, Reason})
            end;
        {error, Reason} -> reply(From, {error, {invalid_subject, Reason}})
    end;
connected({call, From}, {subscribe, Subject, Options}, State) ->
    case enats_subject:validate(Subject) of
        ok ->
            Sid = integer_to_binary(maps:get(next_sid, State)),
            Queue = maps:get(queue_group, Options, undefined),
            case send_frame({sub, Subject, Sid, Queue}, State) of
                ok ->
                    Ref = make_ref(),
                    Owner = maps:get(owner, Options, maps:get(owner, maps:get(options, State))),
                    Subscription = #{ref => Ref, sid => Sid, subject => Subject, queue_group => Queue, owner => Owner},
                    Subs = maps:put(Ref, Subscription, maps:get(subscriptions, State)),
                    {keep_state, State#{subscriptions => Subs, next_sid => maps:get(next_sid, State) + 1}, reply_action(From, {ok, Ref})};
                {error, Reason} -> lost_with_reply(From, Reason, State)
            end;
        {error, Reason} -> reply(From, {error, {invalid_subject, Reason}})
    end;
connected({call, From}, {unsubscribe, Ref}, State) ->
    case maps:take(Ref, maps:get(subscriptions, State)) of
        {#{sid := Sid}, Subs} ->
            case send_frame({unsub, Sid}, State) of
                ok -> {keep_state, State#{subscriptions => Subs}, reply_action(From, ok)};
                {error, Reason} -> lost_with_reply(From, Reason, State)
            end;
        error -> reply(From, {error, not_found})
    end;
connected({call, From}, {flush, Timeout}, State) ->
    case send_frame(ping, State) of
        ok ->
            Ref = make_ref(),
            Flush = #{kind => flush, ref => Ref, from => From, timer => flush_timer(Timeout, Ref)},
            Flushes = queue:in(Flush, maps:get(flushes, State)),
            {keep_state, State#{flushes => Flushes}, []};
        {error, Reason} -> lost_with_reply(From, Reason, State)
    end;
connected(info, {flush_timeout, Ref}, State) ->
    {keep_state, expire_flush(Ref, State), []};
connected(info, heartbeat, State) ->
    heartbeat(State);
connected({call, From}, disconnect, State) ->
    reply_flushes(State, {error, disconnected}),
    reply_requests(State, {error, disconnected}),
    close(State), notify(State, disconnected, requested),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
connected(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) -> process_data(Data, connected, State);
connected(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) -> process_data(Data, connected, State);
connected(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) -> lost(connected, closed, State);
connected(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) -> lost(connected, closed, State);
connected(info, {request_timeout, Sid}, State) -> request_timeout(Sid, State);
connected(_, _, State) -> keep(State).

request_timeout(Sid, State) ->
    case maps:take(Sid, maps:get(requests, State)) of
        {#{from := From}, Requests} ->
            _ = send_frame({unsub, Sid}, State),
            gen_statem:reply(From, {error, timeout}),
            {keep_state, State#{requests => Requests}, []};
        error ->
            keep(State)
    end.

request_timer(infinity, _Sid) ->
    undefined;
request_timer(Timeout, Sid) ->
    erlang:send_after(Timeout, self(), {request_timeout, Sid}).

flush_timer(infinity, _Ref) ->
    undefined;
flush_timer(Timeout, Ref) ->
    erlang:send_after(Timeout, self(), {flush_timeout, Ref}).

complete_pong(#{flushes := Flushes0} = State) ->
    case queue:out(Flushes0) of
        {{value, #{kind := flush, from := From, timer := Timer}}, Flushes} ->
            cancel_flush_timer(Timer),
            maybe_reply_flush(From, ok),
            State#{flushes => Flushes, pings_out => 0};
        {{value, #{kind := heartbeat}}, Flushes} ->
            State#{flushes => Flushes, pings_out => 0};
        {empty, Flushes} ->
            State#{flushes => Flushes, pings_out => 0}
    end.

heartbeat(#{pings_out := PingsOut, options := Options} = State) ->
    MaxPingsOut = maps:get(max_pings_out, Options),
    case PingsOut >= MaxPingsOut of
        true ->
            lost(connected, stale_connection, State);
        false ->
            case send_frame(ping, State) of
                ok ->
                    Flushes = queue:in(#{kind => heartbeat}, maps:get(flushes, State)),
                    {keep_state, schedule_heartbeat(State#{flushes => Flushes, pings_out => PingsOut + 1}), []};
                {error, Reason} ->
                    lost(connected, Reason, State)
            end
    end.

start_heartbeat(#{options := #{ping_interval := Interval}} = State) when Interval > 0 ->
    schedule_heartbeat(State);
start_heartbeat(State) ->
    State.

schedule_heartbeat(#{options := #{ping_interval := Interval}, heartbeat_timer := Timer} = State) ->
    cancel_timer(Timer),
    State#{heartbeat_timer => erlang:send_after(Interval, self(), heartbeat)}.

cancel_timer(undefined) ->
    ok;
cancel_timer(Timer) ->
    _ = erlang:cancel_timer(Timer),
    ok.

expire_flush(Ref, #{flushes := Flushes0} = State) ->
    {Flushes, From} = expire_flush(Ref, queue:to_list(Flushes0), []),
    maybe_reply_flush(From, {error, timeout}),
    State#{flushes => queue:from_list(Flushes)}.

expire_flush(_Ref, [], Acc) ->
    {lists:reverse(Acc), undefined};
expire_flush(Ref, [#{ref := Ref, from := From} = Flush | Rest], Acc) when From =/= undefined ->
    {lists:reverse(Acc, [Flush#{from => undefined, timer => undefined} | Rest]), From};
expire_flush(Ref, [Flush | Rest], Acc) ->
    expire_flush(Ref, Rest, [Flush | Acc]).

cancel_flush_timer(undefined) ->
    ok;
cancel_flush_timer(Timer) ->
    _ = erlang:cancel_timer(Timer),
    ok.

maybe_reply_flush(undefined, _Reply) ->
    ok;
maybe_reply_flush(From, Reply) ->
    gen_statem:reply(From, Reply).

request_inbox() ->
    <<"_INBOX.enats.", (binary:encode_hex(crypto:strong_rand_bytes(16)))/binary>>.

reconnecting(state_timeout, reconnect, State) ->
    case open_socket(State) of
        {ok, Socket, State1} -> {next_state, waiting_info, State1#{socket => Socket, parse_state => enats_frame:initial_state()}, [{state_timeout, ?TIMEOUT, connect_timeout}]};
        {error, _Reason, State1} -> {keep_state, State1, [{state_timeout, reconnect_delay(State1), reconnect}]}
    end;
reconnecting({call, From}, status, _State) -> reply(From, reconnecting);
reconnecting({call, From}, disconnect, State) ->
    reply_requests(State, {error, disconnected}),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
reconnecting({call, From}, _Request, _State) -> reply(From, {error, disconnected});
reconnecting(_, _, State) -> keep(State).

process_data(Data, StateName, State0) ->
    try enats_frame:parse(Data, maps:get(parse_state, State0)) of
        {Frames, ParseState} -> process_frames(Frames, StateName, State0#{parse_state => ParseState})
    catch Class:Reason -> lost(StateName, {protocol_error, Class, Reason}, State0)
    end.

process_frames([], _StateName, State) -> keep(State);
process_frames([Frame | Rest], StateName, State0) ->
    case process_frame(Frame, StateName, State0) of
        {next_state, NewName, State1, Actions} when Rest =:= [] -> {next_state, NewName, State1, Actions};
        {next_state, NewName, State1, Actions} ->
            append_transition(process_frames(Rest, NewName, State1), NewName, Actions);
        {keep_state, State1, Actions} when Rest =:= [] -> {keep_state, State1, Actions};
        {keep_state, State1, Actions} ->
            append_actions(process_frames(Rest, StateName, State1), Actions)
    end.

process_frame({info, RawInfo}, waiting_info, State) ->
    Info = normalize_info(RawInfo),
    Base0 = #{verbose => false, pedantic => false, tls_required => false, protocol => 1,
        headers => true, lang => <<"erlang">>, version => <<"0.1.10">>},
    Base = case maps:get(headers, Info, false) of
        true -> Base0#{no_responders => true};
        false -> Base0
    end,
    case maybe_upgrade_tls(Info, State) of
        {error, Reason} -> connect_failed(waiting_info, {tls_upgrade_failed, Reason}, State);
        {ok, TransportState} -> process_connect_info(Info, Base, update_servers(Info, TransportState))
    end;
process_frame({info, RawInfo}, connected, State) ->
    Info = normalize_info(RawInfo),
    State1 = update_servers(Info, State#{server_info => maps:merge(maps:get(server_info, State), Info)}),
    notify(State1, info_updated, Info),
    {keep_state, State1, []};
process_frame(pong, waiting_pong, State) ->
    reply_connect(State, ok),
    restore_subscriptions(State),
    notify(State, connected, maps:get(server_info, State)),
    {next_state, connected, start_heartbeat(clear_pending_connect(State)), []};
process_frame({error, Reason}, waiting_info, State) -> connect_failed(waiting_info, {server_error, Reason}, State);
process_frame({error, Reason}, waiting_pong, State) -> connect_failed(waiting_pong, {server_error, Reason}, State);
process_frame({error, Reason}, connected, State) -> lost(connected, {server_error, Reason}, State);
process_frame(ping, _StateName, State) -> ok = send_frame(pong, State), {keep_state, State, []};
process_frame(pong, connected, State) ->
    {keep_state, complete_pong(State), []};
process_frame(#{type := msg} = Message, connected, State) -> {keep_state, deliver(Message, State), []};
process_frame(#{type := hmsg} = Message, connected, State) -> {keep_state, deliver(Message, State), []};
process_frame(ok, _StateName, State) -> {keep_state, State, []};
process_frame(_Frame, _StateName, State) -> {keep_state, State, []}.

append_actions({keep_state, State, Later}, Actions) -> {keep_state, State, Actions ++ Later};
append_actions({next_state, Name, State, Later}, Actions) -> {next_state, Name, State, Actions ++ Later}.

append_transition({keep_state, State, Later}, Name, Actions) -> {next_state, Name, State, Actions ++ Later};
append_transition({next_state, Name, State, Later}, _OldName, Actions) -> {next_state, Name, State, Actions ++ Later}.

process_connect_info(Info, Base, State) ->
    case enats_auth:connect_params(maps:get(auth, maps:get(options, State)), Info, Base) of
        {ok, Params} ->
            ok = send_frame({connect, Params}, State),
            ok = send_frame(ping, State),
            {next_state, waiting_pong, State#{server_info => Info}, []};
        {error, Reason} -> connect_failed(waiting_info, Reason, State)
    end.

connect_failed(StateName, _Reason, #{connect_from := From, connect_attempts := Attempts} = State)
    when From =/= undefined, Attempts > 0 ->
    close(State),
    Timeout = maps:get(pending_connect_timeout, State),
    State0 = reset_transport(State),
    case open_socket(State0, length(maps:get(servers, State0)), maps:get(options, State0), Timeout) of
        {ok, Socket, State1} ->
            {next_state, waiting_info, State1#{socket => Socket,
                connect_attempts => Attempts - 1,
                parse_state => enats_frame:initial_state()},
                [{state_timeout, Timeout, connect_timeout}]};
        {error, NextReason, State1} ->
            lost(StateName, NextReason, State1)
    end;
connect_failed(StateName, Reason, State) ->
    lost(StateName, Reason, State).

open_socket(State) ->
    Options = maps:get(options, State),
    Servers = maps:get(servers, State),
    open_socket(State, length(Servers), Options, maps:get(connect_timeout, Options)).

open_socket(State, 0, _Options, _Timeout) ->
    {error, no_servers_available, State};
open_socket(State, Attempts, Options, Timeout) ->
    Servers = maps:get(servers, State),
    Index = maps:get(server_index, State),
    {Host, Port} = lists:nth(Index, Servers),
    NextIndex = next_server_index(Index, length(Servers)),
    State1 = State#{server_index => NextIndex, current_server => {Host, Port}},
    case open_transport(Host, Port, Options, Timeout) of
        {ok, Socket} -> {ok, Socket, State1};
        {error, _Reason} when Attempts > 1 -> open_socket(State1, Attempts - 1, Options, Timeout);
        {error, Reason} -> {error, Reason, State1}
    end.

tcp_result({ok, Socket}) -> {ok, {tcp, Socket}};
tcp_result(Error) -> Error.

open_transport(Host, Port, #{tls := true, tls_handshake := first, ssl_opts := SslOpts}, Timeout) ->
    ssl_result(ssl:connect(Host, Port, transport_options(Host, [
        binary, {active, true}, {server_name_indication, tls_server_name(Host)}
    ] ++ SslOpts), Timeout));
open_transport(Host, Port, _Options, Timeout) ->
    tcp_result(gen_tcp:connect(Host, Port, transport_options(Host, [
        binary, {active, true}, {nodelay, true}
    ]), Timeout)).

ssl_result({ok, Socket}) -> {ok, {ssl, Socket}};
ssl_result(Error) -> Error.
maybe_upgrade_tls(Info, #{socket := {tcp, Socket}, options := Options, current_server := {Host, _Port}} = State) ->
    NeedTLS = maps:get(tls, Options) andalso
        (maps:get(tls_required, Info, false) orelse maps:get(tls_available, Info, false)),
    case {maps:get(tls, Options), NeedTLS} of
        {true, false} -> {error, tls_not_available};
        {false, false} -> {ok, State};
        {true, true} ->
            _ = inet:setopts(Socket, [{active, false}]),
            SslOpts = transport_options(Host, [{active, true},
                {server_name_indication, tls_server_name(Host)} | maps:get(ssl_opts, Options)]),
            case ssl:connect(Socket, SslOpts, maps:get(connect_timeout, Options)) of
                {ok, SslSocket} -> {ok, State#{socket => {ssl, SslSocket}}};
                Error -> Error
            end
    end;
maybe_upgrade_tls(_Info, #{socket := {ssl, _Socket}} = State) ->
    {ok, State};
maybe_upgrade_tls(Info, #{options := Options} = State) ->
    case maps:get(tls, Options) andalso maps:get(tls_required, Info, false) of
        true -> {error, tls_already_established};
        false -> {ok, State}
    end.

send_frame({connect, Params}, #{socket := Socket}) -> send_socket(Socket, enats_frame:serialize_connect(Params));
send_frame(Frame, #{socket := Socket}) -> send_socket(Socket, enats_frame:serialize(Frame)).
send_socket({tcp, Socket}, Data) -> gen_tcp:send(Socket, Data);
send_socket({ssl, Socket}, Data) -> ssl:send(Socket, Data).

publish_frame(Subject, Payload, Options) ->
    case maps:get(headers, Options, []) of
        [] -> case maps:get(reply_to, Options, undefined) of
            undefined -> {pub, Subject, Payload};
            ReplyTo -> {pub, Subject, ReplyTo, Payload}
        end;
        Headers -> {hpub, Subject, maps:get(reply_to, Options, undefined), Headers, Payload}
    end.

validate_publish_options(Payload, Options, State) ->
    Info = maps:get(server_info, State),
    MaxPayload = maps:get(max_payload, Info, infinity),
    Headers = maps:get(headers, Options, []),
    case Headers =/= [] andalso maps:get(headers, Info, true) =:= false of
        true -> {error, headers_not_supported};
        false ->
            case enats_frame:validate_headers(Headers) of
                {error, _} = Error -> Error;
                ok ->
                    MessageSize = byte_size(Payload) + enats_frame:headers_size(Headers),
                    case is_integer(MaxPayload) andalso MessageSize > MaxPayload of
                        true -> {error, {payload_too_large, MaxPayload}};
                        false -> ok
                    end
            end
    end.

restore_subscriptions(State) ->
    maps:foreach(fun(_Ref, #{subject := Subject, sid := Sid, queue_group := Queue}) ->
        _ = send_frame({sub, Subject, Sid, Queue}, State)
    end, maps:get(subscriptions, State)).

deliver(Message, State) ->
    Sid = maps:get(sid, Message),
    case maps:take(Sid, maps:get(requests, State)) of
        {#{from := From, timer := Timer}, Requests} ->
            cancel_request_timer(Timer),
            _ = send_frame({unsub, Sid}, State),
            gen_statem:reply(From, request_result(Message)),
            State#{requests => Requests};
        error -> deliver_subscription(Message, State)
    end.

request_result(#{type := hmsg, payload := <<>>, headers := Headers} = Message) ->
    case lists:keyfind(<<"Status">>, 1, Headers) of
        {_, <<"503">> = Status} -> {error, {no_responders, Status}};
        _ -> {ok, Message}
    end;
request_result(Message) ->
    {ok, Message}.

deliver_subscription(Message, State) ->
    Sid = maps:get(sid, Message),
    case [Sub || Sub <- maps:values(maps:get(subscriptions, State)), maps:get(sid, Sub) =:= Sid] of
        [#{owner := Owner} | _] -> Owner ! {enats_client, self(), {message, Message}};
        [] -> ok
    end,
    State.

lost(_StateName, Reason, State) ->
    close(State),
    reply_connect(State, {error, Reason}),
    reply_flushes(State, {error, {disconnected, Reason}}),
    reply_requests(State, {error, {disconnected, Reason}}),
    notify(State, disconnected, Reason),
    case maps:get(reconnect, maps:get(options, State)) of
        false -> {next_state, disconnected, clear_socket(State), []};
        true -> {next_state, reconnecting, clear_socket(State), [{state_timeout, reconnect_delay(State), reconnect}]}
    end.

lost_with_reply(From, Reason, State) ->
    gen_statem:reply(From, {error, Reason}), lost(connected, Reason, State).

reply_connect(#{connect_from := undefined}, _Reply) -> ok;
reply_connect(#{connect_from := From}, Reply) -> gen_statem:reply(From, Reply).
reply_flushes(#{flushes := Flushes}, Reply) ->
    lists:foreach(
        fun(Flush) ->
            cancel_flush_timer(maps:get(timer, Flush, undefined)),
            maybe_reply_flush(maps:get(from, Flush, undefined), Reply)
        end,
        queue:to_list(Flushes)
    ).
reply_requests(#{requests := Requests}, Reply) ->
    maps:foreach(
        fun(_Sid, #{from := From, timer := Timer}) ->
            cancel_request_timer(Timer),
            gen_statem:reply(From, Reply)
        end,
        Requests
    ).

cancel_request_timer(undefined) ->
    ok;
cancel_request_timer(Timer) ->
    _ = erlang:cancel_timer(Timer),
    ok.
notify(State, Event, Data) ->
    Options = maps:get(options, State),
    case maps:get(notify, Options, true) of
        true -> maps:get(owner, Options) ! {enats_client, self(), Event, Data};
        false -> ok
    end.
reply(From, Value) -> {keep_state_and_data, {reply, From, Value}}.
reply_action(From, Value) -> [{reply, From, Value}].
keep(State) -> {keep_state, State, []}.

clear_socket(State) ->
    clear_pending_connect(reset_transport(State)).

clear_pending_connect(State) ->
    State#{connect_from => undefined, connect_attempts => 0,
        pending_connect_timeout => undefined}.

reset_transport(State) ->
    cancel_timer(maps:get(heartbeat_timer, State, undefined)),
    State#{socket => undefined, flushes => queue:new(), requests => #{},
        heartbeat_timer => undefined, pings_out => 0,
        parse_state => enats_frame:initial_state()}.
close(#{socket := undefined}) -> ok;
close(#{socket := {tcp, Socket}}) -> gen_tcp:close(Socket);
close(#{socket := {ssl, Socket}}) -> ssl:close(Socket).
validate_options(Options) when is_map(Options) ->
    case maps:get(tls_handshake, Options, starttls) of
        starttls -> validate_heartbeat_options(Options);
        first -> validate_heartbeat_options(Options);
        Value -> {error, {invalid_option, tls_handshake, Value}}
    end;
validate_options(Options) ->
    {error, {invalid_options, Options}}.

validate_heartbeat_options(Options) ->
    PingInterval = maps:get(ping_interval, Options, 120000),
    MaxPingsOut = maps:get(max_pings_out, Options, 2),
    case is_integer(PingInterval) andalso PingInterval >= 0 of
        false -> {error, {invalid_option, ping_interval, PingInterval}};
        true ->
            case is_integer(MaxPingsOut) andalso MaxPingsOut > 0 of
                true -> ok;
                false -> {error, {invalid_option, max_pings_out, MaxPingsOut}}
            end
    end.

normalize_options(Options) -> maps:merge(#{host => "127.0.0.1", port => 4222, tls => false, ssl_opts => [],
    tls_handshake => starttls, connect_timeout => 5000, reconnect => false, reconnect_delay => 1000,
    auth => none, owner => self(), notify => true, ping_interval => 120000, max_pings_out => 2},
    normalize_servers(Options)).
normalize_info(Info) ->
    maps:fold(fun(Key, Value, Acc) -> maps:put(info_key(Key), Value, Acc) end, #{}, Info).

info_key(<<"server_id">>) -> server_id;
info_key(<<"server_name">>) -> server_name;
info_key(<<"version">>) -> version;
info_key(<<"go">>) -> go;
info_key(<<"host">>) -> host;
info_key(<<"port">>) -> port;
info_key(<<"headers">>) -> headers;
info_key(<<"max_payload">>) -> max_payload;
info_key(<<"proto">>) -> proto;
info_key(<<"auth_required">>) -> auth_required;
info_key(<<"tls_required">>) -> tls_required;
info_key(<<"tls_verify">>) -> tls_verify;
info_key(<<"tls_available">>) -> tls_available;
info_key(<<"connect_urls">>) -> connect_urls;
info_key(<<"ws_connect_urls">>) -> ws_connect_urls;
info_key(<<"jetstream">>) -> jetstream;
info_key(<<"nonce">>) -> nonce;
info_key(<<"client_id">>) -> client_id;
info_key(<<"client_ip">>) -> client_ip;
info_key(Key) -> Key.
reconnect_delay(State) -> maps:get(reconnect_delay, maps:get(options, State)).
terminate(_Reason, _StateName, State) -> close(State), ok.

normalize_servers(Options) ->
    case maps:get(servers, Options, undefined) of
        undefined -> Options#{servers => [{maps:get(host, Options, "127.0.0.1"), maps:get(port, Options, 4222)}]};
        Servers when is_list(Servers), Servers =/= [] -> Options;
        _ -> error({invalid_servers, maps:get(servers, Options)})
    end.

next_server_index(Index, Count) when Index >= Count -> 1;
next_server_index(Index, _Count) -> Index + 1.

update_servers(#{connect_urls := Urls}, State) when is_list(Urls) ->
    Existing = maps:get(servers, State),
    Discovered = [Server || Url <- Urls, {ok, Server} <- [parse_server_url(Url)]],
    State#{servers => unique_servers(Existing ++ Discovered)};
update_servers(_Info, State) -> State.

parse_server_url(Url0) when is_binary(Url0) -> parse_server_url(binary_to_list(Url0));
parse_server_url("nats://" ++ Address) -> parse_host_port(Address);
parse_server_url(Address) when is_list(Address) -> parse_host_port(Address).

parse_host_port([$[ | Rest]) ->
    case string:split(Rest, "]", leading) of
        [Host, [$: | Port]] -> parse_host_port_value(Host, Port);
        _ -> {error, invalid_server_url}
    end;
parse_host_port(Address) ->
    case string:split(Address, ":", trailing) of
        [Host, Port] -> parse_host_port_value(Host, Port);
        _ -> {error, invalid_server_url}
    end.

parse_host_port_value(Host, Port0) ->
    try
        Port = list_to_integer(Port0),
        true = Port > 0 andalso Port =< 65535,
        case inet:parse_address(Host) of
            {ok, Address} -> {ok, {Address, Port}};
            {error, _} -> {ok, {Host, Port}}
        end
    catch
        _:_ -> {error, invalid_server_url}
    end.

transport_options(Host, Options) when is_tuple(Host), tuple_size(Host) =:= 8 ->
    [inet6 | Options];
transport_options(_Host, Options) ->
    Options.

tls_server_name(Host) when is_tuple(Host) ->
    inet:ntoa(Host);
tls_server_name(Host) ->
    Host.

unique_servers(Servers) -> lists:usort(Servers).
