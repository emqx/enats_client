-module(enats_connection).
-behaviour(gen_statem).

-export([start_link/1, connect/1, disconnect/1, stop/1, status/1, info/1,
    publish/4, subscribe/3, unsubscribe/2, flush/2]).
-export([init/1, callback_mode/0, terminate/3, disconnected/3, waiting_info/3,
    waiting_pong/3, connected/3, reconnecting/3]).

-define(TIMEOUT, 5000).

start_link(Options) -> gen_statem:start_link(?MODULE, Options, []).
connect(Pid) -> gen_statem:call(Pid, connect, ?TIMEOUT).
disconnect(Pid) -> gen_statem:call(Pid, disconnect, ?TIMEOUT).
stop(Pid) -> gen_statem:stop(Pid).
status(Pid) -> gen_statem:call(Pid, status, ?TIMEOUT).
info(Pid) -> gen_statem:call(Pid, info, ?TIMEOUT).
publish(Pid, Subject, Payload, Options) -> gen_statem:call(Pid, {publish, Subject, Payload, Options}, ?TIMEOUT).
subscribe(Pid, Subject, Options) -> gen_statem:call(Pid, {subscribe, Subject, Options}, ?TIMEOUT).
unsubscribe(Pid, Ref) -> gen_statem:call(Pid, {unsubscribe, Ref}, ?TIMEOUT).
flush(Pid, Timeout) -> gen_statem:call(Pid, {flush, Timeout}, Timeout + 1000).

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
        flush_from => undefined,
        subscriptions => #{},
        next_sid => 1,
        servers => maps:get(servers, Options),
        server_index => 1
    }}.

disconnected({call, From}, connect, State) ->
    case open_socket(State) of
        {ok, Socket, State1} ->
            {next_state, waiting_info, State1#{socket => Socket, connect_from => From,
                parse_state => enats_frame:initial_state()}, [{state_timeout, ?TIMEOUT, connect_timeout}]};
        {error, Reason, State1} -> {keep_state, State1, reply_action(From, {error, Reason})}
    end;
disconnected({call, From}, status, _State) -> reply(From, disconnected);
disconnected({call, From}, info, State) -> reply(From, maps:get(server_info, State));
disconnected({call, From}, disconnect, _State) -> reply(From, ok);
disconnected({call, From}, _Request, _State) -> reply(From, {error, disconnected});
disconnected(_, _, State) -> keep(State).

waiting_info(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) -> process_data(Data, waiting_info, State);
waiting_info(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) -> process_data(Data, waiting_info, State);
waiting_info(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) -> lost(waiting_info, closed, State);
waiting_info(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) -> lost(waiting_info, closed, State);
waiting_info(state_timeout, connect_timeout, State) -> lost(waiting_info, timeout, State);
waiting_info({call, From}, status, _State) -> reply(From, connecting);
waiting_info({call, From}, disconnect, State) ->
    reply_connect(State, {error, disconnected}),
    close(State),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
waiting_info({call, From}, _Request, _State) -> reply(From, {error, connecting});
waiting_info(_, _, State) -> keep(State).

waiting_pong(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) -> process_data(Data, waiting_pong, State);
waiting_pong(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) -> process_data(Data, waiting_pong, State);
waiting_pong(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) -> lost(waiting_pong, closed, State);
waiting_pong(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) -> lost(waiting_pong, closed, State);
waiting_pong(state_timeout, connect_timeout, State) -> lost(waiting_pong, timeout, State);
waiting_pong({call, From}, status, _State) -> reply(From, connecting);
waiting_pong({call, From}, disconnect, State) ->
    reply_connect(State, {error, disconnected}),
    close(State),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
waiting_pong({call, From}, _Request, _State) -> reply(From, {error, connecting});
waiting_pong(_, _, State) -> keep(State).

connected({call, From}, status, _State) -> reply(From, connected);
connected({call, From}, info, State) -> reply(From, maps:get(server_info, State));
connected({call, From}, {publish, Subject, Payload0, Options}, State) ->
    case enats_subject:validate_publish(Subject) of
        ok ->
            Payload = iolist_to_binary(Payload0),
            case send_frame(publish_frame(Subject, Payload, Options), State) of
                ok -> reply(From, ok);
                {error, Reason} -> lost_with_reply(From, Reason, State)
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
        ok -> {keep_state, State#{flush_from => From}, [{state_timeout, Timeout, flush_timeout}]};
        {error, Reason} -> lost_with_reply(From, Reason, State)
    end;
connected(state_timeout, flush_timeout, #{flush_from := undefined} = State) -> keep(State);
connected(state_timeout, flush_timeout, #{flush_from := From} = State) ->
    {keep_state, State#{flush_from => undefined}, reply_action(From, {error, timeout})};
connected({call, From}, disconnect, State) ->
    close(State), notify(State, disconnected, requested),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
connected(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) -> process_data(Data, connected, State);
connected(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) -> process_data(Data, connected, State);
connected(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) -> lost(connected, closed, State);
connected(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) -> lost(connected, closed, State);
connected(_, _, State) -> keep(State).

reconnecting(state_timeout, reconnect, State) ->
    case open_socket(State) of
        {ok, Socket, State1} -> {next_state, waiting_info, State1#{socket => Socket, parse_state => enats_frame:initial_state()}, [{state_timeout, ?TIMEOUT, connect_timeout}]};
        {error, _Reason, State1} -> {keep_state, State1, [{state_timeout, reconnect_delay(State1), reconnect}]}
    end;
reconnecting({call, From}, status, _State) -> reply(From, reconnecting);
reconnecting({call, From}, disconnect, _State) -> {next_state, disconnected, _State, reply_action(From, ok)};
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
        {next_state, NewName, State1, _Actions} -> process_frames(Rest, NewName, State1);
        {keep_state, State1, Actions} when Rest =:= [] -> {keep_state, State1, Actions};
        {keep_state, State1, _Actions} -> process_frames(Rest, StateName, State1)
    end.

process_frame({info, RawInfo}, waiting_info, State) ->
    Info = normalize_info(RawInfo),
    Base = #{verbose => false, pedantic => false, tls_required => false, protocol => 1,
        headers => true, lang => <<"erlang">>, version => <<"0.1.0">>},
    case maybe_upgrade_tls(Info, State) of
        {error, Reason} -> lost(waiting_info, {tls_upgrade_failed, Reason}, State);
        {ok, TransportState} -> process_connect_info(Info, Base, update_servers(Info, TransportState))
    end;
process_frame(pong, waiting_pong, State) ->
    reply_connect(State, ok),
    restore_subscriptions(State),
    notify(State, connected, maps:get(server_info, State)),
    {next_state, connected, State#{connect_from => undefined}, []};
process_frame({error, Reason}, waiting_info, State) -> lost(waiting_info, {server_error, Reason}, State);
process_frame({error, Reason}, waiting_pong, State) -> lost(waiting_pong, {server_error, Reason}, State);
process_frame(ping, _StateName, State) -> ok = send_frame(pong, State), {keep_state, State, []};
process_frame(pong, connected, #{flush_from := undefined} = State) -> {keep_state, State, []};
process_frame(pong, connected, #{flush_from := From} = State) ->
    {keep_state, State#{flush_from => undefined}, reply_action(From, ok)};
process_frame(#{type := msg} = Message, connected, State) -> deliver(Message, State), {keep_state, State, []};
process_frame(#{type := hmsg} = Message, connected, State) -> deliver(Message, State), {keep_state, State, []};
process_frame(ok, _StateName, State) -> {keep_state, State, []};
process_frame(_Frame, _StateName, State) -> {keep_state, State, []}.

process_connect_info(Info, Base, State) ->
    case enats_auth:connect_params(maps:get(auth, maps:get(options, State)), Info, Base) of
        {ok, Params} ->
            ok = send_frame({connect, Params}, State),
            ok = send_frame(ping, State),
            {next_state, waiting_pong, State#{server_info => Info}, []};
        {error, Reason} -> lost(waiting_info, Reason, State)
    end.

open_socket(State) ->
    Options = maps:get(options, State),
    Servers = maps:get(servers, State),
    open_socket(State, length(Servers), Options).

open_socket(State, 0, _Options) ->
    {error, no_servers_available, State};
open_socket(State, Attempts, Options) ->
    Servers = maps:get(servers, State),
    Index = maps:get(server_index, State),
    {Host, Port} = lists:nth(Index, Servers),
    NextIndex = next_server_index(Index, length(Servers)),
    State1 = State#{server_index => NextIndex},
    Timeout = maps:get(connect_timeout, Options),
    case tcp_result(gen_tcp:connect(Host, Port, [binary, {active, true}, {nodelay, true}], Timeout)) of
        {ok, Socket} -> {ok, Socket, State1};
        {error, _Reason} when Attempts > 1 -> open_socket(State1, Attempts - 1, Options);
        {error, Reason} -> {error, Reason, State1}
    end.

tcp_result({ok, Socket}) -> {ok, {tcp, Socket}};
tcp_result(Error) -> Error.
maybe_upgrade_tls(Info, #{socket := {tcp, Socket}, options := Options} = State) ->
    case maps:get(tls, Options) andalso maps:get(tls_required, Info, false) of
        false -> {ok, State};
        true ->
            _ = inet:setopts(Socket, [{active, false}]),
            SslOpts = [{active, true}, {server_name_indication, maps:get(host, Options)} |
                maps:get(ssl_opts, Options)],
            case ssl:connect(Socket, SslOpts, maps:get(connect_timeout, Options)) of
                {ok, SslSocket} -> {ok, State#{socket => {ssl, SslSocket}}};
                Error -> Error
            end
    end;
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

restore_subscriptions(State) ->
    maps:foreach(fun(_Ref, #{subject := Subject, sid := Sid, queue_group := Queue}) ->
        _ = send_frame({sub, Subject, Sid, Queue}, State)
    end, maps:get(subscriptions, State)).

deliver(Message, State) ->
    Sid = maps:get(sid, Message),
    case [Sub || Sub <- maps:values(maps:get(subscriptions, State)), maps:get(sid, Sub) =:= Sid] of
        [#{owner := Owner} | _] -> Owner ! {enats_client, self(), {message, Message}};
        [] -> ok
    end.

lost(_StateName, Reason, State) ->
    close(State),
    reply_connect(State, {error, Reason}),
    notify(State, disconnected, Reason),
    case maps:get(reconnect, maps:get(options, State)) of
        false -> {next_state, disconnected, clear_socket(State), []};
        true -> {next_state, reconnecting, clear_socket(State), [{state_timeout, reconnect_delay(State), reconnect}]}
    end.

lost_with_reply(From, Reason, State) ->
    gen_statem:reply(From, {error, Reason}), lost(connected, Reason, State).

reply_connect(#{connect_from := undefined}, _Reply) -> ok;
reply_connect(#{connect_from := From}, Reply) -> gen_statem:reply(From, Reply).
notify(State, Event, Data) -> maps:get(owner, maps:get(options, State)) ! {enats_client, self(), Event, Data}.
reply(From, Value) -> {keep_state_and_data, {reply, From, Value}}.
reply_action(From, Value) -> [{reply, From, Value}].
keep(State) -> {keep_state, State, []}.

clear_socket(State) -> State#{socket => undefined, connect_from => undefined, flush_from => undefined}.
close(#{socket := undefined}) -> ok;
close(#{socket := {tcp, Socket}}) -> gen_tcp:close(Socket);
close(#{socket := {ssl, Socket}}) -> ssl:close(Socket).
normalize_options(Options) -> maps:merge(#{host => "127.0.0.1", port => 4222, tls => false, ssl_opts => [],
    connect_timeout => 5000, reconnect => false, reconnect_delay => 1000, auth => none, owner => self()},
    normalize_servers(Options)).
normalize_info(Info) -> maps:fold(fun(Key, Value, Acc) -> maps:put(binary_to_atom(Key, utf8), Value, Acc) end, #{}, Info).
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

parse_host_port(Address) ->
    case string:split(Address, ":", trailing) of
        [Host, Port] ->
            try {ok, {Host, list_to_integer(Port)}} catch _:_ -> {error, invalid_server_url} end;
        _ -> {error, invalid_server_url}
    end.

unique_servers(Servers) -> lists:usort(Servers).
