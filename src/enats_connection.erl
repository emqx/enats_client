-module(enats_connection).
-moduledoc "Internal gen_statem implementation for a single NATS connection.".
-behaviour(gen_statem).

-export([
    start_link/1,
    connect/1, connect/2,
    disconnect/1,
    stop/1,
    status/1,
    info/1,
    stats/1,
    publish/4,
    request/4,
    subscribe/3,
    unsubscribe/2,
    flush/2,
    drain/2,
    enable_diagnostics/2,
    disable_diagnostics/1,
    diagnostics/1,
    reset_diagnostics/1
]).
-export([
    init/1,
    callback_mode/0,
    terminate/3,
    disconnected/3,
    waiting_info/3,
    waiting_pong/3,
    connected/3,
    draining/3,
    reconnecting/3
]).

-define(TIMEOUT, 5000).

-type value() ::
    atom()
    | binary()
    | boolean()
    | integer()
    | float()
    | pid()
    | reference()
    | [value()]
    | {value(), value()}
    | {value(), value(), value()}
    | queue:queue()
    | #{atom() | binary() => value()}.
-type state() :: #{flushes := queue:queue(), atom() | binary() => value() | queue:queue()}.
-type call_from() :: {pid(), value()}.
-type action() :: {reply, call_from(), value()} | {state_timeout, non_neg_integer(), atom()}.
-type state_result() ::
    {keep_state, state(), [action()]}
    | {next_state, atom(), state(), [action()]}
    | {keep_state_and_data, action()}.
-type error_result() :: ok | {error, enats_client:error_reason()}.

-spec start_link(enats_client:options()) -> {ok, pid()} | {error, enats_client:error_reason()}.

start_link(Options) ->
    case validate_options(Options) of
        ok -> gen_statem:start_link(?MODULE, Options, []);
        Error -> Error
    end.
-spec connect(pid()) -> error_result().
connect(Pid) -> safe_call(Pid, connect, ?TIMEOUT).
-spec connect(pid(), non_neg_integer() | infinity) -> error_result().
connect(Pid, Timeout) -> safe_call(Pid, {connect, Timeout}, Timeout).
-spec disconnect(pid()) -> error_result().
disconnect(Pid) -> safe_call(Pid, disconnect, ?TIMEOUT).
-spec stop(pid()) -> error_result().
stop(Pid) ->
    try gen_statem:stop(Pid) of
        ok -> ok
    catch
        exit:noproc -> ok;
        exit:{noproc, _} -> ok;
        exit:Reason -> {error, {client_exit, Reason}}
    end.
-spec status(pid()) -> enats_client:status() | {error, enats_client:error_reason()}.
status(Pid) -> safe_call(Pid, status, ?TIMEOUT).
-spec info(pid()) -> enats_client:server_info() | {error, enats_client:error_reason()}.
info(Pid) -> safe_call(Pid, info, ?TIMEOUT).
-spec stats(pid()) -> enats_client:stats() | {error, enats_client:error_reason()}.
stats(Pid) -> safe_call(Pid, stats, ?TIMEOUT).
-spec publish(
    pid(), binary(), binary(), enats_client:publish_options()
) -> error_result().
publish(Pid, Subject, Payload, Options) ->
    safe_call(Pid, {publish, Subject, Payload, Options}, maps:get(timeout, Options, ?TIMEOUT)).
-spec request(pid(), binary(), binary(), enats_client:connection_request_options()) ->
    {ok, enats_client:message()} | {error, enats_client:error_reason()}.
request(Pid, Subject, Payload, Options) ->
    safe_call(Pid, {request, Subject, Payload, Options}, maps:get(timeout, Options, ?TIMEOUT)).
-spec subscribe(pid(), binary(), enats_client:subscribe_options()) ->
    {ok, reference()} | {error, enats_client:error_reason()}.
subscribe(Pid, Subject, Options) -> safe_call(Pid, {subscribe, Subject, Options}, ?TIMEOUT).
-spec unsubscribe(pid(), reference()) -> error_result().
unsubscribe(Pid, Ref) -> safe_call(Pid, {unsubscribe, Ref}, ?TIMEOUT).
-spec flush(pid(), non_neg_integer() | infinity) -> error_result().
flush(Pid, Timeout) -> safe_call(Pid, {flush, Timeout}, Timeout).
-spec drain(pid(), non_neg_integer() | infinity) -> error_result().
drain(Pid, Timeout) -> safe_call(Pid, {drain, Timeout}, Timeout).
-spec enable_diagnostics(pid(), enats_client:diagnostics_options()) -> error_result().
enable_diagnostics(Pid, Options) -> safe_call(Pid, {enable_diagnostics, Options}, ?TIMEOUT).
-spec disable_diagnostics(pid()) -> error_result().
disable_diagnostics(Pid) -> safe_call(Pid, disable_diagnostics, ?TIMEOUT).
-spec diagnostics(pid()) ->
    {ok, enats_client:diagnostics_snapshot()} | {error, enats_client:error_reason()}.
diagnostics(Pid) -> safe_call(Pid, diagnostics, ?TIMEOUT).
-spec reset_diagnostics(pid()) -> error_result().
reset_diagnostics(Pid) -> safe_call(Pid, reset_diagnostics, ?TIMEOUT).

safe_call(Pid, Request, Timeout) ->
    CallTimeout =
        case Timeout of
            infinity -> infinity;
            Value -> Value + 1000
        end,
    try gen_statem:call(Pid, Request, CallTimeout) of
        Result -> Result
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _} -> {error, disconnected};
        exit:Reason -> {error, {client_exit, Reason}}
    end.

-spec callback_mode() -> state_functions.
callback_mode() -> state_functions.

-spec init(enats_client:options()) -> {ok, atom(), state()}.
init(Options0) ->
    process_flag(trap_exit, true),
    Options = normalize_options(Options0),
    {ok, disconnected, #{
        options => Options,
        socket => undefined,
        parse_state => parser_state(Options),
        server_info => #{},
        connect_from => undefined,
        connect_attempts => 0,
        pending_connect_timeout => undefined,
        connect_deadline => undefined,
        flushes => queue:new(),
        subscriptions => #{},
        requests => #{},
        request_inbox => undefined,
        request_sid => undefined,
        next_sid => 1,
        servers => maps:get(servers, Options),
        configured_servers => maps:get(servers, Options),
        server_index => 1,
        current_server => undefined,
        heartbeat_timer => undefined,
        pings_out => 0,
        diagnostics => diagnostics_disabled(),
        connect_started_at => undefined,
        nats_started_at => undefined,
        delivery_started_at => undefined,
        drain_from => undefined,
        drain_barrier_done => false,
        reconnect_attempt => 0
    }}.

-spec disconnected(value(), value(), state()) -> state_result().
disconnected({call, From}, connect, State) ->
    connect_call(From, maps:get(connect_timeout, maps:get(options, State)), State);
disconnected({call, From}, {connect, Timeout}, State) ->
    connect_call(From, Timeout, State);
disconnected({call, From}, status, _State) ->
    reply(From, disconnected);
disconnected({call, From}, info, State) ->
    reply(From, maps:get(server_info, State));
disconnected({call, From}, stats, State) ->
    reply(From, stats_snapshot(disconnected, State));
disconnected({call, From}, diagnostics, State) ->
    reply(From, diagnostics_snapshot(State));
disconnected({call, From}, {enable_diagnostics, Options}, State) ->
    diagnostic_call(From, enable, Options, State);
disconnected({call, From}, disable_diagnostics, State) ->
    diagnostic_call(From, disable, #{}, State);
disconnected({call, From}, reset_diagnostics, State) ->
    diagnostic_call(From, reset, #{}, State);
disconnected({call, From}, disconnect, _State) ->
    reply(From, ok);
disconnected(info, {'DOWN', Monitor, process, _Owner, _Reason}, State) ->
    owner_down(Monitor, State);
disconnected({call, From}, _Request, _State) ->
    reply(From, {error, disconnected});
disconnected(_, _, State) ->
    keep(State).

connect_call(From, Timeout, State) ->
    Options = maps:get(options, State),
    Attempts = length(maps:get(servers, State)),
    State0 = State#{
        connect_from => From,
        connect_attempts => Attempts,
        pending_connect_timeout => Timeout,
        connect_started_at => diagnostic_now(State),
        connect_deadline => connection_deadline(Timeout)
    },
    case
        open_socket(
            State0, Attempts, Options, attempt_timeout(State0)
        )
    of
        {ok, Socket, State1} ->
            {next_state, waiting_info,
                State1#{
                    socket => Socket,
                    connect_attempts => max(Attempts - 1, 0),
                    parse_state => parser_state(maps:get(options, State1))
                },
                [
                    {state_timeout, attempt_timeout(State1), connect_timeout}
                ]};
        {error, Reason, State1} ->
            {keep_state, clear_pending_connect(State1), reply_action(From, {error, Reason})}
    end.

-spec waiting_info(value(), value(), state()) -> state_result().
waiting_info(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) ->
    process_data(Data, waiting_info, State);
waiting_info(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) ->
    process_data(Data, waiting_info, State);
waiting_info(info, {tcp_passive, Socket}, #{socket := {tcp, Socket}} = State) ->
    rearm_socket(State);
waiting_info(info, {ssl_passive, Socket}, #{socket := {ssl, Socket}} = State) ->
    rearm_socket(State);
waiting_info(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) ->
    connect_failed(waiting_info, closed, State);
waiting_info(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) ->
    connect_failed(waiting_info, closed, State);
waiting_info(state_timeout, connect_timeout, State) ->
    connect_failed(waiting_info, timeout, State);
waiting_info({call, From}, status, _State) ->
    reply(From, connecting);
waiting_info({call, From}, stats, State) ->
    reply(From, stats_snapshot(waiting_info, State));
waiting_info({call, From}, diagnostics, State) ->
    reply(From, diagnostics_snapshot(State));
waiting_info({call, From}, disconnect, State) ->
    reply_connect(State, {error, disconnected}),
    reply_requests(State, {error, disconnected}),
    close(State),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
waiting_info(info, {'DOWN', Monitor, process, _Owner, _Reason}, State) ->
    owner_down(Monitor, State);
waiting_info({call, From}, _Request, _State) ->
    reply(From, {error, connecting});
waiting_info(_, _, State) ->
    keep(State).

-spec waiting_pong(value(), value(), state()) -> state_result().
waiting_pong(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) ->
    process_data(Data, waiting_pong, State);
waiting_pong(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) ->
    process_data(Data, waiting_pong, State);
waiting_pong(info, {tcp_passive, Socket}, #{socket := {tcp, Socket}} = State) ->
    rearm_socket(State);
waiting_pong(info, {ssl_passive, Socket}, #{socket := {ssl, Socket}} = State) ->
    rearm_socket(State);
waiting_pong(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) ->
    connect_failed(waiting_pong, closed, State);
waiting_pong(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) ->
    connect_failed(waiting_pong, closed, State);
waiting_pong(state_timeout, connect_timeout, State) ->
    connect_failed(waiting_pong, timeout, State);
waiting_pong({call, From}, status, _State) ->
    reply(From, connecting);
waiting_pong({call, From}, stats, State) ->
    reply(From, stats_snapshot(waiting_pong, State));
waiting_pong({call, From}, diagnostics, State) ->
    reply(From, diagnostics_snapshot(State));
waiting_pong({call, From}, disconnect, State) ->
    reply_connect(State, {error, disconnected}),
    reply_requests(State, {error, disconnected}),
    close(State),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
waiting_pong(info, {'DOWN', Monitor, process, _Owner, _Reason}, State) ->
    owner_down(Monitor, State);
waiting_pong({call, From}, _Request, _State) ->
    reply(From, {error, connecting});
waiting_pong(_, _, State) ->
    keep(State).

-spec connected(value(), value(), state()) -> state_result().
connected({call, From}, status, _State) ->
    reply(From, connected);
connected({call, From}, info, State) ->
    reply(From, maps:get(server_info, State));
connected({call, From}, stats, State) ->
    reply(From, stats_snapshot(connected, State));
connected({call, From}, diagnostics, State) ->
    reply(From, diagnostics_snapshot(State));
connected({call, From}, {enable_diagnostics, Options}, State) ->
    diagnostic_call(From, enable, Options, State);
connected({call, From}, disable_diagnostics, State) ->
    diagnostic_call(From, disable, #{}, State);
connected({call, From}, reset_diagnostics, State) ->
    diagnostic_call(From, reset, #{}, State);
connected({call, From}, {drain, Timeout}, State) ->
    drain_call(From, Timeout, State);
connected({call, From}, connect, _State) ->
    reply(From, {error, already_connected});
connected({call, From}, {connect, _Timeout}, _State) ->
    reply(From, {error, already_connected});
connected({call, From}, {publish, Subject, Payload0, Options}, State) ->
    case validate_subject(Subject, false) of
        ok ->
            Payload = iolist_to_binary(Payload0),
            case validate_publish_options(Payload, Options, State) of
                ok ->
                    {SendResult, MeasuredState} = timed_publish(Subject, Payload, Options, State),
                    case SendResult of
                        ok -> {keep_state, MeasuredState, reply_action(From, ok)};
                        {error, Reason} -> lost_with_reply(From, Reason, MeasuredState)
                    end;
                {error, Reason} ->
                    reply(From, {error, Reason})
            end;
        {error, Reason} ->
            reply(From, {error, {invalid_subject, Reason}})
    end;
connected({call, From}, {request, Subject, Payload0, Options}, State) ->
    case validate_subject(Subject, false) of
        ok ->
            Payload = iolist_to_binary(Payload0),
            case validate_publish_options(Payload, Options, State) of
                ok ->
                    Token = integer_to_binary(maps:get(next_sid, State)),
                    Inbox = maps:get(request_inbox, State),
                    ReplyTo = <<Inbox/binary, ".", Token/binary>>,
                    RequestOptions = Options#{
                        reply_to => ReplyTo,
                        headers => request_headers(Options, State)
                    },
                    case validate_publish_options(Payload, RequestOptions, State) of
                        {error, Reason} ->
                            reply(From, {error, Reason});
                        ok ->
                            case
                                send_frame(publish_frame(Subject, Payload, RequestOptions), State)
                            of
                                ok ->
                                    Timer = request_timer(
                                        maps:get(timeout, Options, ?TIMEOUT), Token
                                    ),
                                    Metric = maps:get(diagnostic_metric, Options, request_latency),
                                    {Measure, SampledState} = take_message_sample(Metric, State),
                                    Requests = maps:put(
                                        Token,
                                        #{
                                            from => From,
                                            timer => Timer,
                                            metric => Metric,
                                            started_at =>
                                                case Measure of
                                                    true -> erlang:monotonic_time(microsecond);
                                                    false -> undefined
                                                end
                                        },
                                        maps:get(requests, SampledState)
                                    ),
                                    CountedState = record_counter(requests_started, SampledState),
                                    {keep_state,
                                        CountedState#{
                                            requests => Requests,
                                            next_sid => maps:get(next_sid, State) + 1
                                        },
                                        []};
                                {error, Reason} ->
                                    lost_with_reply(From, Reason, State)
                            end
                    end;
                {error, Reason} ->
                    reply(From, {error, Reason})
            end;
        {error, Reason} ->
            reply(From, {error, {invalid_subject, Reason}})
    end;
connected({call, From}, {subscribe, Subject, Options}, State) ->
    case validate_subject(Subject, true) of
        ok ->
            Sid = integer_to_binary(maps:get(next_sid, State)),
            Queue = maps:get(queue_group, Options, undefined),
            case send_frame({sub, Subject, Sid, Queue}, State) of
                ok ->
                    Ref = make_ref(),
                    Owner = maps:get(owner, Options, maps:get(owner, maps:get(options, State))),
                    Monitor = erlang:monitor(process, Owner),
                    Subscription = #{
                        ref => Ref,
                        sid => Sid,
                        subject => Subject,
                        queue_group => Queue,
                        owner => Owner,
                        monitor => Monitor
                    },
                    Subs = maps:put(Ref, Subscription, maps:get(subscriptions, State)),
                    {keep_state,
                        State#{subscriptions => Subs, next_sid => maps:get(next_sid, State) + 1},
                        reply_action(From, {ok, Ref})};
                {error, Reason} ->
                    lost_with_reply(From, Reason, State)
            end;
        {error, Reason} ->
            reply(From, {error, {invalid_subject, Reason}})
    end;
connected({call, From}, {unsubscribe, Ref}, State) ->
    case maps:take(Ref, maps:get(subscriptions, State)) of
        {#{sid := Sid}, Subs} ->
            maybe_demonitor(
                maps:get(monitor, maps:get(Ref, maps:get(subscriptions, State)), undefined)
            ),
            case send_frame({unsub, Sid}, State) of
                ok -> {keep_state, State#{subscriptions => Subs}, reply_action(From, ok)};
                {error, Reason} -> lost_with_reply(From, Reason, State)
            end;
        error ->
            reply(From, {error, not_found})
    end;
connected({call, From}, {flush, Timeout}, State) ->
    case send_frame(ping, State) of
        ok ->
            Ref = make_ref(),
            Flush = #{kind => flush, ref => Ref, from => From, timer => flush_timer(Timeout, Ref)},
            Flushes = queue:in(Flush, maps:get(flushes, State)),
            {keep_state, State#{flushes => Flushes}, []};
        {error, Reason} ->
            lost_with_reply(From, Reason, State)
    end;
connected(info, {flush_timeout, Ref}, State) ->
    {keep_state, expire_flush(Ref, State), []};
connected(info, heartbeat, State) ->
    heartbeat(State);
connected({call, From}, disconnect, State) ->
    reply_flushes(State, {error, disconnected}),
    reply_requests(State, {error, disconnected}),
    close(State),
    notify(State, disconnected, requested),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
connected(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) ->
    process_data(Data, connected, State);
connected(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) ->
    process_data(Data, connected, State);
connected(info, {tcp_passive, Socket}, #{socket := {tcp, Socket}} = State) ->
    rearm_socket(State);
connected(info, {ssl_passive, Socket}, #{socket := {ssl, Socket}} = State) ->
    rearm_socket(State);
connected(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) ->
    lost(connected, closed, State);
connected(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) ->
    lost(connected, closed, State);
connected(info, {request_timeout, Sid}, State) ->
    request_timeout(Sid, State);
connected(info, {'DOWN', Monitor, process, _Owner, _Reason}, State) ->
    owner_down(Monitor, State);
connected(_, _, State) ->
    keep(State).

-spec draining(value(), value(), state()) -> state_result().
draining({call, From}, status, _State) ->
    reply(From, draining);
draining({call, From}, stats, State) ->
    reply(From, stats_snapshot(draining, State));
draining({call, From}, diagnostics, State) ->
    reply(From, diagnostics_snapshot(State));
draining({call, From}, disconnect, State) ->
    close(State),
    reply_flushes(State, {error, disconnected}),
    reply_requests(State, {error, disconnected}),
    {next_state, disconnected, clear_socket(clear_subscriptions(State)), reply_action(From, ok)};
draining({call, From}, _Request, _State) ->
    reply(From, {error, draining});
draining(info, {tcp, Socket, Data}, #{socket := {tcp, Socket}} = State) ->
    process_data(Data, draining, State);
draining(info, {ssl, Socket, Data}, #{socket := {ssl, Socket}} = State) ->
    process_data(Data, draining, State);
draining(info, {tcp_passive, Socket}, #{socket := {tcp, Socket}} = State) ->
    rearm_socket(State);
draining(info, {ssl_passive, Socket}, #{socket := {ssl, Socket}} = State) ->
    rearm_socket(State);
draining(info, {tcp_closed, Socket}, #{socket := {tcp, Socket}} = State) ->
    drain_failed(closed, State);
draining(info, {ssl_closed, Socket}, #{socket := {ssl, Socket}} = State) ->
    drain_failed(closed, State);
draining(info, {flush_timeout, Ref}, State) ->
    State1 = expire_flush(Ref, State),
    close(State1),
    reply_requests(State1, {error, timeout}),
    {next_state, disconnected, clear_socket(clear_subscriptions(State1)), []};
draining(info, {request_timeout, Sid}, State) ->
    request_timeout(Sid, State);
draining(info, {'DOWN', Monitor, process, _Owner, _Reason}, State) ->
    owner_down(Monitor, State);
draining(_, _, State) ->
    keep(State).

drain_call(From, Timeout, State) ->
    case send_unsubscribes(State) of
        ok ->
            case send_frame(ping, State) of
                ok ->
                    Ref = make_ref(),
                    Drain = #{
                        kind => drain, ref => Ref, from => From, timer => flush_timer(Timeout, Ref)
                    },
                    Flushes = queue:in(Drain, maps:get(flushes, State)),
                    {next_state, draining,
                        State#{
                            flushes => Flushes,
                            drain_from => undefined,
                            drain_barrier_done => false
                        },
                        []};
                {error, Reason} ->
                    lost_with_reply(From, Reason, State)
            end;
        {error, Reason} ->
            lost_with_reply(From, Reason, State)
    end.

send_unsubscribes(State) ->
    maps:fold(
        fun
            (_Ref, #{sid := Sid}, ok) -> send_frame({unsub, Sid}, State);
            (_Ref, _Subscription, Error) -> Error
        end,
        ok,
        maps:get(subscriptions, State)
    ).

request_timeout(Sid, State) ->
    case maps:take(Sid, maps:get(requests, State)) of
        {#{from := From}, Requests} ->
            gen_statem:reply(From, {error, timeout}),
            maybe_finish_drain(
                record_counter(requests_timed_out, State#{requests => Requests})
            );
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
        {{value, #{kind := drain, from := From, timer := Timer}}, Flushes} ->
            cancel_flush_timer(Timer),
            {drained, State#{flushes => Flushes, pings_out => 0, drain_from => From}};
        {{value, #{kind := flush, from := From, timer := Timer}}, Flushes} ->
            cancel_flush_timer(Timer),
            maybe_reply_flush(From, ok),
            State#{flushes => Flushes, pings_out => 0};
        {{value, #{kind := heartbeat}}, Flushes} ->
            State#{flushes => Flushes, pings_out => 0};
        {empty, Flushes} ->
            State#{flushes => Flushes, pings_out => 0}
    end.

complete_pong_transition(State) ->
    case complete_pong(State) of
        {drained, #{drain_from := From} = State1} ->
            maybe_finish_drain(State1#{drain_barrier_done => true, drain_from => From});
        State1 ->
            {keep_state, State1, []}
    end.

maybe_finish_drain(#{drain_barrier_done := true, requests := Requests} = State) when
    map_size(Requests) =:= 0
->
    gen_statem:reply(maps:get(drain_from, State), ok),
    close(State),
    {next_state, disconnected, clear_socket(clear_subscriptions(State)), []};
maybe_finish_drain(State) ->
    {keep_state, State, []}.

heartbeat(#{pings_out := PingsOut, options := Options} = State) ->
    MaxPingsOut = maps:get(max_pings_out, Options),
    case PingsOut >= MaxPingsOut of
        true ->
            lost(connected, stale_connection, State);
        false ->
            case send_frame(ping, State) of
                ok ->
                    Flushes = queue:in(#{kind => heartbeat}, maps:get(flushes, State)),
                    {keep_state,
                        schedule_heartbeat(State#{flushes => Flushes, pings_out => PingsOut + 1}),
                        []};
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
    <<"_INBOX.enats.", (binary:encode_hex(crypto:strong_rand_bytes(12)))/binary>>.

-spec reconnecting(value(), value(), state()) -> state_result().
reconnecting(state_timeout, reconnect, State) ->
    case reconnect_allowed(State) of
        false ->
            notify(State, reconnect_exhausted, maps:get(last_error, State, exhausted)),
            {next_state, disconnected, clear_socket(State), []};
        true ->
            case open_socket(State) of
                {ok, Socket, State1} ->
                    {next_state, waiting_info,
                        State1#{
                            socket => Socket, parse_state => parser_state(maps:get(options, State1))
                        },
                        [
                            {state_timeout, attempt_timeout(State1), connect_timeout}
                        ]};
                {error, _Reason, State1} ->
                    NextState = State1#{
                        reconnect_attempt => maps:get(reconnect_attempt, State1, 0) + 1
                    },
                    {keep_state, NextState, [
                        {state_timeout, reconnect_delay(NextState), reconnect}
                    ]}
            end
    end;
reconnecting({call, From}, status, _State) ->
    reply(From, reconnecting);
reconnecting({call, From}, stats, State) ->
    reply(From, stats_snapshot(reconnecting, State));
reconnecting({call, From}, diagnostics, State) ->
    reply(From, diagnostics_snapshot(State));
reconnecting({call, From}, {enable_diagnostics, Options}, State) ->
    diagnostic_call(From, enable, Options, State);
reconnecting({call, From}, disable_diagnostics, State) ->
    diagnostic_call(From, disable, #{}, State);
reconnecting({call, From}, reset_diagnostics, State) ->
    diagnostic_call(From, reset, #{}, State);
reconnecting({call, From}, disconnect, State) ->
    reply_requests(State, {error, disconnected}),
    {next_state, disconnected, clear_socket(State), reply_action(From, ok)};
reconnecting(info, {'DOWN', Monitor, process, _Owner, _Reason}, State) ->
    owner_down(Monitor, State);
reconnecting({call, From}, _Request, _State) ->
    reply(From, {error, disconnected});
reconnecting(_, _, State) ->
    keep(State).

process_data(Data, StateName, State0) ->
    State1 = State0#{delivery_started_at => diagnostic_now(State0)},
    try enats_frame:parse(Data, maps:get(parse_state, State0)) of
        {Frames, ParseState} ->
            process_frames(Frames, StateName, State1#{parse_state => ParseState})
    catch
        _:_ -> lost(StateName, {protocol, invalid_frame}, State1)
    end.

process_frames([], _StateName, State) ->
    keep(State);
process_frames([Frame | Rest], StateName, State0) ->
    case process_frame(Frame, StateName, State0) of
        {next_state, NewName, State1, Actions} when Rest =:= [] ->
            {next_state, NewName, State1, Actions};
        {next_state, NewName, State1, Actions} ->
            append_transition(process_frames(Rest, NewName, State1), NewName, Actions);
        {keep_state, State1, Actions} when Rest =:= [] -> {keep_state, State1, Actions};
        {keep_state, State1, Actions} ->
            append_actions(process_frames(Rest, StateName, State1), Actions)
    end.

process_frame({info, RawInfo}, waiting_info, State) ->
    Info = normalize_info(RawInfo),
    Base0 = #{
        verbose => false,
        pedantic => false,
        tls_required => false,
        protocol => 1,
        headers => true,
        lang => <<"erlang">>,
        version => client_version()
    },
    Base =
        case maps:get(headers, Info, false) of
            true -> Base0#{no_responders => true};
            false -> Base0
        end,
    case maybe_upgrade_tls(Info, State) of
        {error, Reason} ->
            connect_failed(waiting_info, {tls_upgrade_failed, Reason}, State);
        {ok, TransportState} ->
            process_connect_info(Info, Base, update_servers(Info, TransportState))
    end;
process_frame({info, RawInfo}, connected, State) ->
    Info = normalize_info(RawInfo),
    State1 = update_servers(Info, State#{
        server_info => maps:merge(maps:get(server_info, State), Info)
    }),
    notify(State1, info_updated, Info),
    {keep_state, State1, []};
process_frame(pong, waiting_pong, State) ->
    MeasuredState = record_connect_latencies(State),
    ReconnectedState =
        case maps:get(reconnect_attempt, State, 0) > 0 of
            true -> record_counter(reconnects, MeasuredState);
            false -> MeasuredState
        end,
    reply_connect(ReconnectedState, ok),
    restore_subscriptions(ReconnectedState),
    notify(ReconnectedState, connected, maps:get(server_info, ReconnectedState)),
    {next_state, connected,
        start_heartbeat(clear_pending_connect(ReconnectedState#{reconnect_attempt => 0})), []};
process_frame({error, Reason}, waiting_info, State) ->
    connect_failed(waiting_info, {server_error, Reason}, State);
process_frame({error, Reason}, waiting_pong, State) ->
    connect_failed(waiting_pong, {server_error, Reason}, State);
process_frame({error, Reason}, connected, State) ->
    lost(connected, {server_error, Reason}, State);
process_frame(ping, _StateName, State) ->
    case send_frame(pong, State) of
        ok -> {keep_state, State, []};
        {error, Reason} -> lost(connected, {transport, Reason}, State)
    end;
process_frame(pong, connected, State) ->
    complete_pong_transition(State);
process_frame(pong, draining, State) ->
    complete_pong_transition(State);
process_frame(#{type := msg} = Message, draining, State) ->
    maybe_finish_drain(deliver(Message, State));
process_frame(#{type := hmsg} = Message, draining, State) ->
    maybe_finish_drain(deliver(Message, State));
process_frame(#{type := msg} = Message, connected, State) ->
    {keep_state, deliver(Message, State), []};
process_frame(#{type := hmsg} = Message, connected, State) ->
    {keep_state, deliver(Message, State), []};
process_frame(ok, _StateName, State) ->
    {keep_state, State, []};
process_frame(_Frame, _StateName, State) ->
    {keep_state, State, []}.

append_actions({keep_state, State, Later}, Actions) ->
    {keep_state, State, Actions ++ Later};
append_actions({next_state, Name, State, Later}, Actions) ->
    {next_state, Name, State, Actions ++ Later}.

append_transition({keep_state, State, Later}, Name, Actions) ->
    {next_state, Name, State, Actions ++ Later};
append_transition({next_state, Name, State, Later}, _OldName, Actions) ->
    {next_state, Name, State, Actions ++ Later}.

process_connect_info(Info, Base, State) ->
    case enats_auth:connect_params(maps:get(auth, maps:get(options, State)), Info, Base) of
        {ok, Params} ->
            RequestInbox = request_inbox(),
            RequestSid = integer_to_binary(maps:get(next_sid, State)),
            case send_frame({connect, Params}, State) of
                ok ->
                    case
                        send_frame(
                            {sub, <<RequestInbox/binary, ".*">>, RequestSid, undefined}, State
                        )
                    of
                        ok ->
                            case send_frame(ping, State) of
                                ok ->
                                    {next_state, waiting_pong,
                                        State#{
                                            server_info => Info,
                                            request_inbox => RequestInbox,
                                            request_sid => RequestSid,
                                            next_sid => maps:get(next_sid, State) + 1
                                        },
                                        [{state_timeout, attempt_timeout(State), connect_timeout}]};
                                {error, Reason} ->
                                    connect_failed(waiting_info, {transport, Reason}, State)
                            end;
                        {error, Reason} ->
                            connect_failed(waiting_info, {transport, Reason}, State)
                    end;
                {error, Reason} ->
                    connect_failed(waiting_info, {transport, Reason}, State)
            end;
        {error, Reason} ->
            connect_failed(waiting_info, Reason, State)
    end.

connect_failed(
    StateName, _Reason, #{connect_from := From, connect_attempts := Attempts} = State
) when
    From =/= undefined, Attempts > 0
->
    close(State),
    Timeout = attempt_timeout(State),
    State0 = reset_transport(State),
    case
        open_socket(State0, length(maps:get(servers, State0)), maps:get(options, State0), Timeout)
    of
        {ok, Socket, State1} ->
            {next_state, waiting_info,
                State1#{
                    socket => Socket,
                    connect_attempts => Attempts - 1,
                    parse_state => parser_state(maps:get(options, State1))
                },
                [
                    {state_timeout, attempt_timeout(State1), connect_timeout}
                ]};
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
open_socket(State, Attempts, Options, Timeout) when Timeout > 0; Timeout =:= infinity ->
    Servers = maps:get(servers, State),
    Index = maps:get(server_index, State),
    {Host, Port} = lists:nth(Index, Servers),
    NextIndex = next_server_index(Index, length(Servers)),
    State1 = record_counter(
        connect_attempts,
        State#{server_index => NextIndex, current_server => {Host, Port}}
    ),
    TransportStartedAt = diagnostic_now(State1),
    case open_transport(Host, Port, Options, Timeout) of
        {ok, Socket} ->
            State2 = record_latency(
                transport_connect_latency,
                diagnostic_elapsed(TransportStartedAt),
                State1
            ),
            {ok, Socket, State2#{nats_started_at => diagnostic_now(State2)}};
        {error, _Reason} when Attempts > 1 ->
            FailedState = record_counter(connect_failures, State1),
            open_socket(
                FailedState,
                Attempts - 1,
                Options,
                remaining_timeout(maps:get(connect_deadline, State1, infinity))
            );
        {error, Reason} ->
            {error, Reason, record_counter(connect_failures, State1)}
    end;
open_socket(State, _Attempts, _Options, _Timeout) ->
    {error, timeout, State}.

tcp_result({ok, Socket}) -> {ok, {tcp, Socket}};
tcp_result(Error) -> Error.

open_transport(
    Host,
    Port,
    #{
        tls := true,
        tls_handshake := first,
        ssl_opts := SslOpts,
        socket_active_n := ActiveN
    },
    Timeout
) ->
    ssl_result(
        safe_ssl_connect(
            Host,
            Port,
            transport_options(
                Host,
                [
                    binary, {active, ActiveN}, {server_name_indication, tls_server_name(Host)}
                ] ++ SslOpts
            ),
            Timeout
        )
    );
open_transport(Host, Port, #{socket_active_n := ActiveN}, Timeout) ->
    tcp_result(
        gen_tcp:connect(
            Host,
            Port,
            transport_options(Host, [
                binary, {active, ActiveN}, {nodelay, true}
            ]),
            Timeout
        )
    ).

safe_ssl_connect(Host, Port, Options, Timeout) ->
    try ssl:connect(Host, Port, Options, Timeout) of
        Result -> Result
    catch
        error:Reason -> {error, {invalid_ssl_options, Reason}}
    end.

ssl_result({ok, Socket}) -> {ok, {ssl, Socket}};
ssl_result(Error) -> Error.
maybe_upgrade_tls(
    Info, #{socket := {tcp, Socket}, options := Options, current_server := {Host, _Port}} = State
) ->
    NeedTLS =
        maps:get(tls, Options) andalso
            (maps:get(tls_required, Info, false) orelse maps:get(tls_available, Info, false)),
    case {maps:get(tls, Options), NeedTLS} of
        {true, false} ->
            {error, tls_not_available};
        {false, false} ->
            {ok, State};
        {true, true} ->
            _ = inet:setopts(Socket, [{active, false}]),
            ActiveN = maps:get(socket_active_n, Options),
            SslOpts = transport_options(Host, [
                {active, ActiveN},
                {server_name_indication, tls_server_name(Host)}
                | maps:get(ssl_opts, Options)
            ]),
            case safe_ssl_upgrade(Socket, SslOpts, connection_timeout(State)) of
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

safe_ssl_upgrade(Socket, Options, Timeout) ->
    try ssl:connect(Socket, Options, Timeout) of
        Result -> Result
    catch
        error:Reason -> {error, {invalid_ssl_options, Reason}}
    end.

send_frame({connect, Params}, #{socket := Socket}) ->
    send_socket(Socket, enats_frame:serialize_connect(Params));
send_frame(Frame, #{socket := Socket}) ->
    send_socket(Socket, enats_frame:serialize(Frame)).
send_socket({tcp, Socket}, Data) -> gen_tcp:send(Socket, Data);
send_socket({ssl, Socket}, Data) -> ssl:send(Socket, Data).

rearm_socket(#{socket := {tcp, Socket}, options := #{socket_active_n := ActiveN}} = State) ->
    case inet:setopts(Socket, [{active, ActiveN}]) of
        ok -> keep(State);
        {error, Reason} -> lost(connected, {transport, Reason}, State)
    end;
rearm_socket(#{socket := {ssl, Socket}, options := #{socket_active_n := ActiveN}} = State) ->
    case ssl:setopts(Socket, [{active, ActiveN}]) of
        ok -> keep(State);
        {error, Reason} -> lost(connected, {transport, Reason}, State)
    end.

diagnostic_elapsed(undefined) -> 0;
diagnostic_elapsed(StartedAt) -> erlang:monotonic_time(microsecond) - StartedAt.

publish_frame(Subject, Payload, Options) ->
    case maps:get(headers, Options, []) of
        [] ->
            case maps:get(reply_to, Options, undefined) of
                undefined -> {pub, Subject, Payload};
                ReplyTo -> {pub, Subject, ReplyTo, Payload}
            end;
        Headers ->
            {hpub, Subject, maps:get(reply_to, Options, undefined), Headers, Payload}
    end.

request_headers(Options, State) ->
    Headers = maps:get(headers, Options, []),
    case maps:get(headers, maps:get(server_info, State), true) of
        true -> [{<<"Nats-Request-Info">>, <<"true">>} | Headers];
        false -> Headers
    end.

timed_publish(Subject, Payload, Options, State) ->
    CountedState = record_counter(messages_out, State),
    case diagnostics_on(State) of
        false ->
            {send_frame(publish_frame(Subject, Payload, Options), State), CountedState};
        true ->
            {Measure, SampledState} = take_message_sample(publish_latency, CountedState),
            case Measure of
                false ->
                    {send_frame(publish_frame(Subject, Payload, Options), State), SampledState};
                true ->
                    StartedAt = erlang:monotonic_time(microsecond),
                    Result = send_frame(publish_frame(Subject, Payload, Options), State),
                    Duration = erlang:monotonic_time(microsecond) - StartedAt,
                    {Result, record_latency(publish_latency, Duration, SampledState)}
            end
    end.

validate_publish_options(Payload, Options, State) ->
    Info = maps:get(server_info, State),
    MaxPayload = maps:get(max_payload, Info, infinity),
    Headers = maps:get(headers, Options, []),
    case Headers =/= [] andalso maps:get(headers, Info, true) =:= false of
        true ->
            {error, headers_not_supported};
        false ->
            case enats_frame:validate_headers(Headers) of
                {error, _} = Error ->
                    Error;
                ok ->
                    MessageSize = byte_size(Payload) + enats_frame:headers_size(Headers),
                    case is_integer(MaxPayload) andalso MessageSize > MaxPayload of
                        true -> {error, {payload_too_large, MaxPayload}};
                        false -> ok
                    end
            end
    end.

restore_subscriptions(State) ->
    maps:foreach(
        fun(_Ref, #{subject := Subject, sid := Sid, queue_group := Queue}) ->
            _ = send_frame({sub, Subject, Sid, Queue}, State)
        end,
        maps:get(subscriptions, State)
    ).

deliver(Message, State) ->
    case request_token(Message, State) of
        {ok, Token} -> deliver_request(Token, Message, State);
        error -> deliver_subscription(Message, State)
    end.

deliver_request(Token, Message, State) ->
    case maps:take(Token, maps:get(requests, State)) of
        {#{from := From, timer := Timer, metric := Metric, started_at := StartedAt}, Requests} ->
            cancel_request_timer(Timer),
            gen_statem:reply(From, request_result(Message)),
            State1 = State#{requests => Requests},
            case StartedAt of
                undefined -> State1;
                _ -> record_latency(Metric, diagnostic_elapsed(StartedAt), State1)
            end;
        error ->
            State
    end.

request_token(#{subject := Subject}, #{request_inbox := Inbox}) when
    is_binary(Subject), is_binary(Inbox)
->
    Prefix = <<Inbox/binary, ".">>,
    case binary:match(Subject, Prefix) of
        {0, _} ->
            TokenSize = byte_size(Subject) - byte_size(Prefix),
            case TokenSize > 0 of
                true -> {ok, binary:part(Subject, byte_size(Prefix), TokenSize)};
                false -> error
            end;
        nomatch ->
            error
    end;
request_token(_Message, _State) ->
    error.

request_result(#{type := hmsg, payload := <<>>, headers := Headers} = Message) ->
    case lists:keyfind(<<"Status">>, 1, Headers) of
        {_, <<"503">> = Status} -> {error, {no_responders, Status}};
        _ -> {ok, Message}
    end;
request_result(Message) ->
    {ok, Message}.

deliver_subscription(Message, State) ->
    Sid = maps:get(sid, Message),
    State1 =
        case
            [Sub || Sub <- maps:values(maps:get(subscriptions, State)), maps:get(sid, Sub) =:= Sid]
        of
            [#{ref := Ref, owner := Owner} | _] ->
                case owner_is_slow(Owner, State) of
                    true ->
                        _ = send_frame({unsub, Sid}, State),
                        notify(State, slow_consumer, #{subscription => Ref}),
                        maybe_demonitor(
                            maps:get(
                                monitor, maps:get(Ref, maps:get(subscriptions, State)), undefined
                            )
                        ),
                        record_counter(
                            slow_consumers,
                            State#{
                                subscriptions => maps:remove(Ref, maps:get(subscriptions, State))
                            }
                        );
                    false ->
                        {Measure, SampledState} = take_message_sample(delivery_latency, State),
                        Owner ! {enats_client, self(), {message, Message}},
                        case Measure of
                            true ->
                                record_latency(
                                    delivery_latency,
                                    diagnostic_elapsed(
                                        maps:get(delivery_started_at, State, undefined)
                                    ),
                                    SampledState
                                );
                            false ->
                                SampledState
                        end
                end;
            [] ->
                State
        end,
    record_counter(messages_in, State1).

owner_is_slow(Owner, State) when is_pid(Owner) ->
    Limit = maps:get(slow_consumer_limit, maps:get(options, State), infinity),
    case {Limit, process_info(Owner, message_queue_len)} of
        {infinity, _} -> false;
        {Value, {message_queue_len, QueueLen}} when is_integer(Value), QueueLen >= Value -> true;
        _ -> false
    end;
owner_is_slow(_Owner, _State) ->
    false.

owner_down(Monitor, State) ->
    case
        [
            {Ref, Sid}
         || {Ref, #{monitor := SubscriptionMonitor, sid := Sid}} <- maps:to_list(
                maps:get(subscriptions, State)
            ),
            SubscriptionMonitor =:= Monitor
        ]
    of
        [{Ref, Sid}] ->
            maybe_unsubscribe(Sid, State),
            {keep_state, State#{subscriptions => maps:remove(Ref, maps:get(subscriptions, State))},
                []};
        [] ->
            keep(State)
    end.

maybe_unsubscribe(_Sid, #{socket := undefined}) ->
    ok;
maybe_unsubscribe(Sid, State) ->
    _ = send_frame({unsub, Sid}, State),
    ok.

drain_failed(Reason, State) ->
    close(State),
    reply_flushes(State, {error, {disconnected, Reason}}),
    reply_requests(State, {error, {disconnected, Reason}}),
    notify(State, disconnected, Reason),
    {next_state, disconnected, clear_socket(clear_subscriptions(State)), []}.

lost(_StateName, Reason, State) ->
    State0 = record_counter(error_counter(Reason), State#{last_error => Reason}),
    close(State0),
    reply_connect(State0, {error, Reason}),
    reply_flushes(State0, {error, {disconnected, Reason}}),
    reply_requests(State0, {error, {disconnected, Reason}}),
    notify(State0, disconnected, Reason),
    case reconnect_enabled(maps:get(reconnect, maps:get(options, State0))) of
        false ->
            {next_state, disconnected, clear_socket(State0), []};
        true ->
            ReconnectState = clear_socket(State0),
            {next_state, reconnecting,
                ReconnectState#{reconnect_attempt => maps:get(reconnect_attempt, State0, 0) + 1}, [
                    {state_timeout, reconnect_delay(ReconnectState), reconnect}
                ]}
    end.

error_counter({protocol, _}) -> protocol_errors;
error_counter({transport, _}) -> transport_errors;
error_counter({tls, _}) -> transport_errors;
error_counter({tls_upgrade_failed, _}) -> transport_errors;
error_counter({server_error, _}) -> protocol_errors;
error_counter(_) -> transport_errors.

lost_with_reply(From, Reason, State) ->
    gen_statem:reply(From, {error, Reason}),
    lost(connected, Reason, State).

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

clear_subscriptions(State) ->
    maps:foreach(
        fun(_Ref, Subscription) ->
            maybe_demonitor(maps:get(monitor, Subscription, undefined))
        end,
        maps:get(subscriptions, State)
    ),
    State#{subscriptions => #{}}.

maybe_demonitor(undefined) ->
    ok;
maybe_demonitor(Monitor) ->
    erlang:demonitor(Monitor, [flush]),
    ok.

clear_socket(State) ->
    clear_pending_connect(reset_transport(State)).

clear_pending_connect(State) ->
    State#{
        connect_from => undefined,
        connect_attempts => 0,
        pending_connect_timeout => undefined,
        connect_deadline => undefined,
        drain_from => undefined,
        drain_barrier_done => false
    }.

reset_transport(State) ->
    cancel_timer(maps:get(heartbeat_timer, State, undefined)),
    State#{
        socket => undefined,
        flushes => queue:new(),
        requests => #{},
        request_inbox => undefined,
        request_sid => undefined,
        heartbeat_timer => undefined,
        pings_out => 0,
        parse_state => parser_state(maps:get(options, State))
    }.
close(#{socket := undefined}) -> ok;
close(#{socket := {tcp, Socket}}) -> gen_tcp:close(Socket);
close(#{socket := {ssl, Socket}}) -> ssl:close(Socket).
validate_options(Options) when is_map(Options) ->
    case maps:get(tls_handshake, Options, starttls) of
        starttls -> validate_ssl_option_container(Options);
        first -> validate_ssl_option_container(Options);
        Value -> {error, {invalid_option, tls_handshake, Value}}
    end;
validate_options(Options) ->
    {error, {invalid_options, Options}}.

validate_ssl_option_container(Options) ->
    case maps:get(ssl_opts, Options, []) of
        SslOpts when is_list(SslOpts) -> validate_heartbeat_options(Options);
        SslOpts -> {error, {invalid_option, ssl_opts, SslOpts}}
    end.

validate_heartbeat_options(Options) ->
    PingInterval = maps:get(ping_interval, Options, 120000),
    MaxPingsOut = maps:get(max_pings_out, Options, 2),
    case is_integer(PingInterval) andalso PingInterval >= 0 of
        false ->
            {error, {invalid_option, ping_interval, PingInterval}};
        true ->
            case is_integer(MaxPingsOut) andalso MaxPingsOut > 0 of
                false ->
                    {error, {invalid_option, max_pings_out, MaxPingsOut}};
                true ->
                    ActiveN = maps:get(socket_active_n, Options, 100),
                    case is_integer(ActiveN) andalso ActiveN > 0 andalso ActiveN =< 32767 of
                        true -> validate_parser_limits(Options);
                        false -> {error, {invalid_option, socket_active_n, ActiveN}}
                    end
            end
    end.

validate_parser_limits(Options) ->
    Limits = [
        {max_control_line, maps:get(max_control_line, Options, 4096)},
        {max_message_size, maps:get(max_message_size, Options, 8 * 1024 * 1024)},
        {max_parser_buffer, maps:get(max_parser_buffer, Options, 8 * 1024 * 1024)}
    ],
    case
        lists:dropwhile(
            fun({_Name, Value}) -> is_integer(Value) andalso Value > 0 end,
            Limits
        )
    of
        [] -> ok;
        [{Name, Value} | _] -> {error, {invalid_option, Name, Value}}
    end.

normalize_options(Options) ->
    maps:merge(
        #{
            host => "127.0.0.1",
            port => 4222,
            tls => false,
            ssl_opts => [],
            tls_handshake => starttls,
            connect_timeout => 5000,
            reconnect => false,
            reconnect_delay => 1000,
            auth => none,
            owner => self(),
            notify => true,
            ping_interval => 120000,
            max_pings_out => 2,
            socket_active_n => 100,
            slow_consumer_limit => 10000,
            max_control_line => 4096,
            max_message_size => 8 * 1024 * 1024,
            max_parser_buffer => 8 * 1024 * 1024
        },
        normalize_tls_options(normalize_servers(Options))
    ).

normalize_tls_options(Options) ->
    case maps:get(tls, Options, false) of
        false ->
            Options;
        true ->
            SslOpts0 = maps:get(ssl_opts, Options, []),
            Verify = proplists:get_value(verify, SslOpts0, verify_peer),
            SslOpts1 = ensure_ssl_option(verify, Verify, SslOpts0),
            SslOpts = ensure_system_cacerts(Verify, SslOpts1),
            Options#{ssl_opts => SslOpts}
    end.

ensure_ssl_option(Key, Value, Options) ->
    case proplists:is_defined(Key, Options) of
        true -> Options;
        false -> [{Key, Value} | Options]
    end.

ensure_system_cacerts(verify_peer, Options) ->
    case proplists:is_defined(cacerts, Options) orelse proplists:is_defined(cacertfile, Options) of
        true -> Options;
        false -> [{cacerts, public_key:cacerts_get()} | Options]
    end;
ensure_system_cacerts(_Verify, Options) ->
    Options.

parser_state(Options) ->
    enats_frame:initial_state(
        #{
            max_control_line => maps:get(max_control_line, Options, 4096),
            max_message_size => maps:get(max_message_size, Options, 8 * 1024 * 1024),
            max_parser_buffer => maps:get(max_parser_buffer, Options, 8 * 1024 * 1024)
        }
    ).
normalize_info(Info) ->
    {Known, Extra} = maps:fold(
        fun(Key, Value, {KnownAcc, ExtraAcc}) ->
            case known_info_key(Key) of
                undefined -> {KnownAcc, [{Key, Value} | ExtraAcc]};
                NormalizedKey -> {maps:put(NormalizedKey, Value, KnownAcc), ExtraAcc}
            end
        end,
        {#{}, []},
        Info
    ),
    case Extra of
        [] -> Known;
        _ -> Known#{extra => lists:reverse(Extra)}
    end.

known_info_key(<<"server_id">>) -> server_id;
known_info_key(<<"server_name">>) -> server_name;
known_info_key(<<"version">>) -> version;
known_info_key(<<"go">>) -> go;
known_info_key(<<"host">>) -> host;
known_info_key(<<"port">>) -> port;
known_info_key(<<"headers">>) -> headers;
known_info_key(<<"max_payload">>) -> max_payload;
known_info_key(<<"proto">>) -> proto;
known_info_key(<<"auth_required">>) -> auth_required;
known_info_key(<<"tls_required">>) -> tls_required;
known_info_key(<<"tls_verify">>) -> tls_verify;
known_info_key(<<"tls_available">>) -> tls_available;
known_info_key(<<"connect_urls">>) -> connect_urls;
known_info_key(<<"ws_connect_urls">>) -> ws_connect_urls;
known_info_key(<<"jetstream">>) -> jetstream;
known_info_key(<<"nonce">>) -> nonce;
known_info_key(<<"client_id">>) -> client_id;
known_info_key(<<"client_ip">>) -> client_ip;
known_info_key(_) -> undefined.

client_version() ->
    _ = application:load(enats_client),
    case application:get_key(enats_client, vsn) of
        {ok, Version} when is_list(Version) -> list_to_binary(Version);
        {ok, Version} when is_binary(Version) -> Version;
        _ -> <<"unknown">>
    end.

reconnect_delay(#{options := #{reconnect := Policy}, reconnect_attempt := Attempt}) when
    is_map(Policy)
->
    Min = maps:get(min_delay, Policy, 100),
    Max = maps:get(max_delay, Policy, 5000),
    Multiplier = maps:get(multiplier, Policy, 2.0),
    Jitter = maps:get(jitter, Policy, 0.2),
    Base = min(Max, trunc(Min * math:pow(Multiplier, max(Attempt - 1, 0)))),
    apply_jitter(Base, Jitter);
reconnect_delay(State) ->
    maps:get(reconnect_delay, maps:get(options, State)).

apply_jitter(Delay, 0) ->
    Delay;
apply_jitter(Delay, Jitter) ->
    Offset = trunc(Delay * Jitter * (rand:uniform() * 2 - 1)),
    max(0, Delay + Offset).

reconnect_enabled(false) -> false;
reconnect_enabled(_Options) -> true.

reconnect_allowed(#{options := #{reconnect := Policy}, reconnect_attempt := Attempt}) when
    is_map(Policy)
->
    case maps:get(max_attempts, Policy, infinity) of
        infinity -> true;
        MaxAttempts -> Attempt =< MaxAttempts
    end;
reconnect_allowed(_State) ->
    true.

connection_deadline(infinity) -> infinity;
connection_deadline(Timeout) -> erlang:monotonic_time(millisecond) + Timeout.

remaining_timeout(infinity) -> infinity;
remaining_timeout(undefined) -> infinity;
remaining_timeout(Deadline) -> max(Deadline - erlang:monotonic_time(millisecond), 0).

attempt_timeout(State) ->
    DeadlineTimeout = remaining_timeout(maps:get(connect_deadline, State, undefined)),
    ConfiguredTimeout = maps:get(connect_timeout, maps:get(options, State), 5000),
    min_timeout(DeadlineTimeout, ConfiguredTimeout).

min_timeout(infinity, Value) -> Value;
min_timeout(Value, infinity) -> Value;
min_timeout(Left, Right) -> min(Left, Right).

connection_timeout(State) ->
    case maps:get(connect_deadline, State, undefined) of
        undefined -> maps:get(connect_timeout, maps:get(options, State));
        _Deadline -> attempt_timeout(State)
    end.

-spec terminate(value(), atom(), state()) -> ok.
terminate(_Reason, _StateName, State) ->
    close(State),
    ok.

diagnostics_disabled() ->
    #{
        enabled => false,
        sample_every => 100,
        sample_ticks => #{},
        counters => #{},
        latencies => #{}
    }.

diagnostics_enabled(Options) ->
    #{
        enabled => true,
        sample_every => maps:get(message_sample_every, Options, 100),
        sample_ticks => #{},
        counters => #{},
        latencies => #{}
    }.

diagnostics_on(State) ->
    maps:get(enabled, maps:get(diagnostics, State), false).

diagnostic_now(State) ->
    case diagnostics_on(State) of
        true -> erlang:monotonic_time(microsecond);
        false -> undefined
    end.

take_message_sample(Metric, State) ->
    Diagnostics = maps:get(diagnostics, State),
    case maps:get(enabled, Diagnostics, false) of
        false ->
            {false, State};
        true ->
            SampleTicks = maps:get(sample_ticks, Diagnostics, #{}),
            Tick = maps:get(Metric, SampleTicks, 0) + 1,
            Every = maps:get(sample_every, Diagnostics, 100),
            {Tick rem Every =:= 0, State#{
                diagnostics => Diagnostics#{sample_ticks => maps:put(Metric, Tick, SampleTicks)}
            }}
    end.

diagnostic_call(From, enable, Options, State) when is_map(Options) ->
    SampleEvery = maps:get(message_sample_every, Options, 100),
    case is_integer(SampleEvery) andalso SampleEvery > 0 of
        true ->
            {keep_state, State#{diagnostics => diagnostics_enabled(Options)},
                reply_action(From, ok)};
        false ->
            reply(From, {error, {invalid_option, message_sample_every, SampleEvery}})
    end;
diagnostic_call(From, disable, _Options, State) ->
    {keep_state, State#{diagnostics => diagnostics_disabled()}, reply_action(From, ok)};
diagnostic_call(From, reset, _Options, State) ->
    case maps:get(enabled, maps:get(diagnostics, State), false) of
        true ->
            Existing = maps:get(diagnostics, State),
            {keep_state,
                State#{
                    diagnostics => diagnostics_enabled(#{
                        message_sample_every => maps:get(sample_every, Existing, 100)
                    })
                },
                reply_action(From, ok)};
        false ->
            reply(From, {error, diagnostics_disabled})
    end.

stats_snapshot(StateName, State) ->
    #{
        status => stats_status(StateName),
        current_server => maps:get(current_server, State, undefined),
        reconnect_attempts => maps:get(reconnect_attempt, State, 0),
        subscriptions => map_size(maps:get(subscriptions, State)),
        pending_requests => map_size(maps:get(requests, State)),
        pending_flushes => queue:len(maps:get(flushes, State)),
        diagnostics_enabled => maps:get(enabled, maps:get(diagnostics, State), false),
        last_error => maps:get(last_error, State, undefined)
    }.

stats_status(waiting_info) -> connecting;
stats_status(waiting_pong) -> connecting;
stats_status(StateName) -> StateName.

diagnostics_snapshot(State) ->
    Diagnostics = maps:get(diagnostics, State),
    case maps:get(enabled, Diagnostics) of
        false ->
            {error, diagnostics_disabled};
        true ->
            {ok, #{
                enabled => true,
                counters => maps:get(counters, Diagnostics),
                latencies => maps:map(
                    fun(_Metric, Histogram) -> latency_summary(Histogram) end,
                    maps:get(latencies, Diagnostics)
                )
            }}
    end.

record_counter(Name, State) ->
    Diagnostics = maps:get(diagnostics, State),
    case maps:get(enabled, Diagnostics) of
        false ->
            State;
        true ->
            Counters = maps:get(counters, Diagnostics),
            State#{
                diagnostics => Diagnostics#{
                    counters =>
                        maps:put(Name, maps:get(Name, Counters, 0) + 1, Counters)
                }
            }
    end.

record_latency(Name, DurationUs, State) when is_integer(DurationUs), DurationUs >= 0 ->
    Diagnostics = maps:get(diagnostics, State),
    case maps:get(enabled, Diagnostics) of
        false ->
            State;
        true ->
            Histogram0 = maps:get(Name, maps:get(latencies, Diagnostics), new_histogram()),
            Histogram = histogram_add(DurationUs, Histogram0),
            Latencies = maps:put(Name, Histogram, maps:get(latencies, Diagnostics)),
            State#{diagnostics => Diagnostics#{latencies => Latencies}}
    end.

record_connect_latencies(State) ->
    State1 = record_latency(
        nats_connect_latency, diagnostic_elapsed(maps:get(nats_started_at, State, undefined)), State
    ),
    record_latency(
        total_connect_latency,
        diagnostic_elapsed(maps:get(connect_started_at, State1, undefined)),
        State1
    ).

new_histogram() -> #{samples => 0, min => undefined, max => undefined, buckets => #{}}.

histogram_add(DurationUs, #{samples := Samples, min := Min, max := Max, buckets := Buckets}) ->
    Bucket = latency_bucket(DurationUs),
    #{
        samples => Samples + 1,
        min => min_duration(Min, DurationUs),
        max => max_duration(Max, DurationUs),
        buckets => maps:put(Bucket, maps:get(Bucket, Buckets, 0) + 1, Buckets)
    }.

min_duration(undefined, Value) -> Value;
min_duration(Current, Value) -> min(Current, Value).

max_duration(undefined, Value) -> Value;
max_duration(Current, Value) -> max(Current, Value).

latency_bucket(0) -> 0;
latency_bucket(Value) -> latency_bucket(Value, 0).

latency_bucket(1, Bucket) -> Bucket;
latency_bucket(Value, Bucket) -> latency_bucket(Value bsr 1, Bucket + 1).

latency_summary(#{samples := Samples, min := Min, max := Max, buckets := Buckets}) ->
    #{
        samples => Samples,
        min_us => Min,
        max_us => Max,
        p50_us => percentile(Buckets, Samples, 50),
        p90_us => percentile(Buckets, Samples, 90),
        p95_us => percentile(Buckets, Samples, 95),
        p99_us => percentile(Buckets, Samples, 99)
    }.

percentile(_Buckets, 0, _Percent) ->
    undefined;
percentile(Buckets, Samples, Percent) ->
    Target = max(1, (Samples * Percent + 99) div 100),
    percentile_scan(lists:sort(maps:to_list(Buckets)), Target, 0).

percentile_scan([{Bucket, Count} | _Rest], Target, Seen) when Seen + Count >= Target ->
    1 bsl Bucket;
percentile_scan([{_Bucket, Count} | Rest], Target, Seen) ->
    percentile_scan(Rest, Target, Seen + Count);
percentile_scan([], _Target, _Seen) ->
    undefined.

normalize_servers(Options) ->
    case maps:get(servers, Options, undefined) of
        undefined ->
            Options#{
                servers => [{maps:get(host, Options, "127.0.0.1"), maps:get(port, Options, 4222)}]
            };
        Servers when is_list(Servers), Servers =/= [] -> Options;
        _ ->
            error({invalid_servers, maps:get(servers, Options)})
    end.

next_server_index(Index, Count) when Index >= Count -> 1;
next_server_index(Index, _Count) -> Index + 1.

update_servers(#{connect_urls := Urls}, State) when is_list(Urls) ->
    Existing = maps:get(configured_servers, State, maps:get(servers, State)),
    Discovered = [Server || Url <- Urls, {ok, Server} <- [parse_server_url(Url)]],
    Servers = unique_servers(Existing ++ Discovered),
    Index = min(maps:get(server_index, State, 1), length(Servers)),
    State#{servers => Servers, server_index => Index};
update_servers(_Info, State) ->
    State.

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

unique_servers(Servers) ->
    lists:reverse(
        lists:foldl(
            fun(Server, Acc) ->
                case lists:member(Server, Acc) of
                    true -> Acc;
                    false -> [Server | Acc]
                end
            end,
            [],
            Servers
        )
    ).

validate_subject(Subject, AllowWildcard) when is_binary(Subject), byte_size(Subject) > 0 ->
    case binary:match(Subject, [<<" ">>, <<"\t">>, <<"\r">>, <<"\n">>, <<0>>]) of
        nomatch ->
            case {AllowWildcard, binary:match(Subject, [<<"*">>, <<">">>])} of
                {false, {_, _}} -> {error, wildcard_subject_not_allowed};
                _ -> ok
            end;
        _ ->
            {error, invalid_subject}
    end;
validate_subject(_Subject, _AllowWildcard) ->
    {error, invalid_subject}.
