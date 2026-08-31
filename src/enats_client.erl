-module(enats_client).
-moduledoc "Public NATS client lifecycle, publish, request, subscription and diagnostics interface.".

-export([
    child_spec/1,
    start_link/1,
    connect/1,
    connect/2,
    disconnect/1,
    stop/1,
    status/1,
    info/1,
    stats/1,
    publish/3,
    publish/4,
    request/4,
    request/5,
    jetstream_publish/4,
    subscribe/3,
    unsubscribe/2,
    flush/2,
    drain/2,
    enable_diagnostics/2,
    disable_diagnostics/1,
    diagnostics/1,
    reset_diagnostics/1
]).

-type client() :: pid().
-type subject() :: binary().
-type headers() :: [{binary(), binary()}].
-type option_value() ::
    atom()
    | binary()
    | boolean()
    | integer()
    | float()
    | pid()
    | [option_value()]
    | #{atom() => option_value()}
    | fun().
-type options() :: #{atom() => option_value()}.
-type publish_options() :: #{
    headers => headers(),
    reply_to => subject(),
    timeout => call_timeout(),
    msg_id => binary()
}.
-type subscribe_options() :: #{queue_group => subject(), owner => pid()}.
-type call_timeout() :: non_neg_integer() | infinity.
-type status() :: disconnected | connecting | connected | reconnecting | draining.
-type message() :: #{
    type := msg | hmsg,
    subject := binary(),
    sid := binary(),
    payload := binary(),
    headers => headers(),
    reply_to => binary()
}.
-type diagnostics_options() :: #{message_sample_every => pos_integer()}.
-type latency_summary() :: #{
    samples := non_neg_integer(),
    min_us => non_neg_integer(),
    max_us => non_neg_integer(),
    p50_us => non_neg_integer(),
    p90_us => non_neg_integer(),
    p95_us => non_neg_integer(),
    p99_us => non_neg_integer()
}.
-type diagnostics_snapshot() :: #{
    enabled := boolean(),
    counters := #{atom() => non_neg_integer()},
    latencies := #{atom() => latency_summary()}
}.
-type error_reason() ::
    invalid_subject
    | wildcard_subject_not_allowed
    | invalid_options
    | invalid_payload
    | invalid_headers
    | invalid_timeout
    | disconnected
    | connecting
    | already_connected
    | timeout
    | diagnostics_disabled
    | atom()
    | {atom(), atom()}
    | {atom(), binary()}
    | {atom(), integer()}.
-export_type([
    client/0,
    subject/0,
    headers/0,
    options/0,
    publish_options/0,
    subscribe_options/0,
    call_timeout/0,
    status/0,
    message/0,
    diagnostics_options/0,
    latency_summary/0,
    diagnostics_snapshot/0,
    error_reason/0
]).

-spec child_spec(options()) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => maps:get(id, Options, enats_client),
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 5000,
        type => worker
    }.

-spec start_link(options()) -> {ok, client()} | {error, error_reason()}.
start_link(Options) when is_map(Options) ->
    case validate_start_options(Options) of
        ok -> enats_connection:start_link(Options#{owner => maps:get(owner, Options, self())});
        {error, _} = Error -> Error
    end;
start_link(_Options) ->
    {error, invalid_options}.

-spec connect(client()) -> ok | {error, error_reason()}.
connect(Client) -> enats_connection:connect(Client).

-spec connect(client(), call_timeout()) -> ok | {error, error_reason()}.
connect(Client, Timeout) ->
    case validate_timeout(Timeout) of
        ok -> enats_connection:connect(Client, Timeout);
        Error -> Error
    end.

-spec disconnect(client()) -> ok | {error, error_reason()}.
disconnect(Client) -> enats_connection:disconnect(Client).

-spec stop(client()) -> ok | {error, error_reason()}.
stop(Client) -> enats_connection:stop(Client).

-spec status(client()) -> status() | {error, error_reason()}.
status(Client) -> enats_connection:status(Client).

-spec info(client()) -> #{atom() | binary() => option_value()} | {error, error_reason()}.
info(Client) -> enats_connection:info(Client).

-spec stats(client()) ->
    #{atom() => non_neg_integer() | status() | undefined}
    | {error, error_reason()}.
stats(Client) -> enats_connection:stats(Client).

-spec publish(client(), subject(), iodata()) -> ok | {error, error_reason()}.
publish(Client, Subject, Payload) -> publish(Client, Subject, Payload, #{}).

-spec publish(client(), subject(), iodata(), publish_options()) -> ok | {error, error_reason()}.
publish(Client, Subject, Payload0, Options) when is_map(Options) ->
    with_subject(Subject, false, fun() ->
        with_payload(Payload0, fun(Payload) ->
            enats_connection:publish(Client, Subject, Payload, Options)
        end)
    end);
publish(_Client, _Subject, _Payload, _Options) ->
    {error, invalid_options}.

-spec request(client(), subject(), iodata(), publish_options()) ->
    {ok, message()} | {error, error_reason()}.
request(Client, Subject, Payload0, Options) when is_map(Options) ->
    with_subject(Subject, false, fun() ->
        with_payload(Payload0, fun(Payload) ->
            enats_connection:request(Client, Subject, Payload, Options)
        end)
    end);
request(_Client, _Subject, _Payload, _Options) ->
    {error, invalid_options}.

-spec request(client(), subject(), iodata(), publish_options(), call_timeout()) ->
    {ok, message()} | {error, error_reason()}.
request(Client, Subject, Payload, Options, Timeout) when is_map(Options) ->
    case validate_timeout(Timeout) of
        ok -> request(Client, Subject, Payload, Options#{timeout => Timeout});
        Error -> Error
    end;
request(_Client, _Subject, _Payload, _Options, _Timeout) ->
    {error, invalid_options}.

-spec jetstream_publish(client(), subject(), iodata(), publish_options()) ->
    {ok, #{stream := binary(), sequence := integer(), duplicate := boolean()}}
    | {error, error_reason()}.
jetstream_publish(Client, Subject, Payload, Options) when is_map(Options) ->
    Timeout = maps:get(timeout, Options, 5000),
    Headers0 = maps:get(headers, Options, []),
    case maps:get(msg_id, Options, undefined) of
        undefined ->
            jetstream_request(Client, Subject, Payload, Headers0, Timeout);
        MsgId when is_binary(MsgId) ->
            jetstream_request(
                Client, Subject, Payload, [{<<"Nats-Msg-Id">>, MsgId} | Headers0], Timeout
            );
        _ ->
            {error, invalid_options}
    end;
jetstream_publish(_Client, _Subject, _Payload, _Options) ->
    {error, invalid_options}.

-spec subscribe(client(), subject(), subscribe_options()) ->
    {ok, reference()} | {error, error_reason()}.
subscribe(Client, Subject, Options) when is_map(Options) ->
    case validate_subscribe_options(Options) of
        ok ->
            with_subject(Subject, true, fun() ->
                enats_connection:subscribe(Client, Subject, Options)
            end);
        Error ->
            Error
    end;
subscribe(_Client, _Subject, _Options) ->
    {error, invalid_options}.

-spec unsubscribe(client(), reference()) -> ok | {error, error_reason()}.
unsubscribe(Client, Subscription) when is_reference(Subscription) ->
    enats_connection:unsubscribe(Client, Subscription);
unsubscribe(_Client, _Subscription) ->
    {error, invalid_argument}.

-spec flush(client(), call_timeout()) -> ok | {error, error_reason()}.
flush(Client, Timeout) ->
    case validate_timeout(Timeout) of
        ok -> enats_connection:flush(Client, Timeout);
        Error -> Error
    end.

-spec drain(client(), call_timeout()) -> ok | {error, error_reason()}.
drain(Client, Timeout) ->
    case validate_timeout(Timeout) of
        ok -> enats_connection:drain(Client, Timeout);
        Error -> Error
    end.

-spec enable_diagnostics(client(), diagnostics_options()) -> ok | {error, error_reason()}.
enable_diagnostics(Client, Options) when is_map(Options) ->
    enats_connection:enable_diagnostics(Client, Options);
enable_diagnostics(_Client, _Options) ->
    {error, invalid_options}.

-spec disable_diagnostics(client()) -> ok | {error, error_reason()}.
disable_diagnostics(Client) -> enats_connection:disable_diagnostics(Client).

-spec diagnostics(client()) -> {ok, diagnostics_snapshot()} | {error, error_reason()}.
diagnostics(Client) -> enats_connection:diagnostics(Client).

-spec reset_diagnostics(client()) -> ok | {error, error_reason()}.
reset_diagnostics(Client) -> enats_connection:reset_diagnostics(Client).

validate_start_options(Options) ->
    case
        {
            enats_auth:validate(maps:get(auth, Options, none)),
            validate_option_timeout(connect_timeout, maps:get(connect_timeout, Options, 5000)),
            validate_option_timeout(reconnect_delay, maps:get(reconnect_delay, Options, 1000)),
            validate_option_timeout(ping_interval, maps:get(ping_interval, Options, 120000)),
            validate_positive_integer(max_pings_out, maps:get(max_pings_out, Options, 2)),
            validate_active_n(maps:get(socket_active_n, Options, 100)),
            validate_reconnect(maps:get(reconnect, Options, false)),
            validate_nonnegative_integer(
                slow_consumer_limit, maps:get(slow_consumer_limit, Options, 10000)
            )
        }
    of
        {ok, ok, ok, ok, ok, ok, ok, ok} -> ok;
        {Error, _, _, _, _, _, _, _} when Error =/= ok -> Error;
        {_, Error, _, _, _, _, _, _} when Error =/= ok -> Error;
        {_, _, Error, _, _, _, _, _} when Error =/= ok -> Error;
        {_, _, _, Error, _, _, _, _} when Error =/= ok -> Error;
        {_, _, _, _, Error, _, _, _} when Error =/= ok -> Error;
        {_, _, _, _, _, Error, _, _} when Error =/= ok -> Error;
        {_, _, _, _, _, _, Error, _} when Error =/= ok -> Error;
        {_, _, _, _, _, _, _, Error} -> Error
    end.

validate_timeout(infinity) -> ok;
validate_timeout(Value) when is_integer(Value), Value >= 0 -> ok;
validate_timeout(_Value) -> {error, invalid_timeout}.

validate_option_timeout(_Name, infinity) -> ok;
validate_option_timeout(_Name, Value) when is_integer(Value), Value >= 0 -> ok;
validate_option_timeout(Name, Value) -> {error, {invalid_option, Name, Value}}.

validate_positive_integer(_Name, Value) when is_integer(Value), Value > 0 -> ok;
validate_positive_integer(Name, Value) -> {error, {invalid_option, Name, Value}}.

validate_attempts(infinity) -> ok;
validate_attempts(Value) -> validate_positive_integer(max_attempts, Value).

validate_nonnegative_integer(_Name, Value) when is_integer(Value), Value >= 0 -> ok;
validate_nonnegative_integer(Name, Value) -> {error, {invalid_option, Name, Value}}.

validate_active_n(Value) when is_integer(Value), Value > 0, Value =< 32767 -> ok;
validate_active_n(Value) -> {error, {invalid_option, socket_active_n, Value}}.

validate_reconnect(false) ->
    ok;
validate_reconnect(true) ->
    ok;
validate_reconnect(Options) when is_map(Options) ->
    case
        {
            validate_option_timeout(min_delay, maps:get(min_delay, Options, 100)),
            validate_option_timeout(max_delay, maps:get(max_delay, Options, 5000)),
            validate_attempts(maps:get(max_attempts, Options, infinity))
        }
    of
        {ok, ok, ok} -> ok;
        {Error, _, _} when Error =/= ok -> Error;
        {_, Error, _} when Error =/= ok -> Error;
        {_, _, Error} -> Error
    end;
validate_reconnect(Value) ->
    {error, {invalid_option, reconnect, Value}}.

validate_subscribe_options(Options) ->
    case {maps:get(owner, Options, self()), maps:get(queue_group, Options, undefined)} of
        {Owner, QueueGroup} when
            is_pid(Owner), (is_binary(QueueGroup) orelse QueueGroup =:= undefined)
        ->
            ok;
        _ ->
            {error, invalid_options}
    end.

with_subject(Subject, AllowWildcard, Fun) when is_binary(Subject), byte_size(Subject) > 0 ->
    case binary:match(Subject, [<<" ">>, <<"\t">>, <<"\r">>, <<"\n">>, <<0>>]) of
        nomatch ->
            case {AllowWildcard, binary:match(Subject, [<<"*">>, <<">">>])} of
                {false, {_, _}} -> {error, wildcard_subject_not_allowed};
                _ -> Fun()
            end;
        _ ->
            {error, invalid_subject}
    end;
with_subject(_Subject, _AllowWildcard, _Fun) ->
    {error, invalid_subject}.

with_payload(Payload0, Fun) ->
    try
        Fun(iolist_to_binary(Payload0))
    catch
        _:_ -> {error, invalid_payload}
    end.

jetstream_request(Client, Subject, Payload, Headers, Timeout) ->
    case request(Client, Subject, Payload, #{headers => Headers, timeout => Timeout}) of
        {ok, #{payload := Response} = Message} ->
            case response_status(maps:get(headers, Message, [])) of
                undefined -> decode_pub_ack(Response);
                Status -> classify_status(Status)
            end;
        {error, {no_responders, Status}} ->
            {error, {jetstream, unavailable, Status}};
        {error, _} = Error ->
            Error
    end.

response_status(Headers) ->
    case lists:keyfind(<<"Status">>, 1, Headers) of
        {_, Status} -> Status;
        false -> undefined
    end.

classify_status(<<"5", _/binary>> = Status) -> {error, {jetstream, unavailable, Status}};
classify_status(Status) -> {error, {jetstream, rejected, Status}}.

decode_pub_ack(Payload) ->
    try jiffy:decode(Payload, [return_maps]) of
        #{<<"error">> := Error} ->
            classify_error(Error);
        #{<<"stream">> := Stream, <<"seq">> := Sequence} = Ack ->
            {ok, #{
                stream => Stream,
                sequence => Sequence,
                duplicate => maps:get(<<"duplicate">>, Ack, false)
            }};
        _Other ->
            {error, {jetstream, invalid_ack, invalid_payload}}
    catch
        _:_ -> {error, {jetstream, invalid_ack, invalid_payload}}
    end.

classify_error(#{<<"code">> := Code}) when is_integer(Code), Code >= 500 ->
    {error, {jetstream, unavailable, Code}};
classify_error(_Error) ->
    {error, {jetstream, rejected, invalid_payload}}.
