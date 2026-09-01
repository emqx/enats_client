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
-type host() :: binary() | string() | inet:ip_address().
-type server() :: {host(), inet:port_number()}.
-type json_value() ::
    null
    | boolean()
    | integer()
    | float()
    | binary()
    | [json_value()]
    | #{binary() => json_value()}.
-type reconnect_options() :: #{
    min_delay => non_neg_integer(),
    max_delay => non_neg_integer(),
    multiplier => number(),
    jitter => number(),
    max_attempts => pos_integer() | infinity
}.
-type options() :: #{
    id => atom(),
    host => host(),
    port => inet:port_number(),
    servers => [server()],
    tls => boolean(),
    tls_handshake => starttls | first,
    ssl_opts => [ssl:tls_client_option()],
    auth => enats_auth:auth(),
    connect_timeout => call_timeout(),
    reconnect => boolean() | reconnect_options(),
    reconnect_delay => non_neg_integer(),
    owner => pid(),
    notify => boolean(),
    ping_interval => non_neg_integer(),
    max_pings_out => pos_integer(),
    socket_active_n => 1..32767,
    slow_consumer_limit => non_neg_integer(),
    max_control_line => pos_integer(),
    max_message_size => pos_integer(),
    max_parser_buffer => pos_integer()
}.
-type publish_options() :: #{
    headers => headers(),
    reply_to => subject(),
    timeout => call_timeout(),
    msg_id => binary()
}.
-type connection_request_options() :: #{
    headers => headers(),
    reply_to => subject(),
    timeout => call_timeout(),
    msg_id => binary(),
    diagnostic_metric => metric()
}.
-type subscribe_options() :: #{queue_group => subject(), owner => pid()}.
-type call_timeout() :: non_neg_integer() | infinity.
-type status() :: disconnected | connecting | connected | reconnecting | draining.
-type server_info() :: #{
    server_id => binary(),
    server_name => binary(),
    version => binary(),
    go => binary(),
    host => binary(),
    port => inet:port_number(),
    headers => boolean(),
    max_payload => pos_integer(),
    proto => non_neg_integer(),
    auth_required => boolean(),
    tls_required => boolean(),
    tls_verify => boolean(),
    tls_available => boolean(),
    connect_urls => [binary()],
    ws_connect_urls => [binary()],
    jetstream => boolean(),
    nonce => binary(),
    client_id => pos_integer(),
    client_ip => binary(),
    extra => [{binary(), json_value()}]
}.
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
-type stats() :: #{
    status := status(),
    current_server => server(),
    reconnect_attempts := non_neg_integer(),
    subscriptions := non_neg_integer(),
    pending_requests := non_neg_integer(),
    pending_flushes := non_neg_integer(),
    diagnostics_enabled := boolean(),
    last_error => error_reason()
}.
-type metric() ::
    transport_connect_latency
    | nats_connect_latency
    | total_connect_latency
    | publish_latency
    | request_latency
    | jetstream_publish_latency
    | delivery_latency.
-type counter() ::
    messages_in
    | messages_out
    | connect_attempts
    | connect_failures
    | reconnects
    | requests_started
    | requests_timed_out
    | slow_consumers
    | protocol_errors
    | transport_errors.
-type diagnostics_snapshot() :: #{
    enabled := boolean(),
    counters := #{counter() => non_neg_integer()},
    latencies := #{metric() => latency_summary()}
}.
-type error_value() ::
    atom()
    | binary()
    | integer()
    | pid()
    | reference()
    | boolean()
    | undefined
    | [error_value()]
    | {error_value(), error_value()}
    | {error_value(), error_value(), error_value()}
    | {error_value(), error_value(), error_value(), error_value()}
    | {error_value(), error_value(), error_value(), error_value(), error_value()}.
-type error_reason() ::
    invalid_subject
    | wildcard_subject_not_allowed
    | invalid_options
    | invalid_payload
    | invalid_headers
    | invalid_argument
    | invalid_timeout
    | disconnected
    | connecting
    | already_connected
    | timeout
    | diagnostics_disabled
    | draining
    | not_found
    | headers_not_supported
    | tls_not_available
    | tls_already_established
    | no_servers_available
    | closed
    | stale_connection
    | requested
    | {invalid_option, atom()}
    | {invalid_option, atom(), error_value()}
    | {invalid_headers, error_value()}
    | {invalid_header_name, binary()}
    | {invalid_header_value, binary()}
    | {invalid_header, error_value()}
    | {payload_too_large, pos_integer()}
    | {server_error, binary()}
    | {transport, error_value()}
    | {tls_upgrade_failed, error_value()}
    | {invalid_ssl_options, error_value()}
    | {protocol, error_value()}
    | {auth, error_value()}
    | {no_responders, binary()}
    | {disconnected, error_value()}
    | {client_exit, error_value()}
    | {jetstream, unavailable | rejected | invalid_ack, error_value()}.
-export_type([
    client/0,
    subject/0,
    host/0,
    server/0,
    headers/0,
    options/0,
    publish_options/0,
    connection_request_options/0,
    subscribe_options/0,
    call_timeout/0,
    status/0,
    message/0,
    diagnostics_options/0,
    latency_summary/0,
    diagnostics_snapshot/0,
    stats/0,
    server_info/0,
    reconnect_options/0,
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

-spec info(client()) -> server_info() | {error, error_reason()}.
info(Client) -> enats_connection:info(Client).

-spec stats(client()) -> stats() | {error, error_reason()}.
stats(Client) -> enats_connection:stats(Client).

-spec publish(client(), subject(), iodata()) -> ok | {error, error_reason()}.
publish(Client, Subject, Payload) -> publish(Client, Subject, Payload, #{}).

-spec publish(client(), subject(), iodata(), publish_options()) -> ok | {error, error_reason()}.
publish(Client, Subject, Payload0, Options) when is_map(Options) ->
    case validate_publish_options(Options, false) of
        ok ->
            with_subject(Subject, false, fun() ->
                with_payload(Payload0, fun(Payload) ->
                    enats_connection:publish(Client, Subject, Payload, Options)
                end)
            end);
        Error ->
            Error
    end;
publish(_Client, _Subject, _Payload, _Options) ->
    {error, invalid_options}.

-spec request(client(), subject(), iodata(), publish_options()) ->
    {ok, message()} | {error, error_reason()}.
request(Client, Subject, Payload0, Options) when is_map(Options) ->
    case validate_publish_options(Options, false) of
        ok ->
            with_subject(Subject, false, fun() ->
                with_payload(Payload0, fun(Payload) ->
                    enats_connection:request(Client, Subject, Payload, Options)
                end)
            end);
        Error ->
            Error
    end;
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
    case validate_publish_options(Options, true) of
        ok ->
            Timeout = maps:get(timeout, Options, 5000),
            Headers0 = maps:get(headers, Options, []),
            case maps:get(msg_id, Options, undefined) of
                undefined ->
                    jetstream_request(Client, Subject, Payload, Headers0, Timeout);
                MsgId when is_binary(MsgId) ->
                    jetstream_request(
                        Client,
                        Subject,
                        Payload,
                        [{<<"Nats-Msg-Id">>, MsgId} | Headers0],
                        Timeout
                    );
                _ ->
                    {error, {invalid_option, msg_id}}
            end;
        Error ->
            Error
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
    case validate_allowed_keys(diagnostics, Options, [message_sample_every]) of
        ok -> enats_connection:enable_diagnostics(Client, Options);
        Error -> Error
    end;
enable_diagnostics(_Client, _Options) ->
    {error, invalid_options}.

-spec disable_diagnostics(client()) -> ok | {error, error_reason()}.
disable_diagnostics(Client) -> enats_connection:disable_diagnostics(Client).

-spec diagnostics(client()) -> {ok, diagnostics_snapshot()} | {error, error_reason()}.
diagnostics(Client) -> enats_connection:diagnostics(Client).

-spec reset_diagnostics(client()) -> ok | {error, error_reason()}.
reset_diagnostics(Client) -> enats_connection:reset_diagnostics(Client).

validate_start_options(Options) ->
    Checks = [
        validate_allowed_keys(options, Options, [
            id,
            host,
            port,
            servers,
            tls,
            tls_handshake,
            ssl_opts,
            auth,
            connect_timeout,
            reconnect,
            reconnect_delay,
            owner,
            notify,
            ping_interval,
            max_pings_out,
            socket_active_n,
            slow_consumer_limit,
            max_control_line,
            max_message_size,
            max_parser_buffer
        ]),
        enats_auth:validate(maps:get(auth, Options, none)),
        validate_host(maps:get(host, Options, "127.0.0.1")),
        validate_port(maps:get(port, Options, 4222)),
        validate_servers(maps:get(servers, Options, undefined)),
        validate_boolean(tls, maps:get(tls, Options, false)),
        validate_tls_handshake(maps:get(tls_handshake, Options, starttls)),
        validate_ssl_opts(maps:get(ssl_opts, Options, [])),
        validate_boolean(notify, maps:get(notify, Options, true)),
        validate_owner(maps:get(owner, Options, self())),
        validate_option_timeout(connect_timeout, maps:get(connect_timeout, Options, 5000)),
        validate_option_timeout(reconnect_delay, maps:get(reconnect_delay, Options, 1000)),
        validate_option_timeout(ping_interval, maps:get(ping_interval, Options, 120000)),
        validate_positive_integer(max_pings_out, maps:get(max_pings_out, Options, 2)),
        validate_active_n(maps:get(socket_active_n, Options, 100)),
        validate_reconnect(maps:get(reconnect, Options, false)),
        validate_nonnegative_integer(
            slow_consumer_limit, maps:get(slow_consumer_limit, Options, 10000)
        ),
        validate_positive_integer(max_control_line, maps:get(max_control_line, Options, 4096)),
        validate_positive_integer(
            max_message_size, maps:get(max_message_size, Options, 8 * 1024 * 1024)
        ),
        validate_positive_integer(
            max_parser_buffer, maps:get(max_parser_buffer, Options, 8 * 1024 * 1024)
        )
    ],
    case lists:dropwhile(fun(Result) -> Result =:= ok end, Checks) of
        [] -> ok;
        [Error | _] -> Error
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

validate_multiplier(Value) when is_integer(Value), Value >= 1 -> ok;
validate_multiplier(Value) when is_float(Value), Value >= 1.0 -> ok;
validate_multiplier(Value) -> {error, {invalid_option, multiplier, Value}}.

validate_jitter(Value) when is_integer(Value), Value >= 0, Value =< 1 -> ok;
validate_jitter(Value) when is_float(Value), Value >= 0.0, Value =< 1.0 -> ok;
validate_jitter(Value) -> {error, {invalid_option, jitter, Value}}.

validate_delay_range(Options) ->
    Min = maps:get(min_delay, Options, 100),
    Max = maps:get(max_delay, Options, 5000),
    case is_integer(Min) andalso is_integer(Max) andalso Min =< Max of
        true -> ok;
        false -> {error, {invalid_option, reconnect_delay_range, {Min, Max}}}
    end.

validate_nonnegative_integer(_Name, Value) when is_integer(Value), Value >= 0 -> ok;
validate_nonnegative_integer(Name, Value) -> {error, {invalid_option, Name, Value}}.

validate_host(Value) when is_binary(Value), byte_size(Value) > 0 ->
    validate_host_text(Value);
validate_host(Value) when is_list(Value), Value =/= [] ->
    try unicode:characters_to_binary(Value) of
        Binary when is_binary(Binary) -> validate_host_text(Binary);
        _ -> {error, {invalid_option, host, Value}}
    catch
        _:_ -> {error, {invalid_option, host, Value}}
    end;
validate_host(Value) ->
    case inet:is_ip_address(Value) of
        true -> ok;
        false -> {error, {invalid_option, host, Value}}
    end.

validate_host_text(Value) ->
    case binary:match(Value, [<<" ">>, <<"\t">>, <<"\r">>, <<"\n">>, <<0>>]) of
        nomatch -> ok;
        _ -> {error, {invalid_option, host, Value}}
    end.

validate_port(Value) when is_integer(Value), Value > 0, Value =< 65535 -> ok;
validate_port(Value) -> {error, {invalid_option, port, Value}}.

validate_servers(undefined) ->
    ok;
validate_servers(Servers) when is_list(Servers), Servers =/= [] ->
    case
        lists:all(
            fun
                ({Host, Port}) -> validate_host(Host) =:= ok andalso validate_port(Port) =:= ok;
                (_) -> false
            end,
            Servers
        )
    of
        true -> ok;
        false -> {error, {invalid_option, servers, Servers}}
    end;
validate_servers(Value) ->
    {error, {invalid_option, servers, Value}}.

validate_allowed_keys(Scope, Options, AllowedKeys) when is_map(Options) ->
    UnknownKeys = lists:sort([Key || Key <- maps:keys(Options), not lists:member(Key, AllowedKeys)]),
    case UnknownKeys of
        [] -> ok;
        _ -> {error, {invalid_option, Scope, {unknown_keys, UnknownKeys}}}
    end.

validate_boolean(_Name, Value) when is_boolean(Value) -> ok;
validate_boolean(Name, Value) -> {error, {invalid_option, Name, Value}}.

validate_tls_handshake(starttls) -> ok;
validate_tls_handshake(first) -> ok;
validate_tls_handshake(Value) -> {error, {invalid_option, tls_handshake, Value}}.

validate_ssl_opts(Value) when is_list(Value) -> ok;
validate_ssl_opts(Value) -> {error, {invalid_option, ssl_opts, Value}}.

validate_owner(Value) when is_pid(Value) -> ok;
validate_owner(Value) -> {error, {invalid_option, owner, Value}}.

validate_active_n(Value) when is_integer(Value), Value > 0, Value =< 32767 -> ok;
validate_active_n(Value) -> {error, {invalid_option, socket_active_n, Value}}.

validate_reconnect(false) ->
    ok;
validate_reconnect(true) ->
    ok;
validate_reconnect(Options) when is_map(Options) ->
    case
        validate_allowed_keys(reconnect, Options, [
            min_delay, max_delay, multiplier, jitter, max_attempts
        ])
    of
        ok ->
            case
                {
                    validate_option_timeout(min_delay, maps:get(min_delay, Options, 100)),
                    validate_option_timeout(max_delay, maps:get(max_delay, Options, 5000)),
                    validate_attempts(maps:get(max_attempts, Options, infinity)),
                    validate_multiplier(maps:get(multiplier, Options, 2.0)),
                    validate_jitter(maps:get(jitter, Options, 0.2)),
                    validate_delay_range(Options)
                }
            of
                {ok, ok, ok, ok, ok, ok} -> ok;
                {Error, _, _, _, _, _} when Error =/= ok -> Error;
                {_, Error, _, _, _, _} when Error =/= ok -> Error;
                {_, _, Error, _, _, _} when Error =/= ok -> Error;
                {_, _, _, Error, _, _} when Error =/= ok -> Error;
                {_, _, _, _, Error, _} when Error =/= ok -> Error;
                {_, _, _, _, _, Error} -> Error
            end;
        Error ->
            Error
    end;
validate_reconnect(Value) ->
    {error, {invalid_option, reconnect, Value}}.

validate_subscribe_options(Options) ->
    case validate_allowed_keys(subscribe, Options, [queue_group, owner]) of
        ok ->
            case {maps:get(owner, Options, self()), maps:get(queue_group, Options, undefined)} of
                {Owner, QueueGroup} when is_pid(Owner) ->
                    case QueueGroup of
                        undefined ->
                            ok;
                        _ ->
                            case valid_protocol_token(QueueGroup) of
                                true -> ok;
                                false -> {error, {invalid_option, queue_group}}
                            end
                    end;
                _ ->
                    {error, invalid_options}
            end;
        Error ->
            Error
    end.

validate_publish_options(Options, AllowMsgId) ->
    Allowed =
        case AllowMsgId of
            true -> [headers, reply_to, timeout, msg_id];
            false -> [headers, reply_to, timeout]
        end,
    case lists:all(fun(Key) -> lists:member(Key, Allowed) end, maps:keys(Options)) of
        false ->
            {error, invalid_options};
        true ->
            case validate_option_headers(maps:get(headers, Options, [])) of
                ok -> validate_option_reply_to(maps:get(reply_to, Options, undefined), Options);
                Error -> Error
            end
    end.

validate_option_headers(Headers) when is_list(Headers) ->
    enats_frame:validate_headers(Headers);
validate_option_headers(_Headers) ->
    {error, {invalid_option, headers}}.

validate_option_reply_to(undefined, Options) ->
    validate_option_timeout(timeout, maps:get(timeout, Options, 5000));
validate_option_reply_to(ReplyTo, Options) when is_binary(ReplyTo) ->
    case
        {
            valid_protocol_token(ReplyTo),
            validate_option_timeout(timeout, maps:get(timeout, Options, 5000))
        }
    of
        {true, ok} -> ok;
        {false, _} -> {error, {invalid_option, reply_to}};
        {_, Error} -> Error
    end;
validate_option_reply_to(_ReplyTo, _Options) ->
    {error, {invalid_option, reply_to}}.

valid_protocol_token(Value) when is_binary(Value), byte_size(Value) > 0 ->
    binary:match(Value, [<<" ">>, <<"\t">>, <<"\r">>, <<"\n">>, <<0>>, <<"*">>, <<">">>]) =:=
        nomatch;
valid_protocol_token(_Value) ->
    false.

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
    case payload_to_binary(Payload0) of
        {ok, Payload} -> Fun(Payload);
        {error, invalid_payload} = Error -> Error
    end.

payload_to_binary(Payload0) ->
    try iolist_to_binary(Payload0) of
        Payload -> {ok, Payload}
    catch
        error:badarg -> {error, invalid_payload}
    end.

jetstream_request(Client, Subject, Payload, Headers, Timeout) ->
    case
        request_with_metric(
            Client,
            Subject,
            Payload,
            #{headers => Headers, timeout => Timeout},
            jetstream_publish_latency
        )
    of
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

request_with_metric(Client, Subject, Payload0, Options, Metric) ->
    case validate_publish_options(Options, false) of
        ok ->
            with_subject(Subject, false, fun() ->
                with_payload(Payload0, fun(Payload) ->
                    enats_connection:request(
                        Client,
                        Subject,
                        Payload,
                        Options#{diagnostic_metric => Metric}
                    )
                end)
            end);
        Error ->
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
