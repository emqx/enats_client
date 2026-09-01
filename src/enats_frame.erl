-module(enats_frame).
-moduledoc "Internal NATS protocol frame encoder and incremental parser.".
-export([
    initial_state/0,
    initial_state/1,
    parse/2,
    serialize/1,
    serialize_connect/1,
    validate_headers/1,
    headers_size/1
]).

-type header() :: {binary(), binary()}.
-type frame() ::
    {info, #{binary() => binary() | boolean() | integer() | [binary()]}}
    | ping
    | pong
    | ok
    | {error, binary() | {unknown_frame, binary()}}
    | {payload, #{atom() => atom() | binary() | integer()}, non_neg_integer()}
    | #{
        type := msg | hmsg,
        subject := binary(),
        sid := binary(),
        payload := binary(),
        headers => [header()],
        reply_to => binary()
    }.
-type parse_state() :: #{
    buffer := binary(),
    pending :=
        undefined
        | {#{atom() => binary()}, non_neg_integer()},
    limits := limits()
}.
-type limits() :: #{
    max_control_line := pos_integer(),
    max_message_size := pos_integer(),
    max_parser_buffer := pos_integer()
}.
-type limit_options() :: #{
    max_control_line => pos_integer(),
    max_message_size => pos_integer(),
    max_parser_buffer => pos_integer()
}.
-type wire_frame() ::
    ping
    | pong
    | {pub, binary(), binary()}
    | {pub, binary(), binary(), binary()}
    | {hpub, binary(), undefined | binary(), [header()], binary()}
    | {sub, binary(), binary(), undefined | binary()}
    | {unsub, binary()}.
-type header_error_value() ::
    atom()
    | binary()
    | integer()
    | boolean()
    | [header_error_value()]
    | {header_error_value(), header_error_value()}
    | {header_error_value(), header_error_value(), header_error_value()}.
-type header_error() ::
    {invalid_headers, header_error_value()}
    | {invalid_header_name, binary()}
    | {invalid_header_value, binary()}
    | {invalid_header, header_error_value()}.
-export_type([header/0, frame/0, parse_state/0, limits/0, wire_frame/0]).

-spec initial_state() -> parse_state().
initial_state() -> initial_state(#{}).

-spec initial_state(limit_options()) -> parse_state().
initial_state(Options) ->
    #{
        buffer => <<>>,
        pending => undefined,
        limits => maps:merge(
            #{
                max_control_line => 4096,
                max_message_size => 8 * 1024 * 1024,
                max_parser_buffer => 8 * 1024 * 1024
            },
            Options
        )
    }.

-spec parse(binary(), parse_state()) -> {[frame()], parse_state()}.
parse(Data, #{buffer := Buffer, pending := Pending} = State) when is_binary(Data) ->
    parse_loop(<<Buffer/binary, Data/binary>>, State#{buffer => <<>>, pending => Pending}, []).

parse_loop(
    _Bin, #{pending := {_Meta, Need}, limits := #{max_message_size := MaxMessage}}, _Acc
) when
    Need > MaxMessage
->
    error(payload_too_large);
parse_loop(
    Bin,
    #{pending := {_Meta, Need}, limits := #{max_parser_buffer := MaxBuffer}},
    _Acc
) when byte_size(Bin) < Need + 2, byte_size(Bin) > MaxBuffer ->
    error(parser_buffer_limit);
parse_loop(Bin, #{pending := {_Meta, Need}} = State, Acc) when byte_size(Bin) < Need + 2 ->
    {lists:reverse(Acc), State#{buffer => Bin}};
parse_loop(Bin, #{pending := {Meta, Need}} = State, Acc) ->
    <<Body:Need/binary, "\r\n", Rest/binary>> = Bin,
    parse_loop(Rest, State#{pending => undefined}, [complete_payload(Meta, Body) | Acc]);
parse_loop(
    Bin,
    #{limits := #{max_control_line := MaxLine, max_parser_buffer := MaxBuffer}} = State,
    Acc
) ->
    case binary:match(Bin, <<"\r\n">>) of
        nomatch when byte_size(Bin) > MaxBuffer ->
            error(parser_buffer_limit);
        nomatch when byte_size(Bin) > MaxLine ->
            error(control_line_too_long);
        nomatch ->
            {lists:reverse(Acc), State#{buffer => Bin}};
        {Pos, 2} ->
            <<Line:Pos/binary, "\r\n", Rest/binary>> = Bin,
            case parse_line(Line) of
                {payload, Meta, Need} -> parse_loop(Rest, State#{pending => {Meta, Need}}, Acc);
                Frame -> parse_loop(Rest, State, [Frame | Acc])
            end
    end.

parse_line(<<"INFO ", Json/binary>>) -> {info, jiffy:decode(Json, [return_maps])};
parse_line(<<"-ERR ", Error/binary>>) -> {error, Error};
parse_line(<<"+OK">>) -> ok;
parse_line(<<"PING">>) -> ping;
parse_line(<<"PONG">>) -> pong;
parse_line(<<"MSG ", Args/binary>>) -> parse_message(binary:split(Args, <<" ">>, [global]), msg);
parse_line(<<"HMSG ", Args/binary>>) -> parse_message(binary:split(Args, <<" ">>, [global]), hmsg);
parse_line(Line) -> {error, {unknown_frame, Line}}.

parse_message([Subject, Sid, Size], msg) ->
    {payload, #{type => msg, subject => Subject, sid => Sid}, to_int(Size)};
parse_message([Subject, Sid, ReplyTo, Size], msg) ->
    {payload, #{type => msg, subject => Subject, sid => Sid, reply_to => ReplyTo}, to_int(Size)};
parse_message([Subject, Sid, HeaderSize, TotalSize], hmsg) ->
    HeaderSizeValue = to_int(HeaderSize),
    TotalSizeValue = to_int(TotalSize),
    true = HeaderSizeValue =< TotalSizeValue,
    {payload, #{type => hmsg, subject => Subject, sid => Sid, header_size => HeaderSizeValue},
        TotalSizeValue};
parse_message([Subject, Sid, ReplyTo, HeaderSize, TotalSize], hmsg) ->
    HeaderSizeValue = to_int(HeaderSize),
    TotalSizeValue = to_int(TotalSize),
    true = HeaderSizeValue =< TotalSizeValue,
    {payload,
        #{
            type => hmsg,
            subject => Subject,
            sid => Sid,
            reply_to => ReplyTo,
            header_size => HeaderSizeValue
        },
        TotalSizeValue}.

complete_payload(#{type := msg} = Meta, Body) ->
    Meta#{payload => Body};
complete_payload(#{type := hmsg, header_size := HeaderSize} = Meta, Body) ->
    <<Header:HeaderSize/binary, Payload/binary>> = Body,
    Meta#{headers => decode_headers(Header), payload => Payload}.

decode_headers(Header) ->
    case binary:split(Header, <<"\r\n">>, [global]) of
        [VersionLine | Lines] ->
            case status_headers(VersionLine) of
                invalid ->
                    [];
                StatusHeaders ->
                    StatusHeaders ++
                        [
                            ParsedHeader
                         || Line <- Lines,
                            Line =/= <<>>,
                            {ok, ParsedHeader} <- [split_header(Line)]
                        ]
            end;
        _ ->
            []
    end.

status_headers(<<"NATS/1.0">>) ->
    [];
status_headers(<<"NATS/1.0 ", Status/binary>>) ->
    case binary:split(Status, <<" ">>, [global]) of
        [Code] -> [{<<"Status">>, Code}];
        [Code | Description] -> [{<<"Status">>, Code}, {<<"Description">>, join_words(Description)}]
    end;
status_headers(_) ->
    invalid.

join_words(Words) -> iolist_to_binary(lists:join(<<" ">>, Words)).

split_header(Line) ->
    case binary:split(Line, <<":">>) of
        [Key, Value] -> {ok, {Key, trim_leading_ows(Value)}};
        [_] -> error
    end.

trim_leading_ows(<<Char, Rest/binary>>) when Char =:= $\s; Char =:= $\t ->
    trim_leading_ows(Rest);
trim_leading_ows(Value) ->
    Value.

to_int(Bin) ->
    Value = binary_to_integer(Bin),
    true = Value >= 0,
    Value.

-spec serialize_connect(#{atom() | binary() => binary() | boolean() | integer() | [binary()]}) ->
    iodata().
serialize_connect(Params) -> [<<"CONNECT ">>, jiffy:encode(stringify_keys(Params)), <<"\r\n">>].
-spec serialize(wire_frame()) -> iodata().
serialize(ping) ->
    <<"PING\r\n">>;
serialize(pong) ->
    <<"PONG\r\n">>;
serialize({pub, Subject, Payload}) ->
    pub(Subject, undefined, Payload);
serialize({pub, Subject, ReplyTo, Payload}) ->
    pub(Subject, ReplyTo, Payload);
serialize({hpub, Subject, ReplyTo, Headers, Payload}) ->
    HeaderBlock = encode_headers(Headers),
    HSize = byte_size(HeaderBlock),
    TSize = HSize + byte_size(Payload),
    [
        <<"HPUB ">>,
        Subject,
        optional_reply(ReplyTo),
        integer_to_binary(HSize),
        <<" ">>,
        integer_to_binary(TSize),
        <<"\r\n">>,
        HeaderBlock,
        Payload,
        <<"\r\n">>
    ];
serialize({sub, Subject, Sid, undefined}) ->
    [<<"SUB ">>, Subject, <<" ">>, Sid, <<"\r\n">>];
serialize({sub, Subject, Sid, Queue}) ->
    [<<"SUB ">>, Subject, <<" ">>, Queue, <<" ">>, Sid, <<"\r\n">>];
serialize({unsub, Sid}) ->
    [<<"UNSUB ">>, Sid, <<"\r\n">>].

pub(Subject, undefined, Payload) ->
    [
        <<"PUB ">>,
        Subject,
        <<" ">>,
        integer_to_binary(byte_size(Payload)),
        <<"\r\n">>,
        Payload,
        <<"\r\n">>
    ];
pub(Subject, ReplyTo, Payload) ->
    [
        <<"PUB ">>,
        Subject,
        <<" ">>,
        ReplyTo,
        <<" ">>,
        integer_to_binary(byte_size(Payload)),
        <<"\r\n">>,
        Payload,
        <<"\r\n">>
    ].
optional_reply(undefined) -> <<" ">>;
optional_reply(ReplyTo) -> [<<" ">>, ReplyTo, <<" ">>].
encode_headers(Headers) ->
    Lines = [[Key, <<": ">>, Value, <<"\r\n">>] || {Key, Value} <- Headers],
    iolist_to_binary([<<"NATS/1.0\r\n">>, Lines, <<"\r\n">>]).

-spec headers_size([header()]) -> non_neg_integer().
headers_size([]) -> 0;
headers_size(Headers) -> byte_size(encode_headers(Headers)).

-spec validate_headers([header()]) -> ok | {error, header_error()}.
validate_headers(Headers) when is_list(Headers) ->
    validate_headers(Headers, ok);
validate_headers(Headers) ->
    {error, {invalid_headers, Headers}}.

validate_headers([], Result) ->
    Result;
validate_headers([{Key, Value} | Rest], ok) when is_binary(Key), is_binary(Value) ->
    case {valid_header_name(Key), binary:match(Value, [<<"\r">>, <<"\n">>])} of
        {true, nomatch} -> validate_headers(Rest, ok);
        {false, _} -> {error, {invalid_header_name, Key}};
        {_, _} -> {error, {invalid_header_value, Key}}
    end;
validate_headers([Header | _], _Result) ->
    {error, {invalid_header, Header}}.

valid_header_name(<<>>) ->
    false;
valid_header_name(<<Char, Rest/binary>>) when Char >= 33, Char =< 126, Char =/= $: ->
    valid_header_name_tail(Rest);
valid_header_name(_) ->
    false.

valid_header_name_tail(<<>>) ->
    true;
valid_header_name_tail(<<Char, Rest/binary>>) when Char >= 33, Char =< 126, Char =/= $: ->
    valid_header_name_tail(Rest);
valid_header_name_tail(_) ->
    false.
stringify_keys(Map) ->
    maps:from_list([{key_binary(Key), Value} || {Key, Value} <- maps:to_list(Map)]).
key_binary(Key) when is_atom(Key) -> atom_to_binary(Key, utf8);
key_binary(Key) -> Key.
