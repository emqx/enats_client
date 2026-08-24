-module(enats_frame).
-export([initial_state/0, parse/2, serialize/1, serialize_connect/1]).

initial_state() -> #{buffer => <<>>, pending => undefined}.

parse(Data, #{buffer := Buffer, pending := Pending} = State) when is_binary(Data) ->
    parse_loop(<<Buffer/binary, Data/binary>>, State#{buffer => <<>>, pending => Pending}, []).

parse_loop(Bin, #{pending := {_Meta, Need}} = State, Acc) when byte_size(Bin) < Need + 2 ->
    {lists:reverse(Acc), State#{buffer => Bin}};
parse_loop(Bin, #{pending := {Meta, Need}} = State, Acc) ->
    <<Body:Need/binary, "\r\n", Rest/binary>> = Bin,
    parse_loop(Rest, State#{pending => undefined}, [complete_payload(Meta, Body) | Acc]);
parse_loop(Bin, State, Acc) ->
    case binary:match(Bin, <<"\r\n">>) of
        nomatch -> {lists:reverse(Acc), State#{buffer => Bin}};
        {Pos, 2} ->
            <<Line:Pos/binary, "\r\n", Rest/binary>> = Bin,
            case parse_line(Line) of
                {payload, Meta, Need} -> parse_loop(Rest, State#{pending => {Meta, Need}}, Acc);
                Frame -> parse_loop(Rest, State, [Frame | Acc])
            end
    end.

parse_line(<<"INFO ", Json/binary>>) -> {info, jsx:decode(Json, [return_maps])};
parse_line(<<"-ERR ", Error/binary>>) -> {error, Error};
parse_line(<<"+OK">>) -> ok;
parse_line(<<"PING">>) -> ping;
parse_line(<<"PONG">>) -> pong;
parse_line(<<"MSG ", Args/binary>>) -> parse_message(binary:split(Args, <<" ">>, [global]), msg);
parse_line(<<"HMSG ", Args/binary>>) -> parse_message(binary:split(Args, <<" ">>, [global]), hmsg);
parse_line(Line) -> {error, {unknown_frame, Line}}.

parse_message([Subject, Sid, Size], msg) -> {payload, #{type => msg, subject => Subject, sid => Sid}, to_int(Size)};
parse_message([Subject, Sid, ReplyTo, Size], msg) ->
    {payload, #{type => msg, subject => Subject, sid => Sid, reply_to => ReplyTo}, to_int(Size)};
parse_message([Subject, Sid, HeaderSize, TotalSize], hmsg) ->
    {payload, #{type => hmsg, subject => Subject, sid => Sid, header_size => to_int(HeaderSize)}, to_int(TotalSize)};
parse_message([Subject, Sid, ReplyTo, HeaderSize, TotalSize], hmsg) ->
    {payload, #{type => hmsg, subject => Subject, sid => Sid, reply_to => ReplyTo,
        header_size => to_int(HeaderSize)}, to_int(TotalSize)}.

complete_payload(#{type := msg} = Meta, Body) -> Meta#{payload => Body};
complete_payload(#{type := hmsg, header_size := HeaderSize} = Meta, Body) ->
    <<Header:HeaderSize/binary, Payload/binary>> = Body,
    Meta#{headers => decode_headers(Header), payload => Payload}.

decode_headers(<<"NATS/1.0\r\n", Rest/binary>>) ->
    Lines = binary:split(Rest, <<"\r\n">>, [global]),
    [{Key, Value} || Line <- Lines, Line =/= <<>>, {Key, Value} <- [split_header(Line)]];
decode_headers(_) -> [].

split_header(Line) ->
    case binary:split(Line, <<": ">>) of
        [Key, Value] -> {Key, Value};
        [Key] -> {Key, <<>>}
    end.

to_int(Bin) -> binary_to_integer(Bin).

serialize_connect(Params) -> [<<"CONNECT ">>, jsx:encode(stringify_keys(Params)), <<"\r\n">>].
serialize(ping) -> <<"PING\r\n">>;
serialize(pong) -> <<"PONG\r\n">>;
serialize({pub, Subject, Payload}) -> pub(Subject, undefined, Payload);
serialize({pub, Subject, ReplyTo, Payload}) -> pub(Subject, ReplyTo, Payload);
serialize({hpub, Subject, ReplyTo, Headers, Payload}) ->
    HeaderBlock = encode_headers(Headers),
    HSize = byte_size(HeaderBlock), TSize = HSize + byte_size(Payload),
    [<<"HPUB ">>, Subject, optional_reply(ReplyTo), integer_to_binary(HSize), <<" ">>,
        integer_to_binary(TSize), <<"\r\n">>, HeaderBlock, Payload, <<"\r\n">>];
serialize({sub, Subject, Sid, undefined}) -> [<<"SUB ">>, Subject, <<" ">>, Sid, <<"\r\n">>];
serialize({sub, Subject, Sid, Queue}) -> [<<"SUB ">>, Subject, <<" ">>, Queue, <<" ">>, Sid, <<"\r\n">>];
serialize({unsub, Sid}) -> [<<"UNSUB ">>, Sid, <<"\r\n">>].

pub(Subject, undefined, Payload) -> [<<"PUB ">>, Subject, <<" ">>, integer_to_binary(byte_size(Payload)), <<"\r\n">>, Payload, <<"\r\n">>];
pub(Subject, ReplyTo, Payload) -> [<<"PUB ">>, Subject, <<" ">>, ReplyTo, <<" ">>, integer_to_binary(byte_size(Payload)), <<"\r\n">>, Payload, <<"\r\n">>].
optional_reply(undefined) -> <<" ">>;
optional_reply(ReplyTo) -> [<<" ">>, ReplyTo, <<" ">>].
encode_headers(Headers) ->
    Lines = [[Key, <<": ">>, Value, <<"\r\n">>] || {Key, Value} <- Headers],
    iolist_to_binary([<<"NATS/1.0\r\n">>, Lines, <<"\r\n">>]).
stringify_keys(Map) -> maps:from_list([{key_binary(Key), Value} || {Key, Value} <- maps:to_list(Map)]).
key_binary(Key) when is_atom(Key) -> atom_to_binary(Key, utf8);
key_binary(Key) -> Key.
