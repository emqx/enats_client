-module(enats_js).

-export([publish/4]).

publish(Client, Subject, Payload, Options) ->
    Timeout = maps:get(timeout, Options, 5000),
    Headers = maybe_msg_id_header(maps:get(msg_id, Options, undefined),
        maps:get(headers, Options, [])),
    case enats_client:request(Client, Subject, Payload, #{headers => Headers, timeout => Timeout}) of
        {ok, #{payload := Response} = Message} ->
            case response_status(maps:get(headers, Message, [])) of
                undefined -> decode_pub_ack(Response);
                Status -> {error, {jetstream_unavailable, Status}}
            end;
        {error, _} = Error ->
            Error
    end.

response_status(Headers) ->
    case lists:keyfind(<<"Status">>, 1, Headers) of
        {_, Status} -> Status;
        false -> undefined
    end.

maybe_msg_id_header(undefined, Headers) -> Headers;
maybe_msg_id_header(MsgId, Headers) when is_binary(MsgId) ->
    [{<<"Nats-Msg-Id">>, MsgId} | Headers].

decode_pub_ack(Payload) ->
    try jsx:decode(Payload, [return_maps]) of
        #{<<"error">> := Error} ->
            {error, {jetstream_error, Error}};
        #{<<"stream">> := Stream, <<"seq">> := Sequence} = Ack ->
            {ok, #{stream => Stream, sequence => Sequence,
                duplicate => maps:get(<<"duplicate">>, Ack, false)}};
        Other ->
            {error, {invalid_jetstream_ack, Other}}
    catch
        Class:Reason -> {error, {invalid_jetstream_ack, Class, Reason}}
    end.
