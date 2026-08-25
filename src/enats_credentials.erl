-module(enats_credentials).

-export([from_binary/1, from_file/1]).

from_file(Filename) ->
    case file:read_file(Filename) of
        {ok, Contents} -> from_binary(Contents);
        {error, Reason} -> {error, {credentials_file, Reason}}
    end.

from_binary(Contents) when is_binary(Contents) ->
    try
        JWT = extract(Contents, <<"-----BEGIN NATS USER JWT-----">>, <<"------END NATS USER JWT------">>),
        Seed = extract(Contents, <<"-----BEGIN USER NKEY SEED-----">>, <<"------END USER NKEY SEED------">>),
        case enats_nkey:from_seed(Seed) of
            {ok, PublicKey, SignFun} ->
                {ok, #{mechanism => jwt, public_key => PublicKey,
                    jwt => fun() -> JWT end, sign_fun => SignFun}};
            {error, Reason} -> {error, Reason}
        end
    catch
        error:InvalidReason -> {error, {invalid_credentials, InvalidReason}}
    end.

extract(Contents, Begin, End) ->
    case binary:split(Contents, Begin) of
        [_, Rest] ->
            case binary:split(Rest, End) of
                [Value, _] -> trim(Value);
                _ -> error({missing_marker, End})
            end;
        _ -> error({missing_marker, Begin})
    end.

trim(Value) ->
    iolist_to_binary(string:trim(binary_to_list(Value))).
