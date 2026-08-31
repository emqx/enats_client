-module(enats_credentials).

-export([from_binary/1, from_file/1, validate_file/1, validate/1, connect_params/3]).

from_file(Filename) ->
    {ok, #{mechanism => credentials, provider => fun() -> file:read_file(Filename) end}}.

validate_file(Filename) ->
    case file:read_file(Filename) of
        {ok, Contents} -> validate(Contents);
        {error, Reason} -> {error, {credentials_file, Reason}}
    end.

from_binary(Contents) when is_binary(Contents) ->
    case validate(Contents) of
        ok ->
            {ok, #{mechanism => credentials, provider => fun() -> {ok, Contents} end}};
        {error, _} = Error ->
            Error
    end.

validate(Contents) when is_binary(Contents) ->
    try
        _JWT = extract(Contents, <<"-----BEGIN NATS USER JWT-----">>, <<"------END NATS USER JWT------">>),
        Seed = extract(Contents, <<"-----BEGIN USER NKEY SEED-----">>, <<"------END USER NKEY SEED------">>),
        case enats_nkey:from_seed(Seed) of
            {ok, _PublicKey, _SignFun} -> ok;
            {error, Reason} -> {error, Reason}
        end
    catch
        error:InvalidReason -> {error, {invalid_credentials, InvalidReason}}
    end;
validate(Contents) ->
    {error, {invalid_credentials_type, Contents}}.

connect_params(Provider, Info, Base) ->
    case enats_secret:resolve(Provider) of
        {ok, Contents} when is_binary(Contents) ->
            connect_params_from_binary(Contents, Info, Base);
        {ok, _} ->
            {error, invalid_credentials_type};
        {error, Reason} ->
            {error, Reason}
    end.

connect_params_from_binary(Contents, Info, Base) ->
    try
        JWT = extract(Contents, <<"-----BEGIN NATS USER JWT-----">>, <<"------END NATS USER JWT------">>),
        Seed = extract(Contents, <<"-----BEGIN USER NKEY SEED-----">>, <<"------END USER NKEY SEED------">>),
        case maps:get(nonce, Info, undefined) of
            undefined ->
                {error, nkey_nonce_missing};
            Nonce ->
                case enats_nkey:sign_seed(Seed, Nonce) of
                    {ok, PublicKey, Signature} ->
                        {ok, Base#{nkey => PublicKey, sig => Signature, jwt => JWT}};
                    {error, Reason} ->
                        {error, Reason}
                end
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
