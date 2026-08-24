-module(enats_auth).
-export([connect_params/3, describe/1]).

-type auth() :: none
    | #{mechanism := user_password, username := binary(), password := enats_secret:provider(binary())}
    | #{mechanism := token, token := enats_secret:provider(binary())}
    | #{mechanism := nkey, public_key := binary(), sign_fun := fun((binary()) -> binary())}.
-export_type([auth/0]).

connect_params(none, _Info, Base) -> {ok, Base};
connect_params(#{mechanism := user_password, username := Username, password := Password}, _Info, Base) ->
    with_secret(Password, fun(Value) -> {ok, Base#{user => Username, pass => Value}} end);
connect_params(#{mechanism := token, token := Token}, _Info, Base) ->
    with_secret(Token, fun(Value) -> {ok, Base#{auth_token => Value}} end);
connect_params(#{mechanism := nkey, public_key := PublicKey, sign_fun := SignFun}, Info, Base) ->
    case maps:get(nonce, Info, undefined) of
        undefined -> {error, nkey_nonce_missing};
        Nonce ->
            try SignFun(Nonce) of
                Signature when is_binary(Signature) -> {ok, Base#{nkey => PublicKey, sig => Signature}};
                {ok, Signature} when is_binary(Signature) -> {ok, Base#{nkey => PublicKey, sig => Signature}};
                {error, Reason} -> {error, {nkey_sign_failed, Reason}};
                Other -> {error, {invalid_nkey_signature, Other}}
            catch Class:Reason -> {error, {nkey_sign_failed, Class, Reason}}
            end
    end.

describe(none) -> none;
describe(#{mechanism := Mechanism}) -> Mechanism.

with_secret(Provider, Fun) ->
    case enats_secret:resolve(Provider) of
        {ok, Value} when is_binary(Value) -> Fun(Value);
        {ok, _} -> {error, invalid_secret_type};
        {error, Reason} -> {error, Reason}
    end.
