-module(enats_auth).
-export([connect_params/3, describe/1]).

-type auth() :: none
    | #{mechanism := user_password, username := binary(), password := enats_secret:provider(binary())}
    | #{mechanism := token, token := enats_secret:provider(binary())}
    | #{mechanism := nkey, public_key := binary(), sign_fun := fun((binary()) -> binary())}
    | #{mechanism := jwt, jwt := enats_secret:provider(binary()), public_key := binary(),
        sign_fun := fun((binary()) -> binary())}.
-export_type([auth/0]).

connect_params(none, _Info, Base) -> {ok, Base};
connect_params(#{mechanism := user_password, username := Username, password := Password}, _Info, Base) ->
    with_secret(Password, fun(Value) -> {ok, Base#{user => Username, pass => Value}} end);
connect_params(#{mechanism := token, token := Token}, _Info, Base) ->
    with_secret(Token, fun(Value) -> {ok, Base#{auth_token => Value}} end);
connect_params(#{mechanism := nkey, public_key := PublicKey, sign_fun := SignFun}, Info, Base) ->
    signed_connect_params(nkey, undefined, PublicKey, SignFun, Info, Base);
connect_params(#{mechanism := jwt, jwt := JWT, public_key := PublicKey, sign_fun := SignFun}, Info, Base) ->
    case signed_connect_params(jwt, JWT, PublicKey, SignFun, Info, Base) of
        {ok, Params} -> with_secret(JWT, fun(Value) -> {ok, Params#{jwt => Value}} end);
        Error -> Error
    end.

signed_connect_params(Mechanism, JWT, PublicKey, SignFun, Info, Base) ->
    case maps:get(nonce, Info, undefined) of
        undefined -> {error, nkey_nonce_missing};
        Nonce ->
            try SignFun(Nonce) of
                Signature when is_binary(Signature) -> signed_params(Mechanism, JWT, PublicKey, Signature, Base);
                {ok, Signature} when is_binary(Signature) -> signed_params(Mechanism, JWT, PublicKey, Signature, Base);
                {error, Reason} -> {error, {nkey_sign_failed, Reason}};
                Other -> {error, {invalid_nkey_signature, Other}}
            catch Class:Reason -> {error, {nkey_sign_failed, Class, Reason}}
            end
    end.

signed_params(nkey, _JWT, PublicKey, Signature, Base) ->
    {ok, Base#{nkey => PublicKey, sig => Signature}};
signed_params(jwt, _JWT, PublicKey, Signature, Base) ->
    {ok, Base#{nkey => PublicKey, sig => Signature}}.

describe(none) -> none;
describe(#{mechanism := Mechanism}) -> Mechanism.

with_secret(Provider, Fun) ->
    case enats_secret:resolve(Provider) of
        {ok, Value} when is_binary(Value) -> Fun(Value);
        {ok, _} -> {error, invalid_secret_type};
        {error, Reason} -> {error, Reason}
    end.
