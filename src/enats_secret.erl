-module(enats_secret).
-export([resolve/1, redact/1]).
-type provider(T) :: T | fun(() -> T | {ok, T} | {error, term()}).
-export_type([provider/1]).

resolve(Fun) when is_function(Fun, 0) ->
    try Fun() of
        {ok, Value} -> {ok, Value};
        {error, _} = Error -> Error;
        Value -> {ok, Value}
    catch Class:Reason -> {error, {secret_provider_failed, Class, Reason}}
    end;
resolve(Value) -> {ok, Value}.

redact(Map) when is_map(Map) ->
    maps:map(fun(Key, Value) ->
        case lists:member(Key, [password, pass, token, auth_token, sig, jwt, seed]) of
            true -> <<"******">>;
            false -> redact(Value)
        end
    end, Map);
redact(List) when is_list(List) -> [redact(Value) || Value <- List];
redact(Value) -> Value.
