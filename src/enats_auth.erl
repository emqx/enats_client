-module(enats_auth).
-moduledoc "Authentication, credentials and NKey helpers used by enats_client.".

-export([
    connect_params/3,
    validate/1,
    describe/1,
    credentials_file/1,
    credentials/1,
    validate_credentials/1,
    validate_credentials_file/1,
    encode_nkey_public/1,
    nkey_signer/2,
    from_seed/1,
    sign_seed/2,
    resolve_secret/1,
    redact/1
]).

-type secret_provider(T) :: T | fun(() -> T | {ok, T} | {error, auth_error()}).
-type auth() ::
    none
    | #{mechanism := user_password, username := binary(), password := secret_provider(binary())}
    | #{mechanism := token, token := secret_provider(binary())}
    | #{mechanism := nkey_seed, seed := secret_provider(binary())}
    | #{mechanism := nkey, public_key := binary(), sign_fun := fun((binary()) -> binary())}
    | #{mechanism := credentials, provider := secret_provider(binary())}
    | #{
        mechanism := jwt,
        jwt := secret_provider(binary()),
        public_key := binary(),
        sign_fun := fun((binary()) -> binary())
    }.
-type auth_error() ::
    invalid_secret_type
    | invalid_credentials_type
    | invalid_credentials
    | invalid_nkey_seed
    | nkey_nonce_missing
    | secret_provider_failed
    | {credentials_file, file:posix()}
    | {nkey_sign_failed, auth_error()}
    | {invalid_nkey_signature, binary()}.
-type json_value() ::
    null
    | boolean()
    | integer()
    | float()
    | binary()
    | [json_value()]
    | #{binary() => json_value()}.
-type connect_map() :: #{atom() | binary() => json_value()}.
-type redactable() ::
    binary()
    | atom()
    | integer()
    | float()
    | boolean()
    | undefined
    | [redactable()]
    | #{atom() | binary() => redactable()}.
-export_type([auth/0, secret_provider/1, auth_error/0, redactable/0]).

-spec connect_params(auth(), connect_map(), connect_map()) ->
    {ok, connect_map()} | {error, auth_error()}.
connect_params(none, _Info, Base) ->
    {ok, Base};
connect_params(
    #{mechanism := user_password, username := Username, password := Password}, _Info, Base
) ->
    with_secret(Password, fun(Value) -> {ok, Base#{user => Username, pass => Value}} end);
connect_params(#{mechanism := token, token := Token}, _Info, Base) ->
    with_secret(Token, fun(Value) -> {ok, Base#{auth_token => Value}} end);
connect_params(#{mechanism := nkey_seed, seed := Seed}, Info, Base) ->
    case maps:get(nonce, Info, undefined) of
        undefined ->
            {error, nkey_nonce_missing};
        Nonce ->
            with_secret(Seed, fun(Value) ->
                case sign_seed(Value, Nonce) of
                    {ok, PublicKey, Signature} ->
                        {ok, Base#{nkey => PublicKey, sig => Signature}};
                    {error, Reason} ->
                        {error, Reason}
                end
            end)
    end;
connect_params(#{mechanism := credentials, provider := Provider}, Info, Base) ->
    with_secret(Provider, fun(Contents) -> credentials_connect_params(Contents, Info, Base) end);
connect_params(#{mechanism := nkey, public_key := PublicKey, sign_fun := SignFun}, Info, Base) ->
    signed_connect_params(PublicKey, SignFun, Info, Base);
connect_params(
    #{mechanism := jwt, jwt := JWT, public_key := PublicKey, sign_fun := SignFun}, Info, Base
) ->
    case signed_connect_params(PublicKey, SignFun, Info, Base) of
        {ok, Params} -> with_secret(JWT, fun(Value) -> {ok, Params#{jwt => Value}} end);
        Error -> Error
    end.

-spec validate(auth()) -> ok | {error, auth_error()}.
validate(none) ->
    ok;
validate(#{mechanism := user_password, username := Username, password := Password}) when
    is_binary(Username), (is_binary(Password) orelse is_function(Password, 0))
->
    ok;
validate(#{mechanism := token, token := Token}) when
    is_binary(Token); is_function(Token, 0)
->
    ok;
validate(#{mechanism := nkey_seed, seed := Seed}) when
    is_binary(Seed); is_function(Seed, 0)
->
    ok;
validate(#{mechanism := nkey, public_key := PublicKey, sign_fun := SignFun}) when
    is_binary(PublicKey), is_function(SignFun, 1)
->
    ok;
validate(#{mechanism := credentials, provider := Provider}) when
    is_binary(Provider); is_function(Provider, 0)
->
    ok;
validate(#{mechanism := jwt, jwt := JWT, public_key := PublicKey, sign_fun := SignFun}) when
    (is_binary(JWT) orelse is_function(JWT, 0)), is_binary(PublicKey), is_function(SignFun, 1)
->
    ok;
validate(_Auth) ->
    {error, invalid_credentials}.

-spec describe(auth()) -> atom().
describe(none) -> none;
describe(#{mechanism := Mechanism}) -> Mechanism.

-spec credentials_file(binary() | string()) -> {ok, auth()} | {error, auth_error()}.
credentials_file(Filename) when is_binary(Filename); is_list(Filename) ->
    case validate_credentials_file(Filename) of
        ok -> {ok, #{mechanism => credentials, provider => fun() -> file:read_file(Filename) end}};
        {error, Reason} -> {error, Reason}
    end.

-spec credentials(binary()) -> {ok, auth()} | {error, auth_error()}.
credentials(Contents) when is_binary(Contents) ->
    case validate_credentials(Contents) of
        ok -> {ok, #{mechanism => credentials, provider => fun() -> {ok, Contents} end}};
        {error, Reason} -> {error, Reason}
    end.

-spec validate_credentials(binary()) -> ok | {error, auth_error()}.
validate_credentials(Contents) when is_binary(Contents) ->
    try
        _JWT = extract(
            Contents, <<"-----BEGIN NATS USER JWT-----">>, <<"------END NATS USER JWT------">>
        ),
        Seed = extract(
            Contents, <<"-----BEGIN USER NKEY SEED-----">>, <<"------END USER NKEY SEED------">>
        ),
        case from_seed(Seed) of
            {ok, _PublicKey, _Signer} -> ok;
            {error, Reason} -> {error, Reason}
        end
    catch
        error:_ -> {error, invalid_credentials}
    end;
validate_credentials(_Contents) ->
    {error, invalid_credentials_type}.

-spec validate_credentials_file(binary() | string()) -> ok | {error, auth_error()}.
validate_credentials_file(Filename) when is_binary(Filename); is_list(Filename) ->
    case file:read_file(Filename) of
        {ok, Contents} -> validate_credentials(Contents);
        {error, Reason} -> {error, {credentials_file, Reason}}
    end.

-spec encode_nkey_public(binary()) -> binary().
encode_nkey_public(PublicKey) when is_binary(PublicKey), byte_size(PublicKey) =:= 32 ->
    Payload = <<16#A0, PublicKey/binary>>,
    Raw = <<Payload/binary, (crc16(Payload)):16/little-unsigned>>,
    list_to_binary(base32(Raw, [])).

-spec nkey_signer(binary(), binary()) -> fun((binary()) -> binary()).
nkey_signer(_PublicKey, PrivateKey) when is_binary(PrivateKey), byte_size(PrivateKey) =:= 32 ->
    fun(Nonce) ->
        Signature = crypto:sign(eddsa, none, Nonce, [PrivateKey, ed25519]),
        base64url(Signature)
    end.

-spec from_seed(binary()) -> {ok, binary(), fun((binary()) -> binary())} | {error, auth_error()}.
from_seed(Seed0) when is_binary(Seed0) ->
    try
        {_UserPrefix, Seed} = decode_seed(Seed0),
        {PublicKey, _Private} = crypto:generate_key(eddsa, ed25519, Seed),
        Public = encode_nkey_public(PublicKey),
        {ok, Public, nkey_signer(Public, Seed)}
    catch
        _:_ -> {error, invalid_nkey_seed}
    end.

-spec sign_seed(binary(), binary()) -> {ok, binary(), binary()} | {error, auth_error()}.
sign_seed(Seed0, Nonce) when is_binary(Seed0), is_binary(Nonce) ->
    try
        {_UserPrefix, Seed} = decode_seed(Seed0),
        {PublicKey, _Private} = crypto:generate_key(eddsa, ed25519, Seed),
        Public = encode_nkey_public(PublicKey),
        Signature = crypto:sign(eddsa, none, Nonce, [Seed, ed25519]),
        {ok, Public, base64url(Signature)}
    catch
        _:_ -> {error, invalid_nkey_seed}
    end.

-spec resolve_secret(secret_provider(binary())) -> {ok, binary()} | {error, auth_error()}.
resolve_secret(Fun) when is_function(Fun, 0) ->
    try Fun() of
        {ok, Value} when is_binary(Value) -> {ok, Value};
        {error, _} -> {error, secret_provider_failed};
        Value when is_binary(Value) -> {ok, Value};
        _ -> {error, invalid_secret_type}
    catch
        _:_ -> {error, secret_provider_failed}
    end;
resolve_secret(Value) when is_binary(Value) ->
    {ok, Value};
resolve_secret(_Value) ->
    {error, invalid_secret_type}.

-spec redact(redactable()) -> redactable().
redact(Map) when is_map(Map) ->
    maps:map(
        fun(Key, Value) ->
            case
                lists:member(Key, [
                    password,
                    pass,
                    token,
                    auth_token,
                    sig,
                    jwt,
                    seed,
                    <<"password">>,
                    <<"pass">>,
                    <<"token">>,
                    <<"auth_token">>,
                    <<"sig">>,
                    <<"jwt">>,
                    <<"seed">>
                ])
            of
                true -> <<"******">>;
                false -> redact(Value)
            end
        end,
        Map
    );
redact(List) when is_list(List) ->
    [redact(Value) || Value <- List];
redact(Value) ->
    Value.

with_secret(Provider, Fun) ->
    case resolve_secret(Provider) of
        {ok, Value} -> Fun(Value);
        {error, Reason} -> {error, Reason}
    end.

signed_connect_params(PublicKey, SignFun, Info, Base) ->
    case maps:get(nonce, Info, undefined) of
        undefined ->
            {error, nkey_nonce_missing};
        Nonce ->
            try SignFun(Nonce) of
                Signature when is_binary(Signature) ->
                    {ok, Base#{nkey => PublicKey, sig => Signature}};
                {ok, Signature} when is_binary(Signature) ->
                    {ok, Base#{nkey => PublicKey, sig => Signature}};
                _ ->
                    {error, {invalid_nkey_signature, <<"invalid">>}}
            catch
                _:_ -> {error, {nkey_sign_failed, invalid_nkey_seed}}
            end
    end.

credentials_connect_params(Contents, Info, Base) ->
    try
        JWT = extract(
            Contents, <<"-----BEGIN NATS USER JWT-----">>, <<"------END NATS USER JWT------">>
        ),
        Seed = extract(
            Contents, <<"-----BEGIN USER NKEY SEED-----">>, <<"------END USER NKEY SEED------">>
        ),
        case maps:get(nonce, Info, undefined) of
            undefined ->
                {error, nkey_nonce_missing};
            Nonce ->
                case sign_seed(Seed, Nonce) of
                    {ok, PublicKey, Signature} ->
                        {ok, Base#{nkey => PublicKey, sig => Signature, jwt => JWT}};
                    {error, Reason} ->
                        {error, Reason}
                end
        end
    catch
        error:_ -> {error, invalid_credentials}
    end.

extract(Contents, Begin, End) ->
    case binary:split(Contents, Begin) of
        [_, Rest] ->
            case binary:split(Rest, End) of
                [Value, _] -> trim(Value);
                _ -> error(missing_marker)
            end;
        _ ->
            error(missing_marker)
    end.

trim(Value) ->
    iolist_to_binary(string:trim(binary_to_list(Value))).

decode_seed(Seed0) ->
    <<Byte1, Byte2, Seed:32/binary, Crc:16/little-unsigned>> = base32_decode(Seed0),
    SeedPrefix = Byte1 band 16#F8,
    UserPrefix = ((Byte1 band 16#07) bsl 5) bor ((Byte2 band 16#F8) bsr 3),
    true = SeedPrefix =:= 16#90,
    true = UserPrefix =:= 16#A0,
    true = Crc =:= crc16(<<Byte1, Byte2, Seed/binary>>),
    {UserPrefix, Seed}.

base32(<<>>, Acc) ->
    lists:reverse(Acc);
base32(Bin, Acc) when bit_size(Bin) >= 5 ->
    <<Value:5, Rest/bitstring>> = Bin,
    base32(Rest, [lists:nth(Value + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]);
base32(Bin, Acc) ->
    Size = bit_size(Bin),
    <<Value:Size>> = Bin,
    Padded = Value bsl (5 - Size),
    base32(<<>>, [lists:nth(Padded + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]).

base64url(Bin) ->
    Encoded = base64:encode(Bin),
    binary:replace(
        binary:replace(
            binary:replace(Encoded, <<"+">>, <<"-">>, [global]),
            <<"/">>,
            <<"_">>,
            [global]
        ),
        <<"=">>,
        <<>>,
        [global]
    ).

crc16(Bin) -> crc16(Bin, 0).

crc16(<<>>, Crc) ->
    Crc;
crc16(<<Byte, Rest/binary>>, Crc0) ->
    Crc1 = Crc0 bxor (Byte bsl 8),
    crc16(Rest, crc_byte(Crc1, 8)).

crc_byte(Crc, 0) ->
    Crc band 16#FFFF;
crc_byte(Crc, N) when Crc band 16#8000 =/= 0 ->
    crc_byte(((Crc bsl 1) bxor 16#1021) band 16#FFFF, N - 1);
crc_byte(Crc, N) ->
    crc_byte((Crc bsl 1) band 16#FFFF, N - 1).

base32_decode(Bin) ->
    Bits = lists:foldl(
        fun(Char, Acc) -> <<Acc/bitstring, (base32_value(Char)):5>> end,
        <<>>,
        binary_to_list(Bin)
    ),
    <<Bytes:288/bitstring, _Padding:2>> = Bits,
    Bytes.

base32_value(Char) when Char >= $A, Char =< $Z -> Char - $A;
base32_value(Char) when Char >= $2, Char =< $7 -> Char - $2 + 26.
