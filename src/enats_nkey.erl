-module(enats_nkey).

-export([encode_public/1, sign_fun/2, from_seed/1, sign_seed/2]).

-define(USER_PREFIX, 16#A0).
-define(ALPHABET, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567").

encode_public(PublicKey) when is_binary(PublicKey), byte_size(PublicKey) =:= 32 ->
    Payload = <<?USER_PREFIX, PublicKey/binary>>,
    Raw = <<Payload/binary, (crc16(Payload)):16/little-unsigned>>,
    list_to_binary(base32(Raw, [])).

sign_fun(_PublicKey, PrivateKey)
    when is_binary(PrivateKey), byte_size(PrivateKey) =:= 32,
         is_binary(PrivateKey), byte_size(PrivateKey) =:= 32 ->
    fun(Nonce) ->
        Signature = crypto:sign(eddsa, none, Nonce, [PrivateKey, ed25519]),
        base64url(Signature)
    end.

from_seed(Seed0) when is_binary(Seed0) ->
    try
        {_UserPrefix, Seed} = decode_seed(Seed0),
        {PublicKey, _Private} = crypto:generate_key(eddsa, ed25519, Seed),
        Public = encode_public(PublicKey),
        {ok, Public, sign_fun(Public, Seed)}
    catch
        _:_ -> {error, invalid_nkey_seed}
    end.

sign_seed(Seed0, Nonce) when is_binary(Seed0), is_binary(Nonce) ->
    try
        {_UserPrefix, Seed} = decode_seed(Seed0),
        {PublicKey, _Private} = crypto:generate_key(eddsa, ed25519, Seed),
        Public = encode_public(PublicKey),
        Signature = crypto:sign(eddsa, none, Nonce, [Seed, ed25519]),
        {ok, Public, base64url(Signature)}
    catch
        _:_ -> {error, invalid_nkey_seed}
    end.

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
    base32(Rest, [lists:nth(Value + 1, ?ALPHABET) | Acc]);
base32(Bin, Acc) ->
    Size = bit_size(Bin),
    <<Value:Size>> = Bin,
    Padded = Value bsl (5 - Size),
    base32(<<>>, [lists:nth(Padded + 1, ?ALPHABET) | Acc]).

base64url(Bin) ->
    Encoded = base64:encode(Bin),
    binary:replace(
        binary:replace(binary:replace(Encoded, <<"+">>, <<"-">>, [global]), <<"/">>, <<"_">>, [global]),
        <<"=">>, <<>>, [global]
    ).

crc16(Bin) ->
    crc16(Bin, 0).

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
