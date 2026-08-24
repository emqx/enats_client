-module(enats_nkey).

-export([encode_public/1, sign_fun/2]).

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
