-module(enats_error).
-moduledoc "Public error contract for the NATS client.".

-export([wrap/2, normalize/2]).
-export_type([error/0, reason/0]).

-type reason() ::
    badarg
    | bad_operation
    | auth_error
    | connection_failed
    | timeout
    | protocol_error
    | server_error
    | internal_error.

-type option_key() :: atom() | binary().
-type badarg_code() ::
    bad_type
    | bad_value
    | unknown_keys
    | too_large
    | invalid_name
    | invalid_value
    | unsupported
    | wildcard_not_allowed
    | invalid_credentials
    | invalid_seed
    | invalid_result.
-type badarg_details() :: #{
    field := atom(),
    code := badarg_code(),
    index => pos_integer(),
    keys => [option_key()],
    metric => messages | bytes,
    actual => non_neg_integer(),
    limit => pos_integer()
}.
-type bad_operation_details() :: #{
    operation := atom(),
    code :=
        already_connected
        | connecting
        | draining
        | diagnostics_disabled
        | not_found
        | tls_already_established
}.
-type auth_details() :: #{
    operation := read_credentials | resolve_secret | sign_nonce,
    code := file:posix() | provider_failed | signer_failed
}.
-type connection_cause() :: atom().
-type connection_details() :: #{phase := atom(), cause := connection_cause(), outcome => unknown}.
-type timeout_details() :: #{phase := atom(), outcome => unknown}.
-type protocol_details() :: #{
    phase := atom(), code := invalid_frame | invalid_ack | missing_nonce
}.
-type server_details() ::
    #{source := core, code := nats_error, message := binary()}
    | #{source := core, code := no_responders, status := non_neg_integer()}
    | #{
        source := jetstream,
        code := unavailable | rejected,
        status := non_neg_integer(),
        err_code => non_neg_integer()
    }.
-type internal_details() :: #{operation := atom(), code := client_exit | unexpected_failure}.

-type error() ::
    #{reason := badarg, details := badarg_details()}
    | #{reason := bad_operation, details := bad_operation_details()}
    | #{reason := auth_error, details := auth_details()}
    | #{reason := connection_failed, details := connection_details()}
    | #{reason := timeout, details := timeout_details()}
    | #{reason := protocol_error, details := protocol_details()}
    | #{reason := server_error, details := server_details()}
    | #{reason := internal_error, details := internal_details()}.

-spec wrap(atom(), term()) -> term().
wrap(Operation, {error, Raw}) ->
    {error, normalize(Operation, Raw)};
wrap(_Operation, Result) ->
    Result.

-spec normalize(atom(), term()) -> error().
normalize(_Operation, #{reason := _, details := _} = Error) ->
    Error;
normalize(_Operation, {invalid, auth, #{reason := _, details := _} = Error}) ->
    Error;
normalize(Operation, {invalid, batch_message, {Index, Inner}}) when is_integer(Index) ->
    #{reason := badarg, details := InnerDetails} = normalize(Operation, Inner),
    make(badarg, InnerDetails#{index => Index});
normalize(_Operation, {invalid, batch, {too_large, Kind, Actual, Limit}}) ->
    make(badarg, #{
        field => batch, code => too_large, metric => Kind, actual => Actual, limit => Limit
    });
normalize(_Operation, {invalid, Field, {unknown_keys, Keys}}) ->
    make(badarg, #{field => Field, code => unknown_keys, keys => safe_keys(Keys)});
normalize(_Operation, {invalid, Field, {too_large, Limit}}) ->
    make(badarg, #{field => Field, code => too_large, limit => Limit});
normalize(_Operation, {invalid, Field, {bad_value, _Value}}) ->
    make(badarg, #{field => Field, code => bad_value});
normalize(_Operation, {invalid, Field, {invalid_name, _Name}}) ->
    make(badarg, #{field => Field, code => invalid_name});
normalize(_Operation, {invalid, Field, {invalid_value, _Value}}) ->
    make(badarg, #{field => Field, code => invalid_value});
normalize(_Operation, {invalid, Field, Code}) when is_atom(Code) ->
    make(badarg, #{field => Field, code => Code});
normalize(_Operation, {invalid, Field, _Detail}) ->
    make(badarg, #{field => Field, code => bad_value});
normalize(_Operation, invalid_credentials) ->
    badarg(authentication, invalid_credentials);
normalize(_Operation, invalid_credentials_type) ->
    badarg(authentication, bad_type);
normalize(_Operation, invalid_nkey_seed) ->
    badarg(nkey_seed, invalid_seed);
normalize(Operation, invalid_secret_type) when Operation =:= from_seed; Operation =:= sign_seed ->
    badarg(nkey_seed, bad_type);
normalize(_Operation, invalid_secret_type) ->
    badarg(authentication, bad_type);
normalize(_Operation, {invalid_nkey_signature, _}) ->
    badarg(sign_fun, invalid_result);
normalize(_Operation, {credentials_file, Code}) when is_atom(Code) ->
    make(auth_error, #{operation => read_credentials, code => Code});
normalize(_Operation, secret_provider_failed) ->
    make(auth_error, #{operation => resolve_secret, code => provider_failed});
normalize(_Operation, {nkey_sign_failed, _}) ->
    make(auth_error, #{operation => sign_nonce, code => signer_failed});
normalize(_Operation, nkey_nonce_missing) ->
    make(protocol_error, #{phase => authentication, code => missing_nonce});
normalize(Operation, {disconnected, Cause}) ->
    case Cause of
        {server_error, _} -> normalize(Operation, Cause);
        {protocol, _} -> normalize(Operation, Cause);
        {invalid, _, _} -> normalize(Operation, Cause);
        _ -> connection(Operation, Cause)
    end;
normalize(Operation, disconnected) ->
    make(connection_failed, #{phase => Operation, cause => disconnected});
normalize(Operation, Cause) when
    Cause =:= closed;
    Cause =:= stale_connection;
    Cause =:= econnrefused;
    Cause =:= econnreset;
    Cause =:= nxdomain;
    Cause =:= no_servers_available;
    Cause =:= tls_not_available
->
    connection(Operation, Cause);
normalize(Operation, {transport, Cause}) ->
    connection(Operation, Cause);
normalize(Operation, {tls_upgrade_failed, _Cause}) ->
    connection(Operation, tls_failed);
normalize(Operation, timeout) ->
    Details = #{phase => Operation},
    case outcome_unknown(Operation) of
        true -> make(timeout, Details#{outcome => unknown});
        false -> make(timeout, Details)
    end;
normalize(Operation, {protocol, _Cause}) ->
    make(protocol_error, #{phase => Operation, code => invalid_frame});
normalize(_Operation, {server_error, {unknown_frame, _Frame}}) ->
    make(protocol_error, #{phase => 'receive', code => invalid_frame});
normalize(_Operation, {server_error, Message}) when is_binary(Message) ->
    make(server_error, #{source => core, code => nats_error, message => truncate(Message)});
normalize(_Operation, {no_responders, Status}) ->
    case status(Status) of
        {ok, Code} -> make(server_error, #{source => core, code => no_responders, status => Code});
        error -> make(protocol_error, #{phase => request, code => invalid_frame})
    end;
normalize(_Operation, {jetstream, Kind, Status}) when Kind =:= unavailable; Kind =:= rejected ->
    case status(Status) of
        {ok, Code} ->
            make(server_error, #{source => jetstream, code => Kind, status => Code});
        error ->
            make(protocol_error, #{phase => puback, code => invalid_ack})
    end;
normalize(_Operation, {jetstream, Kind, Status, ErrCode}) when
    (Kind =:= unavailable orelse Kind =:= rejected), is_integer(ErrCode), ErrCode >= 0
->
    case status(Status) of
        {ok, Code} ->
            make(server_error, #{
                source => jetstream, code => Kind, status => Code, err_code => ErrCode
            });
        error ->
            make(protocol_error, #{phase => puback, code => invalid_ack})
    end;
normalize(_Operation, {jetstream, invalid_ack, _}) ->
    make(protocol_error, #{phase => puback, code => invalid_ack});
normalize(Operation, {client_exit, _Cause}) ->
    make(internal_error, #{operation => Operation, code => client_exit});
normalize(Operation, Code) when
    Code =:= already_connected;
    Code =:= connecting;
    Code =:= draining;
    Code =:= diagnostics_disabled;
    Code =:= not_found;
    Code =:= tls_already_established
->
    make(bad_operation, #{operation => Operation, code => Code});
normalize(Operation, _Unexpected) ->
    make(internal_error, #{operation => Operation, code => unexpected_failure}).

badarg(Field, Code) -> make(badarg, #{field => Field, code => Code}).

connection(Operation, Cause) ->
    Details = #{phase => Operation, cause => connection_cause(Cause)},
    case outcome_unknown(Operation) of
        true -> make(connection_failed, Details#{outcome => unknown});
        false -> make(connection_failed, Details)
    end.

connection_cause(Cause) when is_atom(Cause) ->
    Cause;
connection_cause(_) ->
    other.

outcome_unknown(Operation) ->
    lists:member(Operation, [publish, publish_batch, flush, request, jetstream_publish, puback]).

safe_keys(Keys) when is_list(Keys) ->
    [Key || Key <- Keys, is_atom(Key) orelse is_binary(Key)];
safe_keys(_) ->
    [].

status(Code) when is_integer(Code), Code >= 0 -> {ok, Code};
status(<<A, B, C>>) when A >= $0, A =< $9, B >= $0, B =< $9, C >= $0, C =< $9 ->
    {ok, binary_to_integer(<<A, B, C>>)};
status(_) ->
    error.

truncate(Bin) when byte_size(Bin) > 256 -> binary:part(Bin, 0, 256);
truncate(Bin) -> Bin.

make(Reason, Details) -> #{reason => Reason, details => Details}.
