-module(enats_subject).
-export([validate/1, validate_publish/1]).

-spec validate(binary()) -> ok | {error, term()}.
validate(Subject) when is_binary(Subject), byte_size(Subject) > 0 ->
    case binary:match(Subject, [<<" ">>, <<"\t">>, <<"\r">>, <<"\n">>, <<0>>]) of
        nomatch -> ok;
        _ -> {error, invalid_subject}
    end;
validate(_) -> {error, invalid_subject}.

validate_publish(Subject) ->
    case validate(Subject) of
        ok ->
            case binary:match(Subject, [<<"*">>, <<">">>]) of
                nomatch -> ok;
                _ -> {error, wildcard_subject_not_allowed}
            end;
        Error -> Error
    end.
