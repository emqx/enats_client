#!/usr/bin/env escript
%%! -mode(compile)

-mode(compile).

main([]) ->
    Files = filelib:wildcard("src/*.erl"),
    Missing = lists:append([missing_specs(File) || File <- Files]),
    case Missing of
        [] ->
            io:format("All exported functions have specs.~n"),
            halt(0);
        _ ->
            lists:foreach(fun({File, Name, Arity}) ->
                io:format("Missing spec: ~s ~p/~p~n", [File, Name, Arity])
            end, Missing),
            halt(1)
    end.

missing_specs(File) ->
    case epp:parse_file(File, [], []) of
        {ok, Forms} ->
            Exports = exported_functions(Forms),
            Specs = specified_functions(Forms),
            [{File, Name, Arity} || {Name, Arity} <- Exports,
                not lists:member({Name, Arity}, Specs),
                Name =/= module_info];
        {error, Error} ->
            io:format(standard_error, "Cannot parse ~s: ~p~n", [File, Error]),
            [{File, parse_error, 0}]
    end.

exported_functions(Forms) ->
    lists:usort(lists:append([
        Functions || {attribute, _, export, Functions} <- Forms
    ])).

specified_functions(Forms) ->
    lists:usort([
        {Name, Arity}
     || {attribute, _, spec, {{Name, Arity}, _Specs}} <- Forms
    ]).
