-module(enats_client).
-export([start_link/1, connect/1, disconnect/1, stop/1, status/1, info/1,
    publish/3, publish/4, subscribe/3, unsubscribe/2, flush/2]).
start_link(Options) ->
    enats_connection:start_link(Options#{owner => maps:get(owner, Options, self())}).
connect(Client) -> enats_connection:connect(Client).
disconnect(Client) -> enats_connection:disconnect(Client).
stop(Client) -> enats_connection:stop(Client).
status(Client) -> enats_connection:status(Client).
info(Client) -> enats_connection:info(Client).
publish(Client, Subject, Payload) -> publish(Client, Subject, Payload, #{}).
publish(Client, Subject, Payload, Options) ->
    case enats_subject:validate_publish(Subject) of
        ok -> enats_connection:publish(Client, Subject, Payload, Options);
        {error, Reason} -> {error, {invalid_subject, Reason}}
    end.
subscribe(Client, Subject, Options) -> enats_connection:subscribe(Client, Subject, Options).
unsubscribe(Client, Subscription) -> enats_connection:unsubscribe(Client, Subscription).
flush(Client, Timeout) -> enats_connection:flush(Client, Timeout).
