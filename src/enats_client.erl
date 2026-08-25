-module(enats_client).
-export([start_link/1, connect/1, connect/2, disconnect/1, stop/1, status/1, info/1,
    publish/3, publish/4, request/4, request/5, jetstream_publish/4,
    subscribe/3, unsubscribe/2, flush/2]).
start_link(Options) ->
    enats_connection:start_link(Options#{owner => maps:get(owner, Options, self())}).
connect(Client) -> enats_connection:connect(Client).
connect(Client, Timeout) -> enats_connection:connect(Client, Timeout).
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
request(Client, Subject, Payload, Options) ->
    enats_connection:request(Client, Subject, Payload, Options).
request(Client, Subject, Payload, Options, Timeout) ->
    request(Client, Subject, Payload, Options#{timeout => Timeout}).
jetstream_publish(Client, Subject, Payload, Options) ->
    enats_js:publish(Client, Subject, Payload, Options).
subscribe(Client, Subject, Options) -> enats_connection:subscribe(Client, Subject, Options).
unsubscribe(Client, Subscription) -> enats_connection:unsubscribe(Client, Subscription).
flush(Client, Timeout) -> enats_connection:flush(Client, Timeout).
