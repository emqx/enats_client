-module(basic_pub_sub).

-export([run/0]).

run() ->
    {ok, Client} = enats_client:start_link(#{host => "127.0.0.1", port => 4222}),
    ok = enats_client:connect(Client),
    {ok, Subscription} = enats_client:subscribe(Client, <<"examples.events">>, #{}),
    ok = enats_client:publish(Client, <<"examples.events">>, <<"hello">>),
    ok = enats_client:flush(Client, 1000),
    ok = enats_client:unsubscribe(Client, Subscription),
    ok = enats_client:stop(Client).
