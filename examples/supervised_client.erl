-module(supervised_client).

-export([child_spec/0]).

child_spec() ->
    enats_client:child_spec(#{id => example_client, host => "127.0.0.1", port => 4222}).
