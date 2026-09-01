-module(enats_frame_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, t_fragmentation_property/1, t_coalesced_frames_property/1]).

all() -> [t_fragmentation_property, t_coalesced_frames_property].

t_fragmentation_property(_Config) ->
    Frame = <<"HMSG subject 7 27 27\r\nNATS/1.0\r\nX-Test: value\r\n\r\n\r\n">>,
    Expected = [
        #{
            type => hmsg,
            subject => <<"subject">>,
            sid => <<"7">>,
            header_size => 27,
            headers => [{<<"X-Test">>, <<"value">>}],
            payload => <<>>
        }
    ],
    lists:foreach(
        fun(Size) ->
            {First, Rest} = split_at(Frame, Size),
            {Frames1, State1} = enats_frame:parse(First, enats_frame:initial_state()),
            {Frames2, _State2} = enats_frame:parse(Rest, State1),
            ?assertEqual([], Frames1),
            ?assertEqual(Expected, Frames2)
        end,
        lists:seq(1, byte_size(Frame) - 1)
    ).

t_coalesced_frames_property(_Config) ->
    Frame = <<"PING\r\nPONG\r\n+OK\r\n">>,
    {Frames, _State} = enats_frame:parse(Frame, enats_frame:initial_state()),
    ?assertEqual([ping, pong, ok], Frames).

split_at(Bin, Size) ->
    {binary:part(Bin, 0, Size), binary:part(Bin, Size, byte_size(Bin) - Size)}.
