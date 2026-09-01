-module(enats_frame_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    t_fragmentation_property/1,
    t_coalesced_frames_property/1,
    t_invalid_payload_property/1
]).

all() -> [t_fragmentation_property, t_coalesced_frames_property, t_invalid_payload_property].

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

t_invalid_payload_property(_Config) ->
    ?assertError(
        badarg,
        enats_frame:parse(<<"MSG subject 1 bad\r\n">>, enats_frame:initial_state())
    ),
    ?assertError(
        {badmatch, false},
        enats_frame:parse(<<"HMSG subject 1 4 3\r\nabc\r\n">>, enats_frame:initial_state())
    ),
    {Frames, _} = enats_frame:parse(
        <<"HMSG subject 1 30 30\r\nNATS/1.0 503 No Responders\r\n\r\n\r\n">>,
        enats_frame:initial_state()
    ),
    ?assertEqual(<<"503">>, proplists:get_value(<<"Status">>, maps:get(headers, hd(Frames)))),
    {CodeOnly, _} = enats_frame:parse(
        <<"HMSG subject 1 16 16\r\nNATS/1.0 503\r\n\r\n\r\n">>,
        enats_frame:initial_state()
    ),
    ?assertEqual(<<"503">>, proplists:get_value(<<"Status">>, maps:get(headers, hd(CodeOnly)))),
    {Malformed, _} = enats_frame:parse(
        <<"HMSG subject 1 23 23\r\nNATS/1.0\r\nMalformed\r\n\r\n\r\n">>,
        enats_frame:initial_state()
    ),
    ?assertEqual([], maps:get(headers, hd(Malformed))).

split_at(Bin, Size) ->
    {binary:part(Bin, 0, Size), binary:part(Bin, Size, byte_size(Bin) - Size)}.
