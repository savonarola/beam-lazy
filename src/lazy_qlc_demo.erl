-module(lazy_qlc_demo).

-export([
    simple_join/0,
    lookup_join/0
]).

-include_lib("stdlib/include/qlc.hrl").

simple_join() ->
    {ok, Conn} = 'Elixir.Redix':start_link(),

    PostQH = lazy_qlc:key_value_table(Conn, <<"post:*">>),
    UserQH = lazy_qlc:key_value_table(Conn, <<"user:*">>),

    TextsQH = qlc:q([
        {key("user:", UserId), Text}
     || {_, #{<<"text">> := Text, <<"user_id">> := UserId}} <- PostQH
    ]),

    TextAuthorsQH =
        qlc:q([
            {Text, maps:get(<<"name">>, User, <<"">>)}
         || {UserKey1, User} <- UserQH,
            {UserKey0, Text} <- TextsQH,
            UserKey0 =:= UserKey1
        ]),

    io:format("Info:~n~s~n", [qlc:info(TextAuthorsQH)]),

    Cursor = qlc:cursor(TextAuthorsQH),
    io:format("Cursor = ~p~n", [Cursor]),
    N = 5,
    io:format("First ~w texts with authors:~n~99999p~n", [N, qlc:next_answers(Cursor, N)]),
    qlc:delete_cursor(Cursor).

lookup_join() ->
    {ok, Conn} = 'Elixir.Redix':start_link(),

    PostQH = lazy_qlc_ext:key_value_table(Conn, <<"post:*">>),
    UserQH = lazy_qlc_ext:key_value_table(Conn, <<"user:*">>),

    TextsQH = qlc:q([
        {key("user:", UserId), Text}
     || {_, #{<<"text">> := Text, <<"user_id">> := UserId}} <- PostQH
    ]),

    TextAuthorsQH =
        qlc:q([
            {Text, maps:get(<<"name">>, User, <<"">>)}
         || {UserKey1, User} <- UserQH,
            {UserKey0, Text} <- TextsQH,
            UserKey0 =:= UserKey1
        ]),

    io:format("Info:~n~s~n", [qlc:info(TextAuthorsQH)]),

    Cursor = qlc:cursor(TextAuthorsQH),
    io:format("Cursor = ~p~n", [Cursor]),
    N = 5,
    io:format("First ~w texts with authors:~n~99999p~n", [N, qlc:next_answers(Cursor, N)]),
    qlc:delete_cursor(Cursor).

key(Prefix, IdBin) ->
    iolist_to_binary([Prefix, IdBin]).
