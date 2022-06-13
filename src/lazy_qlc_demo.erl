-module(lazy_qlc_demo).

-export([
    simple_join/1,
    lookup_join/1
]).

-include_lib("stdlib/include/qlc.hrl").

simple_join(Conn) ->
    PostQH = lazy_qlc:key_value_table(Conn, <<"post:*">>),
    UserQH = lazy_qlc:key_value_table(Conn, <<"user:*">>),

    TextQH = qlc:q([
        {key("user:", UserId), Text}
     || {_, #{<<"text">> := Text, <<"user_id">> := UserId}} <- PostQH
    ]),

    TextAuthorQH =
        qlc:q([
            {Text, maps:get(<<"name">>, User, <<"">>)}
         || {UserKey1, User} <- UserQH,
            {UserKey0, Text} <- TextQH,
            UserKey0 =:= UserKey1
        ]),

    TextAuthorQH.

lookup_join(Conn) ->
    PostQH = lazy_qlc_ext:key_value_table(Conn, <<"post:*">>),
    UserQH = lazy_qlc_ext:key_value_table(Conn, <<"user:*">>),

    TextQH = qlc:q([
        {key("user:", UserId), Text}
     || {_, #{<<"text">> := Text, <<"user_id">> := UserId}} <- PostQH
    ]),

    TextAuthorQH =
        qlc:q([
            {Text, maps:get(<<"name">>, User, <<"">>)}
         || {UserKey1, User} <- UserQH,
            {UserKey0, Text} <- TextQH,
            UserKey0 =:= UserKey1
        ]),

    TextAuthorQH.

key(Prefix, IdBin) ->
    iolist_to_binary([Prefix, IdBin]).
