-module(lazy_qlc).

-include_lib("stdlib/include/qlc.hrl").

-export([
    key_table/1,
    key_table/2,
    key_table/3,

    key_value_table/1,
    key_value_table/2
]).

key_table(Client) ->
    key_table(Client, <<"*">>).

key_table(Client, Pattern) ->
    key_table(Client, Pattern, <<"hash">>).

key_table(Client, Pattern, Type) ->
    NextFun =
        fun
            NextFun(<<"0">>) ->
                [];
            NextFun(RedisCursor) ->
                Command = [<<"SCAN">>, to_cursor(RedisCursor), <<"MATCH">>, Pattern, "TYPE", Type],
                [NewCursor, Keys] = 'Elixir.Redix':'command!'(Client, Command),
                case Keys of
                    [] -> NextFun(NewCursor);
                    SomeKeys -> SomeKeys ++ fun() -> NextFun(NewCursor) end
                end
        end,
    qlc:table(fun() -> NextFun(undefined) end, []).

to_cursor(undefined) -> <<"0">>;
to_cursor(RedisCursor) when is_binary(RedisCursor) -> RedisCursor.

key_value_table(Client) ->
    key_value_table(Client, <<"*">>).

key_value_table(Client, Pattern) ->
    RedisResutltsQH = qlc:q([
        {Key, 'Elixir.Redix':command(Client, [<<"HGETALL">>, Key])}
     || Key <- key_table(Client, Pattern, <<"hash">>)
    ]),
    qlc:q([
        {Key, plain_list_to_map(Fields)}
     || {Key, {ok, Fields}} <- RedisResutltsQH
    ]).

plain_list_to_map(Fields) ->
    plain_list_to_map(Fields, #{}).

plain_list_to_map([], Map) -> Map;
plain_list_to_map([K, V | Fields], Map) -> plain_list_to_map(Fields, Map#{K => V}).
