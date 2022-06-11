-module(lazy_qlc_ext).

-include_lib("stdlib/include/qlc.hrl").

-export([
    key_value_table/1,
    key_value_table/2
]).

key_value_table(Client) ->
    key_value_table(Client, <<"*">>).

key_value_table(Client, Pattern) ->
    key_value_table(Client, Pattern, <<"hash">>).

key_value_table(Client, Pattern, Type) ->
    NextFun =
        fun
            NextFun(<<"0">>) ->
                [];
            NextFun(RedisCursor) ->
                Command = [<<"SCAN">>, to_cursor(RedisCursor), <<"MATCH">>, Pattern, "TYPE", Type],
                {ok, [NewCursor, Keys]} = 'Elixir.Redix':command(Client, Command),
                case key_values(Client, Keys) of
                    [] -> NextFun(NewCursor);
                    SomeKeyValues -> SomeKeyValues ++ fun() -> NextFun(NewCursor) end
                end
        end,
    LookupFun =
        fun(1, Keys) ->
            key_values(Client, Keys)
        end,
    InfoFun =
        fun
            (keypos) -> 1;
            (is_sorted_key) -> false;
            (is_unique_objects) -> true;
            (_) -> undefined
        end,
    qlc:table(fun() -> NextFun(undefined) end, [
        {lookup_fun, LookupFun}, {info_fun, InfoFun}, {key_equality, '=:='}
    ]).

to_cursor(undefined) -> <<"0">>;
to_cursor(RedisCursor) when is_binary(RedisCursor) -> RedisCursor.

key_values(Client, Keys) ->
    lists:flatmap(
        fun(Key) ->
            case 'Elixir.Redix':command(Client, [<<"HGETALL">>, Key]) of
                {ok, Fields} -> [{Key, plain_list_to_map(Fields)}];
                {error, _} -> []
            end
        end,
        Keys
    ).

plain_list_to_map(Fields) ->
    plain_list_to_map(Fields, #{}).

plain_list_to_map([], Map) -> Map;
plain_list_to_map([K, V | Fields], Map) -> plain_list_to_map(Fields, Map#{K => V}).
