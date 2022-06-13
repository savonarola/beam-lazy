-module(lazy_qlc_dummy).

-include_lib("stdlib/include/qlc.hrl").

-export([
    sample_qh/0
]).

sample_qh() ->
    qlc:q([X || X <- [1, 2, 3, 4, 5, 6]]).
