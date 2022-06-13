# Lazy Sequences in Elixir and Erlang

In this article, I would like to demonstrate and compare standard primitives for working with lazy sequences in Elixir and Erlang. While Elixir's ones seem pretty well known, the ones from Erlang seem a bit underrated.

## Lazyness and Lazy Sequences

Here we treat laziness from a practitioner's point of view without diving into sophisticated CS concepts (like functor properties of laziness, etc.).

To be even more concrete, we select a particular task of iterating data structures stored in a [Redis](https://redis.io/) database.

The data structures are Redis [hashes](https://redis.io/docs/manual/data-types/#hashes) representing related entities: Users and Posts.

Users have fields `id` and `name` and are stored by `user:ID` key.

Posts have fields `id`, `text`, and `user_id` fields and are stored by `post:ID` key. `user_id` is the id of the user who has written the post.

Sample data may be created by running [`mix lazy_seed`](https://github.com/savonarola/beam-lazy/blob/main/lib/mix/tasks/lazy_seed.ex) in the [repo with samples](https://github.com/savonarola/beam-lazy).


## Lazy Sequences in General

Generally, a lazy sequence is a sequence in which elements are calculated on demand. Obviously, if a language allows preserving a state anyhow, it is not challenging to handcraft a lazy sequence.

Let's implement an Elixir `GenServer` that allows iterating over Redis keys satisfying a particular pattern.

```Elixir
defmodule Lazy.Naive do
  @moduledoc false

  use GenServer

  @count "10"

  def start_link(client, pattern \\ "*") do
    GenServer.start_link(__MODULE__, [client, pattern])
  end

  def next(pid) do
    GenServer.call(pid, :next)
  end

  def init([client, pattern]) do
    {:ok,
     %{
       client: client,
       pattern: pattern,
       cursor: nil,
       buffer: []
     }}
  end

  def handle_call(:next, _from, %{buffer: [key | keys]} = st) do
    {:reply, key, %{st | buffer: keys}}
  end

  def handle_call(:next, _from, %{cursor: "0"} = st) do
    {:reply, nil, st}
  end

  def handle_call(:next, from, st) do
    command = ["SCAN", st.cursor || "0", "MATCH", st.pattern, "COUNT", @count]
    {:ok, [cursor, buffer]} = Redix.command(st.client, command)
    new_st = %{st | cursor: cursor, buffer: buffer}
    handle_call(:next, from, new_st)
  end
end
```

Things to note:
* We use Redis [`SCAN`](https://redis.io/commands/scan/) command to iterate Redis keys in batches.
* We ask Redis to scan in batches of size 10, and we use an internal buffer to keep results between scans.
* We stop scans when we get `"0"` as a new cursor value from Redis and start to return `nil` values.

This is how the module can be used:
```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.298.0>}
iex(2)> {:ok, c} = Lazy.Naive.start_link(conn)
{:ok, #PID<0.304.0>}
iex(3)> Lazy.Naive.next(c)
"post:135"
iex(4)> Lazy.Naive.next(c)
"post:113"
iex(5)> Lazy.Naive.next(c)
"post:926"
...
```

While this may be acceptable for some tasks, this implementation has many flaws:
* We have to handle many things manually like element buffering.
* If we wanted to reuse such "iterators", e.g., for implementing a sequence of key _values_, we would have to implement
some abstract "glue" code for "mapping" such iterators.

Many languages have built-in tools for dealing with lazy sequences to prevent tedious and complicated code crafting. For example, Python has a very powerful concept of [generators](https://docs.python.org/3/glossary.html#term-generator) and some libraries for composing them, like [itertools](https://docs.python.org/3/library/itertools.html). This allows us to have some useful lazy sequences, like a sequence of lines in a file that is not read yet.

So in Elixir and Erlang, we also expect some first-class support for lazy sequences. Let's see what they offer.

## Lazy Sequences in Elixir

In Elixir, we have the powerful [`Stream`](https://hexdocs.pm/elixir/Stream.html) module for lazy sequences. This module provides functions for creating lazy sequences (streams) and for their lazy combinations and transformations.

One of the essential functions is [`Stream.resource/3`](https://hexdocs.pm/elixir/Stream.html#resource/3) that allows custom stream creation.

Let's implement a lazy sequence of Redis keys with streams.

```Elixir
defmodule Lazy.Stream do
  @moduledoc false

  def keys(client, pattern \\ "*", type \\ "hash") do
    Stream.resource(
      ## start_fun
      fn -> nil end,

      ## next_fun
      fn cursor ->
        case cursor do
          "0" ->
            {:halt, cursor}

          _ ->
            command = ["SCAN", cursor || "0", "MATCH", pattern, "TYPE", type]
            [new_cursor, keys] = Redix.command!(client, command)
            {keys, new_cursor}
        end
      end,

      ## after_fun
      fn _cursor -> :ok end
    )
  end
end
```

The core function here is `next_fun`, which generates new values. We return `{NewElements, NewAcc}` when some elements are fetched from Redis, and we return `{:halt, NewAcc}` when no elements are left.

Streams are lazy, so when we want to finaly evaluate something, we need call an `Enum` function. Example usage:
```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.240.0>}
iex(2)> s = Lazy.Stream.keys(conn, "post:*")
#Function<51.58486609/2 in Stream.resource/3>
iex(3)> |> Enum.take(5)
["post:135", "post:113", "post:926", "post:866", "post:757"]
```

If we attach to the Redis and run the `MONITOR` command, we see that keys are scanned really lazily:
```
>redis-cli
127.0.0.1:6379> monitor
OK
1655138072.880413 [0 127.0.0.1:64636] "SCAN" "0" "MATCH" "post:*" "TYPE" "hash"
```

* No commands are emitted to Redis until we run `Enum.take/2`.
* Only as many keys are scanned as needed.

Now let's demonstrate stream composability. We implement a new stream that contains not only keys but `{key, value}` tuples for Redis keys.

```Elixir
defmodule Lazy.Stream do
  @moduledoc false

  def keys(client, pattern \\ "*", type \\ "hash") do
    ...
  end

  def key_values(client, pattern \\ "*") do
    client
    ## Make a stream of keys
    |> keys(pattern, "hash")
    ## Lazily map keys to {key, value} tuples
    |> Stream.flat_map(fn key ->
      case Redix.command(client, ["HGETALL", key]) do
        {:ok, fields} -> [{key, plain_list_to_map(fields)}]
        _ -> []
      end
    end)
  end

  defp plain_list_to_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Map.new(fn [k, v] -> {k, v} end)
  end
end
```

Example usage:
```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.240.0>}
iex(2)> s = Lazy.Stream.key_values(conn, "post:*")
#Function<59.58486609/2 in Stream.transform/3>
iex(3)> |> Enum.take(5)
[
  {"post:135",
   %{
     "id" => "135",
     "text" => "Ex iure sint omnis aut laborum cumque in maiores.",
     "user_id" => "10"
   }},
  ...
  {"post:757",
   %{
     "id" => "757",
     "text" => "Totam adipisci necessitatibus error voluptas ut.",
     "user_id" => "7"
   }}
]
```

`MONITOR` output:
```
>redis-cli
127.0.0.1:6379> monitor
OK
1655138531.909443 [0 127.0.0.1:64679] "SCAN" "0" "MATCH" "post:*" "TYPE" "hash"
1655138531.909866 [0 127.0.0.1:64679] "HGETALL" "post:135"
1655138531.926525 [0 127.0.0.1:64679] "HGETALL" "post:113"
1655138531.927186 [0 127.0.0.1:64679] "HGETALL" "post:926"
1655138531.927361 [0 127.0.0.1:64679] "HGETALL" "post:866"
1655138531.927525 [0 127.0.0.1:64679] "HGETALL" "post:757"
```

We see that stream combination preserves laziness. We do not need to read the whole stream to run [`flat_map.2`](https://hexdocs.pm/elixir/Stream.html#flat_map/2) over it. All commands are emitted to Redis on demand.

An example usage could be:
```elixir
{:ok, conn} = Redix.start_link()

conn
|> Lazy.Stream.key_values("post:*")
|> Stream.map(fn {_key, post} -> post end)
|> Stream.filter(fn %{"text" => text} -> String.length(text) > 10 end)
|> Enum.take(10)
```
Here we (lazily) fetch the first ten posts with text longer than ten characters.

Now let's see what Erlang offers for lazy sequences.

## Lazy Sequences in Erlang

In Erlang, we have a very powerful module, [`qlc`](https://www.erlang.org/doc/man/qlc.html). Surprisingly, even mature developers are often not aware of `qlc`.

QLC stands for "Query List Comprehensions". This module converts Erlang [list comprehentions](https://www.erlang.org/doc/programming_examples/list_comprehensions.html) into lazily evaluated sequences. This is a language extension implemented with parse transformations, so modules using `qlc` generally should include `qlc.hrl`.

### `qlc` Basics

In `qlc`, we deal not with lazy _sequences_, but with _tables_. Later we will see why. Let's implement a very simple table wrapping a list:

```Erlang
-module(lazy_qlc_dummy).

-include_lib("stdlib/include/qlc.hrl").

-export([
    sample_qh/0
]).

sample_qh() ->
    qlc:q([X || X <- [1, 2, 3, 4, 5, 6]]).
```

This table has nothing to do with laziness: the whole wrapped list resides in memory. We just demonstrate how to work with tables.

We can read the entire table:
```elixir
iex(1)> qh = :lazy_qlc_dummy.sample_qh()
{:qlc_handle,
 {:qlc_lc, #Function<0.114243475/0 in :lazy_qlc_dummy.sample_qh/0>,
  {:qlc_opt, false, false, -1, :any, [], :any, 524288, :allowed}}}
iex(2)> :qlc.eval(qh)
[1, 2, 3, 4, 5, 6]
iex(3)>
```
We may also create a _cursor_ and read values sequentially:

```elixir
iex(5)> c = :qlc.cursor(qh)
{:qlc_cursor, {#PID<0.274.0>, #PID<0.266.0>}}
iex(6)> :qlc.next_answers(c, 2)
[1, 2]
iex(7)> :qlc.next_answers(c, 2)
[3, 4]
iex(8)> :qlc.next_answers(c, 2)
[5, 6]
iex(9)> :qlc.next_answers(c, 2)
[]
iex(10)> :qlc.delete_cursor(c)
:ok
```

`qlc` tables can also be [appended](https://www.erlang.org/doc/man/qlc.html#append-1), [folded](https://www.erlang.org/doc/man/qlc.html#fold-3), etc.

### `qlc` Table for Redis Keys

To implement a custom `qlc` table, we have [`qlc:table/2`](https://www.erlang.org/doc/man/qlc.html#table-2) function. Let's use it to create a minimal lazy table of Redis keys:
```Erlang
-module(lazy_qlc).

-include_lib("stdlib/include/qlc.hrl").

-export([
    key_table/1,
    key_table/2,
    key_table/3
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
```

As we see, `qlc:table/2` has the only required argument: a function generating table values. It looks much like the `next_fun` argument of `Stream.resourse/3`, but there are some differences:
* The generating functions return a simple list of elements to terminate, not a `{:halt, acc}` tuple.
* To continue evaluation, the function should return an _improper list_ of calculated elements ending with a continuation function that calculates the rest.
* We can't return an improper list with no elements. So we immediately make a recursive call if no keys are fetched, but the Redis cursor does not yet indicate the end of scanning (`"0"`).

NB. An _improper_ list is a list with elements concatenated to some value other than an empty list:
```elixir
iex(1)> [1 | [2 | []]]
[1, 2]
iex(2)> [1 | [2 | :a]]
[1, 2 | :a]
iex(3)> [1, 2] ++ :a
[1, 2 | :a]
```

Let's demonstrate the table usage:
```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.247.0>}
iex(2)> qh = :lazy_qlc.key_table(conn, "post:*")
{:qlc_handle,
 {:qlc_table, #Function<1.40036711/0 in :lazy_qlc.key_table/3>, false,
  :undefined, :undefined, :undefined, :undefined, :undefined, :undefined,
  :"=:=", :undefined, :no_match_spec}}
iex(3)> c = :qlc.cursor(qh)
{:qlc_cursor, {#PID<0.253.0>, #PID<0.245.0>}}
iex(4)> :qlc.next_answers(c, 5)
["post:135", "post:113", "post:926", "post:866", "post:757"]
iex(5)> :qlc.delete_cursor(c)
:ok
```

If we look into the `MONITOR` output, we see that:
* Calculations are lazy. Only the required number of keys are scanned.
* Nothing is really emitted to Redis until we create a cursor.
```
>redis-cli
127.0.0.1:6379> monitor
OK
1655142148.374692 [0 127.0.0.1:65048] "SCAN" "0" "MATCH" "post:*" "TYPE" "hash"
```

### `qlc` Table for Redis `{key, value}` Tuples

As for the streams, we now implement a more useful table that holds both keys and values. We do this on top of the previous implementation:

```Erlang
-module(lazy_qlc).

-include_lib("stdlib/include/qlc.hrl").

-export([
    key_table/1,
    key_table/2,
    key_table/3,

    key_value_table/1,
    key_value_table/2
]).

...

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
```

In `key_value_table/2`, we sequentially construct tables on top of previous ones as we do with lists, but `qlc` magic makes everything lazy. Nothing is calculated unless we create a cursor, run `eval`, etc.

Let's try the new function out:
```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.247.0>}
iex(2)> qh = :lazy_qlc.key_value_table(conn, "post:*")
{:qlc_handle,
 {:qlc_lc, #Function<3.40036711/0 in :lazy_qlc.key_value_table/2>,
  {:qlc_opt, false, false, -1, :any, [], :any, 524288, :allowed}}}
iex(3)> c = :qlc.cursor(qh)
{:qlc_cursor, {#PID<0.253.0>, #PID<0.245.0>}}
iex(4)> :qlc.next_answers(c, 5)
[
  {"post:135",
   %{
     "id" => "135",
     "text" => "Ex iure sint omnis aut laborum cumque in maiores.",
     "user_id" => "10"
   }},
...
  {"post:757",
   %{
     "id" => "757",
     "text" => "Totam adipisci necessitatibus error voluptas ut.",
     "user_id" => "7"
   }}
]
iex(5)> :qlc.delete_cursor(c)
:ok
```

The `MONITOR` output is the same as for the streams variant:
```
>redis-cli
127.0.0.1:6379> monitor
OK
1655142882.971584 [0 127.0.0.1:65111] "SCAN" "0" "MATCH" "post:*" "TYPE" "hash"
1655142882.971845 [0 127.0.0.1:65111] "HGETALL" "post:135"
1655142882.971967 [0 127.0.0.1:65111] "HGETALL" "post:113"
1655142882.972115 [0 127.0.0.1:65111] "HGETALL" "post:926"
1655142882.972316 [0 127.0.0.1:65111] "HGETALL" "post:866"
1655142882.972463 [0 127.0.0.1:65111] "HGETALL" "post:757"
```

We see that only required operations are emitted.

### `qlc` Table Joins

Besides serving as lazy sequences, `qlc` tables provide some additional features.

Recall our test data structure.

We have users as Redis hashes, like:
```Elixir
%{
  "id" => "5",
  "name" => "Daphnee Conroy"
}
```
We also have posts like:
```Elixir
%{
    "id" => "866",
    "text" => "Voluptatem architecto nihil blanditiis?",
    "user_id" => "5"
  }
```
I.e., `user_id` in posts serves as an external key.

Let us try to _join_ texts with author names, i.e., obtain a sequence of all texts together with their authors, like
```elixir
{"Assumenda officiis quaerat nihil.", "Arlie Heller Jr."},
{"Aut incidunt veritatis quisquam sit repudiandae voluptas commodi culpa.", "Chanel Rowe"},
{"Debitis et repellat soluta.", "Daphnee Conroy"},
...
```

First, we construct two tables with common structure of the first tuple element:
* `UserQH` using `lazy_qlc:key_value_table/2` directly and holding tuples `{"user:ID", UserData}`
* `TextQH` using `lazy_qlc:key_value_table/2` for posts and then converting them to the form `{"user:ID", PostData}` using posts' `user_id` field.

Then we join these tables with another comprehension, much like we would do this with lists:
```Erlang
-module(lazy_qlc_demo).

-export([
    simple_join/0
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
```

Let's try that in the shell:
```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.247.0>}
iex(2)> qh = :lazy_qlc_demo.simple_join(conn)
{:qlc_handle,
 {:qlc_lc, #Function<1.109218922/0 in :lazy_qlc_demo.simple_join/1>,
  {:qlc_opt, false, false, -1, :any, [], :any, 524288, :allowed}}}
iex(3)> c = :qlc.cursor(qh)
{:qlc_cursor, {#PID<0.253.0>, #PID<0.245.0>}}
iex(4)> :qlc.next_answers(c, 5)
[
  {"Assumenda officiis quaerat nihil.", "Arlie Heller Jr."},
  {"Aut incidunt veritatis quisquam sit repudiandae voluptas commodi culpa.",
   "Arlie Heller Jr."},
  {"Debitis et repellat soluta.", "Arlie Heller Jr."},
  {"Fuga molestiae alias voluptates?", "Arlie Heller Jr."},
  {"Amet quasi explicabo et ut sunt fuga enim blanditiis.", "Arlie Heller Jr."}
]
iex(5)> :qlc.delete_cursor(c)
:ok
```

The result looks like the expected. But what is the complexity of such a computation? For list comprehensions,
we would expect `n_users * n_posts` complexity. But if we look into `MONITOR` output, we see:
```
1655147373.380531 [0 127.0.0.1:65111] "SCAN" "0" "MATCH" "user:*" "TYPE" "hash"
1655147373.380828 [0 127.0.0.1:65111] "SCAN" "640" "MATCH" "user:*" "TYPE" "hash"
1655147373.381067 [0 127.0.0.1:65111] "SCAN" "704" "MATCH" "user:*" "TYPE" "hash"
1655147373.381274 [0 127.0.0.1:65111] "SCAN" "416" "MATCH" "user:*" "TYPE" "hash"
1655147373.381472 [0 127.0.0.1:65111] "SCAN" "992" "MATCH" "user:*" "TYPE" "hash"
1655147373.381615 [0 127.0.0.1:65111] "SCAN" "912" "MATCH" "user:*" "TYPE" "hash"
1655147373.381787 [0 127.0.0.1:65111] "HGETALL" "user:5"
1655147373.381948 [0 127.0.0.1:65111] "SCAN" "560" "MATCH" "user:*" "TYPE" "hash"
1655147373.382064 [0 127.0.0.1:65111] "SCAN" "112" "MATCH" "user:*" "TYPE" "hash"
1655147373.382233 [0 127.0.0.1:65111] "HGETALL" "user:9"
...
1655147745.653085 [0 127.0.0.1:65433] "SCAN" "127" "MATCH" "user:*" "TYPE" "hash"
1655147745.653167 [0 127.0.0.1:65433] "SCAN" "511" "MATCH" "user:*" "TYPE" "hash"
1655147745.653269 [0 127.0.0.1:65433] "SCAN" "0" "MATCH" "post:*" "TYPE" "hash"
1655147745.653421 [0 127.0.0.1:65433] "HGETALL" "post:135"
1655147745.653513 [0 127.0.0.1:65433] "HGETALL" "post:113"
1655147745.653588 [0 127.0.0.1:65433] "HGETALL" "post:926"
1655147745.653663 [0 127.0.0.1:65433] "HGETALL" "post:866"
1655147745.653735 [0 127.0.0.1:65433] "HGETALL" "post:757"
...
1655147745.768472 [0 127.0.0.1:65433] "HGETALL" "post:225"
1655147745.768534 [0 127.0.0.1:65433] "SCAN" "511" "MATCH" "post:*" "TYPE" "hash"
1655147745.768592 [0 127.0.0.1:65433] "HGETALL" "post:423"
```
I.e., all the users are scanned once, and so are the posts.

To know what's happening, we have `qlc:info/2` function.

```elixir
iex(6)> IO.puts(:qlc.info(qh))
begin
    V1 =
        qlc:q([
               {Key,
                'Elixir.Redix':command(Client, [<<"HGETALL">>, Key])} ||
                   Key <- '$MOD':'$FUN'()
              ]),
    V2 =
        qlc:q([
               {Key, plain_list_to_map(Fields)} ||
                   {Key, {ok, Fields}} <- V1
              ]),
    V3 =
        qlc:q([
               P0 ||
                   P0 = {UserKey1, User} <- qlc:keysort(1, V2, [])
              ]),
    V4 =
        qlc:q([
               {Key,
                'Elixir.Redix':command(Client, [<<"HGETALL">>, Key])} ||
                   Key <- '$MOD':'$FUN'()
              ]),
    V5 =
        qlc:q([
               {Key, plain_list_to_map(Fields)} ||
                   {Key, {ok, Fields}} <- V4
              ]),
    V6 =
        qlc:q([
               {key("user:", UserId), Text} ||
                   {_, #{<<"text">> := Text, <<"user_id">> := UserId}} <-
                       V5
              ]),
    V7 =
        qlc:q([
               P0 ||
                   P0 = {UserKey0, Text} <- qlc:keysort(1, V6, [])
              ]),
    V8 =
        qlc:q([
               [G1 | G2] ||
                   G1 <- V3,
                   G2 <- V7,
                   element(1, G1) == element(1, G2)
              ],
              [{join, merge}]),
    qlc:q([
           {Text, maps:get(<<"name">>, User, <<"">>)} ||
               [{UserKey1, User} | {UserKey0, Text}] <- V8,
               UserKey0 =:= UserKey1
          ])
end
```

Wow, that's quite a plan!

This function shows how a table is going to be calculated. The most important line here is
```Erlang
 [{join, merge}]),
```
It means that `qlc` is going to perform a [merge join](https://en.wikipedia.org/wiki/Sort-merge_join) for our two tables (`UserQH` and `TextQH`), like a relational database.

Although the very algorithm is quite efficient, it requires sorting the incoming tables, so they should be read in advance.

What if we want to preserve laziness and run joins in constant memory?

### `qlc` Table Lookup Joins

Now we will see why tables are called so.

First, we rewrite our `lazy_qlc:key_value_table/2` function a bit:

```erlang
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
```

Things to note:
* We now implement `key_value_table` function from scratch through `qlc:table/2`, not on top of `key_table`. This makes us look up values inside `NextFun` directly.
* We also provide `InfoFun`, saying that tuples in our table have unique (key) values in the first position (indeed, it is the Redis key).
* We also provide `LookupFun` that allows us to fetch a table record directly by a key.

Things do not differ significantly if we make some simple use of the new table.

```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.267.0>}
iex(2)> qh = :lazy_qlc_ext.key_value_table(conn, "post:*")
{:qlc_handle,
 {:qlc_table, #Function<3.87812728/0 in :lazy_qlc_ext.key_value_table/3>, false,
  :undefined, :undefined,
  #Function<2.87812728/1 in :lazy_qlc_ext.key_value_table/3>, :undefined,
  #Function<1.87812728/2 in :lazy_qlc_ext.key_value_table/3>, :undefined,
  :"=:=", :undefined, :no_match_spec}}
iex(3)> c = :qlc.cursor(qh)
{:qlc_cursor, {#PID<0.273.0>, #PID<0.265.0>}}
iex(4)> :qlc.next_answers(c, 5)
[
  {"post:135",
   %{
     "id" => "135",
     "text" => "Ex iure sint omnis aut laborum cumque in maiores.",
     "user_id" => "10"
   }},
...
  {"post:757",
   %{
     "id" => "757",
     "text" => "Totam adipisci necessitatibus error voluptas ut.",
     "user_id" => "7"
   }}
]
iex(5)> :qlc.delete_cursor(c)
:ok
```

But let us try to make a join using our new tables:

```erlang
-module(lazy_qlc_demo).

...

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
```

The plan is entirely different now:

```elixir
iex(1)> {:ok, conn} = Redix.start_link()
{:ok, #PID<0.247.0>}
iex(2)> qh = :lazy_qlc_d
lazy_qlc_demo     lazy_qlc_dummy
iex(2)> qh = :lazy_qlc_demo.lookup_join(conn)
{:qlc_handle,
 {:qlc_lc, #Function<3.109218922/0 in :lazy_qlc_demo.lookup_join/1>,
  {:qlc_opt, false, false, -1, :any, [], :any, 524288, :allowed}}}
iex(3)> IO.puts(:qlc.info(qh))
begin
    V1 =
        qlc:q([
               {key("user:", UserId), Text} ||
                   {_, #{<<"text">> := Text, <<"user_id">> := UserId}} <-
                       '$MOD':'$FUN'()
              ]),
    V2 =
        qlc:q([
               P0 ||
                   P0 = {UserKey0, Text} <- V1
              ]),
    V3 =
        qlc:q([
               [G1 | G2] ||
                   G2 <- V2,
                   G1 <- '$MOD':'$FUN'(),
                   element(1, G1) =:= element(1, G2)
              ],
              [{join, lookup}]),
    qlc:q([
           {Text, maps:get(<<"name">>, User, <<"">>)} ||
               [{UserKey1, User} | {UserKey0, Text}] <- V3
          ])
end
:ok
```

First, it is smaller because we have fewer intermediate tables now.

Second, the join method is now `[{join, lookup}]),`.

To see what that means, let's fetch some results from the table and look at the `MONITOR` output:

```elixir
iex(10)> c = :qlc.cursor(qh)
{:qlc_cursor, {#PID<0.265.0>, #PID<0.245.0>}}
iex(11)> :qlc.next_answers(c, 20)
[
  {"Ex iure sint omnis aut laborum cumque in maiores.", "Genevieve Schuster"},
  {"Deleniti quasi temporibus accusamus in illum quisquam dolores qui quasi.",
   "Genevieve Schuster"},
...
  {"Assumenda officiis quaerat nihil.", "Arlie Heller Jr."},
  {"Voluptatem perspiciatis vel eius.", "Genevieve Schuster"},
  {"Quisquam dolore corrupti minima ut?", "Lavinia Pagac"}
]
iex(12)> :qlc.delete_cursor(c)
:ok
```

```
>redis-cli
127.0.0.1:6379> monitor
OK
1655152183.526654 [0 127.0.0.1:49337] "SCAN" "0" "MATCH" "post:*" "TYPE" "hash"
1655152183.526952 [0 127.0.0.1:49337] "HGETALL" "post:135"
1655152183.527153 [0 127.0.0.1:49337] "HGETALL" "post:113"
1655152183.527327 [0 127.0.0.1:49337] "HGETALL" "post:926"
1655152183.527528 [0 127.0.0.1:49337] "HGETALL" "post:866"
1655152183.527698 [0 127.0.0.1:49337] "HGETALL" "post:757"
1655152183.527859 [0 127.0.0.1:49337] "HGETALL" "post:691"
1655152183.528006 [0 127.0.0.1:49337] "HGETALL" "post:198"
1655152183.528207 [0 127.0.0.1:49337] "HGETALL" "post:59"
1655152183.528386 [0 127.0.0.1:49337] "HGETALL" "post:358"
1655152183.528545 [0 127.0.0.1:49337] "HGETALL" "post:975"
1655152183.528725 [0 127.0.0.1:49337] "HGETALL" "post:888"
1655152183.528857 [0 127.0.0.1:49337] "HGETALL" "user:10"
1655152183.528984 [0 127.0.0.1:49337] "HGETALL" "user:10"
1655152183.529097 [0 127.0.0.1:49337] "HGETALL" "user:9"
1655152183.529244 [0 127.0.0.1:49337] "HGETALL" "user:8"
1655152183.529401 [0 127.0.0.1:49337] "HGETALL" "user:7"
1655152183.529526 [0 127.0.0.1:49337] "HGETALL" "user:4"
1655152183.529667 [0 127.0.0.1:49337] "HGETALL" "user:9"
1655152183.529862 [0 127.0.0.1:49337] "HGETALL" "user:9"
1655152183.529991 [0 127.0.0.1:49337] "HGETALL" "user:8"
1655152183.530206 [0 127.0.0.1:49337] "HGETALL" "user:8"
1655152183.530339 [0 127.0.0.1:49337] "HGETALL" "user:2"
1655152183.530463 [0 127.0.0.1:49337] "SCAN" "640" "MATCH" "post:*" "TYPE" "hash"
1655152183.530641 [0 127.0.0.1:49337] "HGETALL" "post:836"
1655152183.530756 [0 127.0.0.1:49337] "HGETALL" "post:474"
1655152183.530866 [0 127.0.0.1:49337] "HGETALL" "post:391"
1655152183.530967 [0 127.0.0.1:49337] "HGETALL" "post:388"
1655152183.531084 [0 127.0.0.1:49337] "HGETALL" "post:240"
1655152183.531183 [0 127.0.0.1:49337] "HGETALL" "post:239"
1655152183.531287 [0 127.0.0.1:49337] "HGETALL" "post:208"
1655152183.531427 [0 127.0.0.1:49337] "HGETALL" "post:893"
1655152183.531560 [0 127.0.0.1:49337] "HGETALL" "post:503"
1655152183.531665 [0 127.0.0.1:49337] "HGETALL" "post:687"
1655152183.531767 [0 127.0.0.1:49337] "HGETALL" "post:651"
1655152183.531868 [0 127.0.0.1:49337] "HGETALL" "user:6"
1655152183.531988 [0 127.0.0.1:49337] "HGETALL" "user:10"
1655152183.532101 [0 127.0.0.1:49337] "HGETALL" "user:3"
1655152183.532202 [0 127.0.0.1:49337] "HGETALL" "user:10"
1655152183.532320 [0 127.0.0.1:49337] "HGETALL" "user:9"
1655152183.532429 [0 127.0.0.1:49337] "HGETALL" "user:7"
1655152183.532594 [0 127.0.0.1:49337] "HGETALL" "user:1"
1655152183.532730 [0 127.0.0.1:49337] "HGETALL" "user:10"
1655152183.532850 [0 127.0.0.1:49337] "HGETALL" "user:6"
```

We see that `qlc` scans as many posts as needed and then looks up user data for each. This can be done because we provide an appropriate lookup function.

Now we can iterate through the whole table in constant memory. However, with the lookup join _in our setup_, we make more Redis operations. We fetch the same users over and over for different posts.

Finally, we do not verify keys passed to the lookup function against the key pattern for simplicity.

## Conclusion

We tried out lazy sequence support in Elixir and Erlang.

Elixir provides the powerful [`Stream`](https://hexdocs.pm/elixir/Stream.html) module. It has many functions for creating and composing streams. Moreover, the API is clean and understandable.

Erlang provides the `qlc` module. It has a more sophisticated API, and some of its capabilities are available only at compile time. On the other hand, `qlc` can upgrade lazy sequences to _tables_ so that we can treat them as tables in a tiny built-in relational database, i.e., perform different kinds of joins, etc.
