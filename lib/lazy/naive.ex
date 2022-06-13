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
