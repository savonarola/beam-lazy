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

  def key_values(client, pattern \\ "*") do
    client
    |> keys(pattern, "hash")
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
