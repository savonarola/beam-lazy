defmodule Lazy.StreamDemo do
  @moduledoc false

  def demo() do
    {:ok, conn} = Redix.start_link()

    conn
    |> Lazy.Stream.key_values("post:*")
    |> Stream.map(fn {_key, post} -> post end)
    |> Stream.filter(fn %{"text" => text} -> String.length(text) > 10 end)
    |> Enum.take(10)
  end

end
