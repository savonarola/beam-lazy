defmodule Mix.Tasks.LazySeed do
  @moduledoc "Seed sample data"
  use Mix.Task

  @shortdoc "Seed sample data"
  def run(_) do
    Mix.Task.run("app.start")
    shell = Mix.shell()

    {:ok, conn} = Redix.start_link()

    shell.info("Making cleanup...")

    for pattern <- ["user:*", "post:*"] do
      for key <- Redix.command!(conn, ["KEYS", pattern]) do
        Redix.command!(conn, ["DEL", key])
      end
    end

    shell.info("Cleanup done")

    n_users = 10
    n_posts = 1000

    shell.info("Creating #{n_users} users...")

    for id <- 1..n_users do
      name = Faker.Person.name()
      user = ["name", name, "id", to_string(id)]
      Redix.command!(conn, ["HSET", "user:#{id}" | user])
    end

    shell.info("Created")

    shell.info("Creating #{n_posts} posts...")

    for id <- 1..n_posts do
      user_id = :rand.uniform(n_users)
      text = Faker.Lorem.sentence(4..10)
      post = ["text", text, "user_id", to_string(user_id), "id", to_string(id)]
      Redix.command!(conn, ["HSET", "post:#{id}" | post])
    end

    shell.info("Created")
  end
end
