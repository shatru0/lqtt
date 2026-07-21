defmodule Lqtt.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    connect_peers()
    Lqtt.Route.Mnesia.setup()
    join_peers()

    port = String.to_integer(System.get_env("PORT", "1883"))

    children = [
      Lqtt.Cluster,
      {Lqtt.Server, port: port}
    ]

    opts = [strategy: :one_for_one, name: Lqtt.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp connect_peers do
    case System.get_env("PEER_NODES") do
      nil -> :ok
      peers ->
        for peer <- String.split(peers, ",", trim: true) do
          peer_atom = String.to_atom(peer)
          Logger.info("Connecting to peer node: #{peer}")
          case Node.connect(peer_atom) do
            true -> Logger.info("Connected to peer: #{peer}")
            false -> Logger.warning("Failed to connect to peer: #{peer}")
          end
        end
    end
  end

  defp join_peers do
    case Node.list() do
      [] ->
        Lqtt.Route.Mnesia.ensure_tables()
      nodes ->
        for node <- nodes do
          Lqtt.Route.Mnesia.join_cluster(node)
        end
    end
  end

end
