defmodule Lqtt.Cluster do
  @moduledoc """
  Cluster node monitoring for Mnesia-backed MQTT routing.

  Monitors distributed Erlang node up/down events and:
  - On `:nodeup` — the JOINING node calls `Lqtt.Route.Mnesia.join_cluster/1`
    to tell Mnesia about the existing node, which auto-replicates tables.
  - On `:nodedown` — removes all routes belonging to the departed node.
  """

  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    case :net_kernel.monitor_nodes(true, node_type: :visible) do
      {:error, :not_allowed} ->
        Logger.warning("Not a distributed Erlang node — cluster monitoring disabled")
        {:ok, %{monitoring: false}}

      _ ->
        {:ok, %{monitoring: true}}
    end
  end

  @impl true
  def handle_info({:nodeup, node, _}, state) do
    Logger.info("Cluster node up: #{node}")
    Lqtt.Route.Mnesia.join_cluster(node)
    ensure_full_mesh(node)
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _}, state) do
    Logger.info("Cluster node down: #{node}")
    Lqtt.Route.Mnesia.delete_node_routes(node)
    {:noreply, state}
  end

  def handle_info({:nodeup, node}, state) do
    Logger.info("Cluster node up: #{node}")
    Lqtt.Route.Mnesia.join_cluster(node)
    ensure_full_mesh(node)
    {:noreply, state}
  end

  def handle_info({:nodedown, node}, state) do
    Logger.info("Cluster node down: #{node}")
    Lqtt.Route.Mnesia.delete_node_routes(node)
    {:noreply, state}
  end

  defp ensure_full_mesh(joined_node) do
    known = :mnesia.system_info(:running_db_nodes)
    my_node = node()
    node_list = Node.list()
    Logger.info("ensure_mesh joined=#{joined_node} known=#{inspect(known)} my=#{my_node} node_list=#{inspect(node_list)}")
    for n <- known, n != my_node, n not in node_list do
      Logger.info("Connecting to cluster node: #{n}")
      Node.connect(n)
    end
  end
end
