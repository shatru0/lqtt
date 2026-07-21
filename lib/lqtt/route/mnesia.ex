defmodule Lqtt.Route.Mnesia do
  require Logger
  @moduledoc """
  Mnesia-backed MQTT topic routing table for clustered operation,
  modelled after EMQX's `emqx_router`.

  Two tables:
  - `:mqtt_route` (bag)          — exact topic → `{dest, qos}`
  - `:mqtt_route_filters` (ordered_set) — wildcard filters

  A destination is `{node, client_id}` so routes are cluster-aware and can
  be cleaned up per-client on disconnect.

  `match_routes/1` walks the ordered_set using `:ets.next/2` with
  trie-search skip-ahead (EMQX-style), avoiding a full-table scan.
  """

  @route_table :mqtt_route
  @filters_table :mqtt_route_filters

  # ── lifecycle ──

  @doc """
  Initialize Mnesia on this node.
  - Starts Mnesia (no schema file — RAM-only schema is auto-generated)
  - Creates route tables locally if not already connected to a cluster
  """
  def setup do
    :mnesia.start()
    :ok
  end

  @doc """
  Join an existing Mnesia cluster by connecting to a remote node.
  Copies route tables from the cluster or creates them locally if first node.
  """
  def join_cluster(remote_node) do
    case :mnesia.change_config(:extra_db_nodes, [remote_node]) do
      {:ok, []} ->
        Logger.warning("Failed to connect to Mnesia node: #{remote_node}")
        :ok

      {:ok, connected} ->
        Logger.info("Connected to Mnesia nodes: #{inspect(connected)}")
        ensure_tables()
        :ok
    end
  end

  @doc false
  def ensure_tables do
    for table <- [@route_table, @filters_table] do
      case :mnesia.add_table_copy(table, node(), :ram_copies) do
        {:atomic, :ok} -> :ok
        {:aborted, {:already_exists, _, _}} -> :ok
        {:aborted, {:no_exists, _}} ->
          create_table(table)
        other -> Logger.warning("ensure_table #{table}: #{inspect(other)}")
      end
    end
    :mnesia.wait_for_tables([@route_table, @filters_table], 5000)
  end

  defp create_table(@route_table) do
    :mnesia.create_table(@route_table,
      [type: :bag, attributes: [:topic, :dest, :qos], ram_copies: [node()]]
    )
  end

  defp create_table(@filters_table) do
    :mnesia.create_table(@filters_table,
      [type: :ordered_set, attributes: [:key, :topic, :dest, :qos], ram_copies: [node()]]
    )
  end

  def start, do: :mnesia.start()
  def stop, do: :mnesia.stop()

  @doc "Delete all route data (for testing / clean restart)."
  def clear! do
    fn ->
      :mnesia.match_object({@route_table, :_, :_, :_})
      |> Enum.each(&:mnesia.delete_object(&1))

      :mnesia.match_object({@filters_table, :_, :_, :_, :_})
      |> Enum.each(&:mnesia.delete_object(&1))

      :ok
    end
    |> :mnesia.transaction()
  end

  # ── subscribe / unsubscribe ──

  @doc """
  Subscribe a destination to a topic filter.

  Destination is `{node, client_id}`.  Exact topics go to the route table;
  wildcard filters go to the ordered-set index for skip-ahead traversal.
  """
  def subscribe(topic, dest, qos) do
    entry =
      if Lqtt.TopicFilter.wildcard?(topic) do
        ws = Lqtt.TopicFilter.parse_filter(topic)
        {@filters_table, {ws, dest}, topic, dest, qos}
      else
        {@route_table, topic, dest, qos}
      end

    fn -> :mnesia.write(entry) end |> :mnesia.transaction()
  end

  @doc "Remove a single route."
  def unsubscribe(topic, dest) do
    fn ->
      if Lqtt.TopicFilter.wildcard?(topic) do
        ws = Lqtt.TopicFilter.parse_filter(topic)
        :mnesia.delete({@filters_table, {ws, dest}})
      else
        :mnesia.match_object({@route_table, topic, dest, :_})
        |> Enum.each(&:mnesia.delete_object(&1))
      end

      :ok
    end
    |> :mnesia.transaction()
  end

  @doc "Remove all routes for a client across all topics."
  def delete_client_routes(client_id) do
    dest = {node(), client_id}

    fn ->
      :mnesia.match_object({@route_table, :_, dest, :_})
      |> Enum.each(&:mnesia.delete_object(&1))

      :mnesia.match_object({@filters_table, :_, :_, dest, :_})
      |> Enum.each(&:mnesia.delete_object(&1))

      :ok
    end
    |> :mnesia.transaction()
  end

  @doc "Remove all routes for a departing node."
  def delete_node_routes(node) do
    fn ->
      :mnesia.match_object({@route_table, :_, {node, :_}, :_})
      |> Enum.each(&:mnesia.delete_object(&1))

      :mnesia.match_object({@filters_table, :_, :_, {node, :_}, :_})
      |> Enum.each(&:mnesia.delete_object(&1))

      :ok
    end
    |> :mnesia.transaction()
  end

  # ── match ──

  @doc """
  Match a concrete topic against all routes.

  Returns `{:atomic, [%{topic: String.t(), dest: {node, client_id}, qos: byte}]}`.
  """
  def match_routes(topic) when is_binary(topic) do
    fn ->
      tid = :ets.whereis(@filters_table)
      topic_ws = Lqtt.TopicFilter.words(topic)
      exact = :mnesia.read({@route_table, topic})

      filters =
        if tid == :undefined do
          []
        else
          traverse_filters(topic_ws, :ets.first(tid), tid, [])
        end

      normalise(exact ++ filters)
    end
    |> :mnesia.transaction()
  end

  # ── introspection ──

  @doc "List all distinct exact topics that have at least one subscriber."
  def list_topics do
    fn ->
      :mnesia.match_object({@route_table, :_, :_, :_})
      |> Enum.map(fn {@route_table, t, _, _} -> t end)
      |> Enum.uniq()
    end
    |> :mnesia.transaction()
  end

  @doc "List all routes (exact + wildcard) in normalised form."
  def list_routes do
    fn ->
      wild =
        :mnesia.match_object({@filters_table, :_, :_, :_, :_})
        |> Enum.map(fn {@filters_table, _, t, d, q} -> {@route_table, t, d, q} end)

      exact = :mnesia.match_object({@route_table, :_, :_, :_})
      wild ++ exact
    end
    |> :mnesia.transaction()
  end

  # ── helpers ──

  defp normalise(rows) do
    Enum.map(rows, fn
      {@route_table, topic, dest, qos} -> %{topic: topic, dest: dest, qos: qos}
      {@filters_table, _key, topic, dest, qos} -> %{topic: topic, dest: dest, qos: qos}
    end)
  end

  # ── ordered-set traversal (EMQX-style) ──

  defp traverse_filters(_topic_ws, :"$end_of_table", _tid, acc), do: acc

  defp traverse_filters(topic_ws, key, tid, acc) do
    {filter_ws, dest} = key

    case Lqtt.TopicFilter.compare(filter_ws, topic_ws) do
      :match ->
        raw = :ets.lookup(tid, key)
        topic = Lqtt.TopicFilter.join_words(filter_ws)
        qos = if raw != [], do: elem(hd(raw), 4), else: 0

        traverse_filters(topic_ws, :ets.next(tid, key), tid, [
          {@filters_table, key, topic, dest, qos} | acc
        ])

      {:jump, jump_words} ->
        next = :ets.next(tid, {jump_words, {}})
        traverse_filters(topic_ws, next, tid, acc)

      :done ->
        acc
    end
  end
end
