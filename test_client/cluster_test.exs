# Run: elixir --sname lqtt_test --cookie lqtt -r test_client/cluster_test.exs

defmodule ClusterTest do
  @node1_port 18833
  @node2_port 18834

  def run do
    IO.puts("=== Cluster Test ===")

    # Start node1
    IO.puts("Starting node1 on port #{@node1_port}...")
    {:ok, node1, _} = :peer.start(%{name: :'lqtt1@127.0.0.1', args: ~w(--cookie lqtt)})
    IO.puts("Node1: #{node1}")

    # Start node2
    IO.puts("Starting node2 on port #{@node2_port}...")
    {:ok, node2, _} = :peer.start(%{name: :'lqtt2@127.0.0.1', args: ~w(--cookie lqtt)})
    IO.puts("Node2: #{node2}")

    # Start the app on node1
    IO.puts("Starting app on node1...")
    :rpc.call(node1, Lqtt.Route.Mnesia, :setup, [])
    :rpc.call(node1, Lqtt.Application, :start, [:normal, [port: 18833]])

    # Start the app on node2 (just start Mnesia, don't create tables - they'll be copied from node1)
    IO.puts("Starting app on node2...")
    :rpc.call(node2, :mnesia, :start, [])
    :rpc.call(node2, Lqtt.Application, :start, [:normal, [port: 18834]])

    # Connect node2 to node1
    IO.puts("Connecting node2 to node1...")
    :rpc.call(node2, Node, :connect, [node1])
    :timer.sleep(1000)

    # Subscribe on node1
    IO.puts("Subscribing on node1...")
    :rpc.call(node1, Lqtt.Route.Mnesia, :subscribe, ["sensor/+/temp", {node1, "test-sub"}, 0])

    # Check routes on node1
    {:atomic, routes1} = :rpc.call(node1, Lqtt.Route.Mnesia, :list_routes, [])
    IO.puts("Routes on node1: #{inspect(routes1)}")

    # Check routes on node2
    {:atomic, routes2} = :rpc.call(node2, Lqtt.Route.Mnesia, :list_routes, [])
    IO.puts("Routes on node2: #{inspect(routes2)}")

    # Match on node2
    {:atomic, matches} = :rpc.call(node2, Lqtt.Route.Mnesia, :match_routes, ["sensor/kitchen/temp"])
    IO.puts("Match on node2: #{inspect(matches)}")

    if routes2 == routes1 do
      IO.puts("SUCCESS: Routes are shared across cluster")
    else
      IO.puts("FAIL: Routes differ between nodes")
    end
  end
end