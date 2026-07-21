defmodule Lqtt.Route.MnesiaTest do
  use ExUnit.Case

  setup do
    Lqtt.Route.Mnesia.setup()
    Lqtt.Route.Mnesia.clear!()
    :ok
  end

  test "subscribe and match exact topic" do
    Lqtt.Route.Mnesia.subscribe("a/b/c", {:node1, "client1"}, 1)
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/b/c")
    assert length(routes) == 1
    assert hd(routes).topic == "a/b/c"
    assert hd(routes).dest == {:node1, "client1"}
    assert hd(routes).qos == 1
  end

  test "exact topic does not match different topic" do
    Lqtt.Route.Mnesia.subscribe("a/b/c", {:node1, "client1"}, 1)
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("x/y/z")
    assert routes == []
  end

  test "subscribe and match wildcard plus" do
    Lqtt.Route.Mnesia.subscribe("a/+/c", {:node1, "client1"}, 1)
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/x/c")
    assert length(routes) == 1
    assert hd(routes).topic == "a/+/c"
    assert hd(routes).dest == {:node1, "client1"}
  end

  test "subscribe and match wildcard hash" do
    Lqtt.Route.Mnesia.subscribe("a/#", {:node1, "client1"}, 2)
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/b/c")
    assert length(routes) == 1
    assert hd(routes).topic == "a/#"
    assert hd(routes).dest == {:node1, "client1"}
    assert hd(routes).qos == 2
  end

  test "bare hash matches everything" do
    Lqtt.Route.Mnesia.subscribe("#", {:node1, "client1"}, 0)
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("anything/at/all")
    assert length(routes) == 1
  end

  test "unsubscribe removes route" do
    Lqtt.Route.Mnesia.subscribe("a/b/c", {:node1, "client1"}, 1)
    Lqtt.Route.Mnesia.unsubscribe("a/b/c", {:node1, "client1"})
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/b/c")
    assert routes == []
  end

  test "unsubscribe wildcard removes route" do
    Lqtt.Route.Mnesia.subscribe("a/+/c", {:node1, "client1"}, 1)
    Lqtt.Route.Mnesia.unsubscribe("a/+/c", {:node1, "client1"})
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/x/c")
    assert routes == []
  end

  test "multiple subscribers on same topic" do
    Lqtt.Route.Mnesia.subscribe("a/b/c", {:node1, "c1"}, 0)
    Lqtt.Route.Mnesia.subscribe("a/b/c", {:node2, "c2"}, 1)
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/b/c")
    assert length(routes) == 2
  end

  test "multiple subscribers with wildcards" do
    Lqtt.Route.Mnesia.subscribe("a/+/c", {:node1, "c1"}, 0)
    Lqtt.Route.Mnesia.subscribe("a/#", {:node2, "c2"}, 1)
    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/x/c")
    assert length(routes) == 2
  end

  test "delete client routes" do
    Lqtt.Route.Mnesia.subscribe("a/b/c", {node(), "client1"}, 1)
    Lqtt.Route.Mnesia.subscribe("x/y/z", {node(), "client1"}, 0)
    Lqtt.Route.Mnesia.subscribe("a/b/c", {node(), "client2"}, 1)

    Lqtt.Route.Mnesia.delete_client_routes("client1")

    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/b/c")
    assert length(routes) == 1
    assert hd(routes).dest == {node(), "client2"}
  end

  test "delete node routes removes only routes for that node" do
    n1 = :"testnode@host1"
    n2 = :"testnode@host2"
    Lqtt.Route.Mnesia.subscribe("a/b/c", {n1, "c1"}, 1)
    Lqtt.Route.Mnesia.subscribe("a/b/c", {n2, "c2"}, 1)

    Lqtt.Route.Mnesia.delete_node_routes(n1)

    {:atomic, routes} = Lqtt.Route.Mnesia.match_routes("a/b/c")
    assert length(routes) == 1
    assert hd(routes).dest == {n2, "c2"}
  end

  test "list topics" do
    Lqtt.Route.Mnesia.subscribe("a/b/c", {node(), "c1"}, 0)
    Lqtt.Route.Mnesia.subscribe("a/b/c", {:other_node, "c2"}, 0)
    Lqtt.Route.Mnesia.subscribe("x/y", {node(), "c1"}, 0)

    {:atomic, topics} = Lqtt.Route.Mnesia.list_topics()
    assert length(topics) == 2
    assert "a/b/c" in topics
    assert "x/y" in topics
  end

  test "list routes" do
    Lqtt.Route.Mnesia.subscribe("a/b/c", {:node1, "c1"}, 0)
    Lqtt.Route.Mnesia.subscribe("a/#", {:node2, "c2"}, 1)

    {:atomic, routes} = Lqtt.Route.Mnesia.list_routes()
    assert length(routes) == 2
  end
end
