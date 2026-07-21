defmodule Lqtt.TopicManagerTest do
  use ExUnit.Case

  test "single level wildcard + matches one level" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "sensor/+/temp", "client1", 1)

    assert length(Lqtt.TopicManager.match(tm, "sensor/room1/temp")) == 1
    assert length(Lqtt.TopicManager.match(tm, "sensor/room2/temp")) == 1
    assert length(Lqtt.TopicManager.match(tm, "sensor/room1/humidity")) == 0
    assert length(Lqtt.TopicManager.match(tm, "other/topic")) == 0
  end

  test "wildcard hash matches everything" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "#", "client1", 0)
    assert length(Lqtt.TopicManager.match(tm, "anything/at/all")) == 1
  end

  test "wildcard hash at depth" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "a/#", "client1", 0)
    assert length(Lqtt.TopicManager.match(tm, "a/b/c")) == 1
    assert length(Lqtt.TopicManager.match(tm, "b")) == 0
  end

  test "single level wildcard" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "+/+", "client1", 0)
    assert length(Lqtt.TopicManager.match(tm, "a/b")) == 1
    assert length(Lqtt.TopicManager.match(tm, "a/b/c")) == 0
  end

  test "plus at each position" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "+/mid/end", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "start/+/end", "c2", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "start/mid/+", "c3", 0)

    assert length(Lqtt.TopicManager.match(tm, "x/mid/end")) == 1
    assert length(Lqtt.TopicManager.match(tm, "start/y/end")) == 1
    assert length(Lqtt.TopicManager.match(tm, "start/mid/z")) == 1
    assert length(Lqtt.TopicManager.match(tm, "start/mid/end")) == 3
  end

  test "multiple plus wildcards" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "+/+/+", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "a/+/c", "c2", 0)

    assert length(Lqtt.TopicManager.match(tm, "x/y/z")) == 1
    assert length(Lqtt.TopicManager.match(tm, "a/b/c")) == 2
    assert length(Lqtt.TopicManager.match(tm, "a/b/c/d")) == 0
  end

  test "hash at depth" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "a/#", "c1", 0)

    assert length(Lqtt.TopicManager.match(tm, "a")) == 1
    assert length(Lqtt.TopicManager.match(tm, "a/b")) == 1
    assert length(Lqtt.TopicManager.match(tm, "a/b/c/d/e")) == 1
    assert length(Lqtt.TopicManager.match(tm, "b")) == 0
    assert length(Lqtt.TopicManager.match(tm, "b/a")) == 0
  end

  test "multiple hash subscribers" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "#", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "#", "c2", 1)
    tm = Lqtt.TopicManager.subscribe(tm, "a/#", "c3", 0)

    assert length(Lqtt.TopicManager.match(tm, "anything")) == 2
    assert length(Lqtt.TopicManager.match(tm, "a/b")) == 3
    assert length(Lqtt.TopicManager.match(tm, "x/y")) == 2
  end

  test "plus then hash" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "+/#", "c1", 0)

    assert length(Lqtt.TopicManager.match(tm, "a")) == 1
    assert length(Lqtt.TopicManager.match(tm, "a/b")) == 1
    assert length(Lqtt.TopicManager.match(tm, "a/b/c/d")) == 1
  end

  test "mixed literal plus hash" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "sensor/+/temp", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "sensor/#", "c2", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "+/status/#", "c3", 0)

    assert length(Lqtt.TopicManager.match(tm, "sensor/room1/temp")) == 2
    assert length(Lqtt.TopicManager.match(tm, "device/status/running")) == 1
    assert length(Lqtt.TopicManager.match(tm, "device/status")) == 1
    assert length(Lqtt.TopicManager.match(tm, "other/status/a/b/c")) == 1
  end

  test "unsubscribe one client" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "a/b/c", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "a/b/c", "c2", 1)
    tm = Lqtt.TopicManager.subscribe(tm, "a/#", "c1", 0)

    assert length(Lqtt.TopicManager.match(tm, "a/b/c")) == 3

    tm = Lqtt.TopicManager.unsubscribe(tm, "a/b/c", "c1")
    assert length(Lqtt.TopicManager.match(tm, "a/b/c")) == 2
  end

  test "unsubscribe hash" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "a/#", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "a/#", "c2", 1)

    tm = Lqtt.TopicManager.unsubscribe(tm, "a/#", "c1")
    subs = Lqtt.TopicManager.match(tm, "a/b")
    assert length(subs) == 1
    assert hd(subs).client_id == "c2"
  end

  test "unsubscribe plus" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "+/+", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "+/+", "c2", 1)

    tm = Lqtt.TopicManager.unsubscribe(tm, "+/+", "c1")
    subs = Lqtt.TopicManager.match(tm, "a/b")
    assert length(subs) == 1
    assert hd(subs).client_id == "c2"
  end

  test "unsubscribe all" do
    tm = Lqtt.TopicManager.new()
    tm = Lqtt.TopicManager.subscribe(tm, "a/b", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "a/#", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "+/x", "c1", 0)
    tm = Lqtt.TopicManager.subscribe(tm, "a/b", "c2", 0)

    tm = Lqtt.TopicManager.unsubscribe_all(tm, "c1")
    subs = Lqtt.TopicManager.match(tm, "a/b")
    assert length(subs) == 1
    assert hd(subs).client_id == "c2"
    assert length(Lqtt.TopicManager.match(tm, "a/b/c")) == 0
    assert length(Lqtt.TopicManager.match(tm, "y/x")) == 0
  end
end
