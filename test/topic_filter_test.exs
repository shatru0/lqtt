defmodule Lqtt.TopicFilterTest do
  use ExUnit.Case

  describe "parse_filter/1" do
    test "exact topic" do
      assert Lqtt.TopicFilter.parse_filter("a/b/c") == ["a", "b", "c"]
    end

    test "single-level wildcard" do
      assert Lqtt.TopicFilter.parse_filter("a/+/c") == ["a", :+, "c"]
    end

    test "multi-level wildcard" do
      assert Lqtt.TopicFilter.parse_filter("a/#") == ["a", :h]
    end

    test "bare hash" do
      assert Lqtt.TopicFilter.parse_filter("#") == [:h]
    end

    test "bare plus" do
      assert Lqtt.TopicFilter.parse_filter("+") == [:+]
    end

    test "mixed wildcards" do
      assert Lqtt.TopicFilter.parse_filter("+/#") == [:+, :h]
      assert Lqtt.TopicFilter.parse_filter("sensor/+/temp/#") == ["sensor", :+, "temp", :h]
    end
  end

  describe "join_words/1" do
    test "exact topic" do
      assert Lqtt.TopicFilter.join_words(["a", "b", "c"]) == "a/b/c"
    end

    test "with wildcards" do
      assert Lqtt.TopicFilter.join_words(["a", :+, "c"]) == "a/+/c"
      assert Lqtt.TopicFilter.join_words(["a", :h]) == "a/#"
      assert Lqtt.TopicFilter.join_words([:+, :h]) == "+/#"
    end
  end

  describe "wildcard?/1" do
    test "exact topic" do
      refute Lqtt.TopicFilter.wildcard?("a/b/c")
    end

    test "with plus" do
      assert Lqtt.TopicFilter.wildcard?("a/+/c")
    end

    test "with hash" do
      assert Lqtt.TopicFilter.wildcard?("a/#")
    end
  end

  describe "match?/2" do
    test "exact match" do
      assert Lqtt.TopicFilter.match?("a/b/c", "a/b/c")
    end

    test "exact mismatch" do
      refute Lqtt.TopicFilter.match?("a/b/c", "a/b/d")
    end

    test "single-level wildcard matches one level" do
      assert Lqtt.TopicFilter.match?("sensor/room1/temp", "sensor/+/temp")
    end

    test "single-level wildcard rejects extra levels" do
      refute Lqtt.TopicFilter.match?("sensor/room1/temp/extra", "sensor/+/temp")
    end

    test "single-level wildcard mismatch" do
      refute Lqtt.TopicFilter.match?("sensor/room1/humidity", "sensor/+/temp")
    end

    test "bare hash matches everything" do
      assert Lqtt.TopicFilter.match?("a", "#")
      assert Lqtt.TopicFilter.match?("a/b/c", "#")
    end

    test "hash at depth" do
      assert Lqtt.TopicFilter.match?("a", "a/#")
      assert Lqtt.TopicFilter.match?("a/b", "a/#")
      assert Lqtt.TopicFilter.match?("a/b/c/d", "a/#")
      refute Lqtt.TopicFilter.match?("b", "a/#")
    end

    test "plus then hash" do
      assert Lqtt.TopicFilter.match?("a", "+/#")
      assert Lqtt.TopicFilter.match?("a/b", "+/#")
      assert Lqtt.TopicFilter.match?("a/b/c", "+/#")
    end

    test "plus at each position" do
      assert Lqtt.TopicFilter.match?("x/mid/end", "+/mid/end")
      assert Lqtt.TopicFilter.match?("start/y/end", "start/+/end")
      assert Lqtt.TopicFilter.match?("start/mid/z", "start/mid/+")
      assert Lqtt.TopicFilter.match?("start/mid/end", "start/mid/+")
      assert Lqtt.TopicFilter.match?("start/mid/end", "+/mid/end")
      assert Lqtt.TopicFilter.match?("start/mid/end", "start/+/end")
      assert Lqtt.TopicFilter.match?("start/mid/end", "start/mid/+")
    end

    test "multiple levels mismatch" do
      refute Lqtt.TopicFilter.match?("a", "a/b")
      refute Lqtt.TopicFilter.match?("a/b", "+")
      refute Lqtt.TopicFilter.match?("a/b/c", "+/+")
    end
  end

  describe "compare/2" do
    test "exact match" do
      assert Lqtt.TopicFilter.compare(["a", "b"], ["a", "b"]) == :match
    end

    test "filter exhausted before topic (partial match)" do
      assert Lqtt.TopicFilter.compare([:h], ["a", "b"]) == :match
    end

    test "hash wildcard" do
      assert Lqtt.TopicFilter.compare(["a", :h], ["a", "b", "c"]) == :match
    end

    test "plus matches one level" do
      assert Lqtt.TopicFilter.compare(["a", :+, "c"], ["a", "b", "c"]) == :match
    end

    test "topic exhausted before filter" do
      assert Lqtt.TopicFilter.compare(["a", "b"], ["a"]) == :done
    end

    test "filter word > topic word without backtrack" do
      assert Lqtt.TopicFilter.compare(["b", "c"], ["a", "d"]) == :done
    end

    test "filter word > topic word with backtrack" do
      assert Lqtt.TopicFilter.compare([:+, "c"], ["a", "d"]) ==
               {:jump, [:+, "d"]}
    end

    test "filter word < topic word" do
      assert Lqtt.TopicFilter.compare(["a", "c"], ["b"]) ==
               {:jump, ["b"]}
    end

    test "no backtrack point" do
      assert Lqtt.TopicFilter.compare(["b", "c"], ["a", "z"]) == :done
    end
  end
end
