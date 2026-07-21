defmodule Lqtt.TopicFilter do
  @moduledoc """
  MQTT topic filter parsing, matching, and trie-based comparison (EMQX-style).

  Provides the core filter primitives used by the in-memory trie
  (`Lqtt.TopicManager`), ACL checks (`Lqtt.ACL`), and the Mnesia-backed
  routing table for clustered operation.
  """

  @typedoc "A parsed filter: literal strings, `:+` for single-level, or `:h` for multi-level."
  @type parsed_filter :: [String.t() | :+ | :h]

  @typedoc "Return value of `compare/2`."
  @type compare_result :: :match | :done | {:jump, [String.t() | :+ | :h]}

  @doc """
  Parse a topic filter string into a word list.
  `+` becomes `:+`, `#` becomes `:h`.
  """
  @spec parse_filter(String.t()) :: parsed_filter
  def parse_filter(topic) do
    String.split(topic, "/")
    |> Enum.map(fn
      "+" -> :+
      "#" -> :h
      w -> w
    end)
  end

  @doc "Split a concrete topic into its slash-separated words."
  @spec words(String.t()) :: [String.t()]
  def words(topic), do: String.split(topic, "/")

  @doc "Join a parsed filter back into a topic string."
  @spec join_words(parsed_filter()) :: String.t()
  def join_words(ws) do
    ws
    |> Enum.map(fn
      :+ -> "+"
      :h -> "#"
      w -> w
    end)
    |> Enum.join("/")
  end

  @doc "Returns `true` if the topic string contains `+` or `#`."
  @spec wildcard?(String.t()) :: boolean()
  def wildcard?(topic), do: String.contains?(topic, ["+", "#"])

  @doc """
  Returns `true` if a concrete topic matches a filter string.

  ## Examples

      iex> Lqtt.TopicFilter.match?("sensor/room1/temp", "sensor/+/temp")
      true

      iex> Lqtt.TopicFilter.match?("sensor/room1/humidity", "sensor/+/temp")
      false

      iex> Lqtt.TopicFilter.match?("a/b/c", "a/#")
      true
  """
  @spec match?(String.t(), String.t()) :: boolean()
  def match?(topic, filter) do
    compare(parse_filter(filter), words(topic)) == :match
  end

  @doc """
  EMQX-style trie comparison between a parsed filter and a topic word list.

  Returns `:match` if the topic matches the filter, `:done` if the filter
  can never match (lexicographically past the topic), or
  `{:jump, jump_words}` indicating the next filter key to seek to in an
  ordered_set traversal (skipping over filters that can't match).
  """
  @spec compare(parsed_filter(), [String.t()]) :: compare_result()
  def compare(filter_ws, topic_ws) do
    do_compare(filter_ws, topic_ws, filter_ws, 0, nil)
  end

  # ── trie comparison (EMQX-style) ──

  defp do_compare([:h | _], _, _orig, _pos, _bt), do: :match

  defp do_compare([], [], _orig, _pos, _bt), do: :match
  defp do_compare([], [_ | _], _orig, _pos, _bt), do: :done
  defp do_compare([_ | _], [], _orig, _pos, _bt), do: :done

  defp do_compare([:+ | ft], [t | tt], orig, pos, _bt) do
    do_compare(ft, tt, orig, pos + 1, {pos, t})
  end

  defp do_compare([f | ft], [t | tt], orig, pos, bt) when f == t do
    do_compare(ft, tt, orig, pos + 1, bt)
  end

  defp do_compare([f | _ft], [t | _tt], orig, _pos, {bt_pos, bt_word}) when f > t do
    {:jump, Enum.take(orig, bt_pos) ++ [bt_word]}
  end

  defp do_compare([f | _ft], [t | _tt], orig, pos, _bt) when f < t do
    {:jump, Enum.take(orig, pos) ++ [t]}
  end

  defp do_compare([_f | _ft], [_t | _tt], _orig, _pos, nil), do: :done
end
