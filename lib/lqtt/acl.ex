defmodule Lqtt.ACL do
  def allow_all do
    fn :publish, _username, _topic -> true
       :subscribe, _username, _topic -> true
    end
  end

  def map_acl(opts \\ []) do
    publish_allowed = Keyword.get(opts, :publish_allowed, %{})
    subscribe_allowed = Keyword.get(opts, :subscribe_allowed, %{})

    fn
      :publish, username, topic ->
        filters = Map.get(publish_allowed, username, [])
        Enum.any?(filters, &match_topics(topic, &1))

      :subscribe, username, topic ->
        filters = Map.get(subscribe_allowed, username, [])
        Enum.any?(filters, &match_topics(topic, &1))
    end
  end

  defp match_topics(topic, filter) do
    topic_parts = String.split(topic, "/")
    filter_parts = String.split(filter, "/")
    do_match_levels(topic_parts, filter_parts, 0)
  end

  defp do_match_levels(_topic_parts, filter_parts, fi) when fi >= length(filter_parts) do
    false
  end

  defp do_match_levels(topic_parts, filter_parts, fi) do
    filter = Enum.at(filter_parts, fi)

    cond do
      filter == "#" ->
        true

      fi >= length(topic_parts) ->
        false

      filter == "+" ->
        do_match_levels(topic_parts, filter_parts, fi + 1)

      Enum.at(topic_parts, fi) == filter ->
        do_match_levels(topic_parts, filter_parts, fi + 1)

      true ->
        false
    end
  end
end
