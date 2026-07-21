defmodule Lqtt.Authenticator do
  def allow_all, do: fn _username, _password -> true end

  def map_auth(users \\ %{}) do
    fn username, password ->
      case Map.fetch(users, username) do
        {:ok, pass} -> pass == password
        :error -> false
      end
    end
  end
end
