defmodule Lqtt.MixProject do
  use Mix.Project

  def project do
    [
      app: :lqtt,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :mnesia],
      mod: {Lqtt.Application, []}
    ]
  end

  defp deps do
    [
      {:mqttc, "~> 0.2"}
    ]
  end
end
