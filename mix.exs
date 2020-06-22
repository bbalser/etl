defmodule Etl.MixProject do
  use Mix.Project

  def project do
    [
      app: :etl,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Etl.Application, []}
    ]
  end

  defp deps do
    [
      {:gen_stage, "~> 1.0"},
      {:brex_result, "~> 0.4.0"},
      {:timex, "~> 3.6"},
      {:dialyxir, "~> 1.0.0-rc.7", only: [:dev], runtime: false},
      {:placebo, "~> 1.2", only: [:dev, :test]},
      {:checkov, "~> 1.0", only: [:dev, :test]}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
