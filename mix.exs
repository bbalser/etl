defmodule Etl.MixProject do
  use Mix.Project

  @github "https://github.com/inhindsight/etl"

  def project do
    [
      app: :etl,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      source_url: @github,
      homepage: @github,
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: [plt_file: {:no_warn, ".dialyzer/#{System.version()}.plt"}]
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
      {:ex_doc, "~> 0.22.1", only: [:dev]},
      {:placebo, "~> 1.2", only: [:dev, :test]},
      {:checkov, "~> 1.0", only: [:dev, :test]}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      maintainers: ["Brian Balser", "Johnson Denen", "Jeff Grunewald"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => @github}
    ]
  end

  defp docs do
    [
      source_url: @github,
      extras: ["README.md"],
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end

  defp description do
    "Declarative definition of Extract/Transform/Load pipelines"
  end
end
