defmodule Natch.MixProject do
  use Mix.Project

  @version "0.2.0"

  def project do
    [
      app: :natch,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Elixir client for ClickHouse database via FINE + clickhouse-cpp (native TCP)",
      package: package(),
      docs: docs(),
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_targets: ["all"],
      make_clean: ["clean"],
      make_cwd: "native/natch_fine",
      # Precompiler configuration
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url:
        "https://github.com/Intellection/natch/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_nif_versions: [versions: ["2.17"]],
      make_nif_filename: "natch_fine",
      make_precompiler_priv_paths: ["natch_fine.*"]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/Intellection/natch",
        "Changelog" => "https://github.com/Intellection/natch/blob/main/CHANGELOG.md"
      },
      files:
        ~w(lib priv native/natch_fine/src native/natch_fine/CMakeLists.txt native/natch_fine/Makefile native/clickhouse-cpp .formatter.exs mix.exs README.md LICENSE THIRD_PARTY_NOTICES.md CHANGELOG.md checksum*.exs)
    ]
  end

  defp docs do
    [
      main: "Natch",
      extras: ["README.md"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Natch.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:fine, "~> 0.1.0"},
      {:elixir_make, "~> 0.6", runtime: false},
      {:cc_precompiler, "~> 0.1.0", runtime: false},
      {:jason, "~> 1.4"},
      {:decimal, "~> 2.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:benchee, "~> 1.3", only: :dev},
      {:benchee_html, "~> 1.0", only: :dev},
      {:pillar, "~> 0.31", only: :dev}
    ]
  end
end
