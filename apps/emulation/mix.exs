defmodule CS3033Emu.MixProject do
    use Mix.Project
  
    def project do
      [
        app: :emulation,
        version: "0.1.0",
        build_path: "../../_build",
        config_path: "../../config/config.exs",
        deps_path: "../../deps",
        lockfile: "../../mix.lock",
        elixir: "~> 1.10",
        start_permanent: Mix.env() == :prod,
        deps: deps(),
        dializer: [
          plt_add_deps: :apps_direct
        ],
        # Docs
        name: "CS2621 Emulator",
        source_url:
          "https://github.com/nyu-distributed-systems/fa20-lab1-code/tree/master/apps/emulation",
        homepage_url: "https://cs.nyu.edu/~apanda/classes/fa20/emdocs/",
        docs: [
          # The main page in the docs
          main: "Emulation"
        ]
      ]
    end
  
    # Run "mix help compile.app" to learn about applications.
    def application do
      [
        extra_applications: [:logger]
      ]
    end
  
    # Run "mix help deps" to learn about dependencies.
    defp deps do
      [
        {:statistics, "~> 0.6.2"},
        {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
        {:credo, "~> 1.4", only: [:dev, :test], runtime: false},
        {:ex_doc, "~> 0.22", only: :dev, runtime: false}
      ]
    end
  end