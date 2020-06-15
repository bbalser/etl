defmodule Etl do
  @type stage :: Supervisor.child_spec() | {module(), arg :: term()} | module()
  @type dictionary :: term()

  defstruct source: nil,
            destination: nil,
            transformations: [],
            stages: [],
            pids: [],
            subscriptions: []

  def run(opts) do
    source = Keyword.fetch!(opts, :source)
    destination = Keyword.fetch!(opts, :destination)
    _dictionary = Keyword.fetch!(opts, :dictionary)
    _transformations = Keyword.get(opts, :transformations, [])

    stages = Etl.Source.stages(source) ++ Etl.Destination.stages(destination)

    pids =
      stages
      |> Enum.map(&DynamicSupervisor.start_child(Etl.DynamicSupervisor, &1))
      |> Enum.map(fn {:ok, pid} -> pid end)

    subscriptions =
      pids
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.map(fn [a, b] -> GenStage.sync_subscribe(b, to: a) end)
      |> Enum.map(fn {:ok, sub} -> sub end)

    %__MODULE__{
      source: source,
      destination: destination,
      stages: stages,
      pids: pids,
      subscriptions: subscriptions
    }
  end

  @spec await(%__MODULE__{}) :: :ok | :timeout
  def await(%__MODULE__{} = etl) do
  end

  @spec done?(%__MODULE__{}) :: boolean()
  def done?(%__MODULE__{} = etl) do
  end

end
