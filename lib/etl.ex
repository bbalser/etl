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
    # _dictionary = Keyword.fetch!(opts, :dictionary)
    transformations = Keyword.get(opts, :transformations, [])

    context = %Etl.Context{
      min_demand: Keyword.get(opts, :min_demand, 500),
      max_demand: Keyword.get(opts, :max_demand, 1000)
    }

    stages =
      Etl.Source.stages(source, context) ++
        transformation_stages(transformations, context) ++
        Etl.Destination.stages(destination, context)

    pids = start_stages(stages)
    subscriptions = setup_pipeline(pids, context)

    %__MODULE__{
      source: source,
      destination: destination,
      stages: stages,
      pids: pids,
      subscriptions: subscriptions
    }
  end

  @spec await(%__MODULE__{}) :: :ok | :timeout
  def await(%__MODULE__{} = etl, opts \\ []) do
    delay = Keyword.get(opts, :delay, 500)
    timeout = Keyword.get(opts, :timeout, 10_000)

    do_await(etl, delay, timeout, 0)
  end

  @spec done?(%__MODULE__{}) :: boolean()
  def done?(%__MODULE__{} = etl) do
    Enum.all?(etl.pids, fn pid -> Process.alive?(pid) == false end)
  end

  defp start_stages(stages) do
    stages
    |> Enum.map(&DynamicSupervisor.start_child(Etl.DynamicSupervisor, &1))
    |> Enum.map(fn {:ok, pid} -> pid end)
  end

  defp setup_pipeline(pids, context) do
    pids
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [a, b] ->
      GenStage.sync_subscribe(b,
        to: a,
        max_demand: context.max_demand,
        min_demand: context.min_demand
      )
    end)
    |> Enum.map(fn {:ok, sub} -> sub end)
  end

  defp transformation_stages(transformations, context) do
    transformations
    |> Enum.map(fn transformation ->
      {Etl.Transformation.stage_or_function(transformation), transformation}
    end)
    |> Enum.chunk_by(fn {type, _} -> type end)
    |> Enum.map(fn
      [{:function, _} | _] = chunk ->
        transformations = Enum.map(chunk, fn {_type, transformation} -> transformation end)
        create_transformation_function_stage(transformations, context)

      [{:stage, _} | _] = chunk ->
        Enum.reduce(chunk, [], fn {_type, transformation}, buffer ->
          buffer ++ Etl.Transformation.stages(transformation, context)
        end)
    end)
    |> List.flatten()
  end

  defp create_transformation_function_stage(transformations, context) do
    functions = Enum.map(transformations, &Etl.Transformation.function(&1, context))
    [{Etl.Transform.Stage, functions: functions}]
  end

  defp do_await(_etl, _delay, timeout, elapsed) when elapsed >= timeout do
    :timeout
  end

  defp do_await(etl, delay, timeout, elapsed) do
    case done?(etl) do
      true ->
        :ok

      false ->
        Process.sleep(delay)
        do_await(etl, delay, timeout, elapsed + delay)
    end
  end
end
