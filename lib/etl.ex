defmodule Etl do
  require Logger
  @type stage :: Supervisor.child_spec() | {module(), arg :: term()} | module()
  @type dictionary :: term()

  @type t :: %__MODULE__{
          source: Etl.Source.t(),
          destination: Etl.Destination.t(),
          transformations: [Etl.Transformation.t()],
          stages: [Etl.stage()],
          pids: [pid],
          subscriptions: [GenStage.subscription_tag()]
        }

  defstruct source: nil,
            destination: nil,
            transformations: [],
            stages: [],
            pids: [],
            subscriptions: []

  @type run_opts :: [
          source: Etl.Source.t(),
          transformations: [Etl.Transformation.t()],
          destination: Etl.Destination.t(),
          dictionary: Etl.Dictionary.t(),
          error_handler: (event :: term(), reason :: term() -> no_return()),
          max_demand: pos_integer(),
          min_demand: pos_integer()
        ]

  @spec run(run_opts) :: t
  def run(opts) do
    source = Keyword.fetch!(opts, :source)
    destination = Keyword.fetch!(opts, :destination)
    dictionary = Keyword.get(opts, :dictionary)
    transformations = Keyword.get(opts, :transformations, [])

    error_handler =
      Keyword.get(opts, :error_handler, fn event, reason ->
        Logger.debug(fn ->
          "Event #{inspect(event)} has been evicted, reason: #{inspect(reason)}"
        end)
      end)

    context = %Etl.Context{
      dictionary: dictionary,
      min_demand: Keyword.get(opts, :min_demand, 500),
      max_demand: Keyword.get(opts, :max_demand, 1000),
      error_handler: error_handler
    }

    source_stages =
      Etl.Source.stages(source, context)
      |> Enum.map(&intercept/1)

    stages =
      source_stages ++
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

  @spec await(t) :: :ok | :timeout
  def await(%__MODULE__{} = etl, opts \\ []) do
    delay = Keyword.get(opts, :delay, 500)
    timeout = Keyword.get(opts, :timeout, 10_000)

    do_await(etl, delay, timeout, 0)
  end

  @spec done?(t) :: boolean()
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
        Enum.map(chunk, fn {_type, transformation} -> transformation end)
        |> create_transformation_function_stage(context)

      [{:stage, _} | _] = chunk ->
        Enum.reduce(chunk, [], fn {_type, transformation}, buffer ->
          buffer ++ Etl.Transformation.stages(transformation, context)
        end)
    end)
    |> List.flatten()
  end

  defp create_transformation_function_stage(transformations, context) do
    functions = Enum.map(transformations, &Etl.Transformation.function(&1, context))
    [{Etl.Transform.Stage, functions: functions, context: context}]
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

  defp intercept({module, args}) do
    {Etl.Stage.Interceptor, stage: module, args: args}
  end

  defp intercept(module) do
    {Etl.Stage.Interceptor, stage: module}
  end
end
