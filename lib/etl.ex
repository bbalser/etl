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
      error_handler: error_handler,
      dynamic_supervisor: Keyword.get(opts, :dynamic_supervisor, Etl.DynamicSupervisor)
    }

    stages =
      source_stages(source, context) ++
        transformation_stages(transformations, context) ++
        destination_stages(destination, context)

    {pids, subscriptions} = Etl.Initializer.start(stages, context)

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

  @spec ack([Etl.Message.t()]) :: :ok
  def ack(messages) do
    Enum.group_by(messages, fn %{acknowledger: {mod, ref, _data}} -> {mod, ref} end)
    |> Enum.map(&group_by_status/1)
    |> Enum.each(fn {{mod, ref}, pass, fail} -> mod.ack(ref, pass, fail) end)
  end

  defp group_by_status({key, messages}) do
    {pass, fail} =
      Enum.reduce(messages, {[], []}, fn
        %{status: :ok} = msg, {pass, fail} ->
          {[msg | pass], fail}

        msg, {pass, fail} ->
          {pass, [msg | fail]}
      end)

    {key, Enum.reverse(pass), Enum.reverse(fail)}
  end

  defp source_stages(source, context) do
    source
    |> Etl.Source.stages(context)
    |> Enum.map(&intercept/1)
  end

  defp transformation_stages(transformations, context) do
    transformations
    |> Enum.map(fn transformation -> {Etl.Transformation.stage_or_function(transformation), transformation} end)
    |> Enum.chunk_by(fn {type, _} -> type end)
    |> Enum.map(&map_functions_or_stages(&1, context))
    |> List.flatten()
    |> Enum.map(&intercept/1)
  end

  defp destination_stages(destination, context) do
    post_processor = fn events -> Etl.ack(events) end

    {last, list} =
      destination
      |> Etl.Destination.stages(context)
      |> List.pop_at(-1)

    Enum.map(list, &intercept/1) ++ [intercept(last, post_process: post_processor)]
  end

  defp map_functions_or_stages([{:function, _} | _] = chunk, context) do
    Enum.map(chunk, fn {_type, transformation} -> transformation end)
    |> create_transformation_function_stage(context)
  end

  defp map_functions_or_stages([{:stage, _} | _] = chunk, context) do
    Enum.reduce(chunk, [], fn {_type, transformation}, buffer ->
      buffer ++ Etl.Transformation.stages(transformation, context)
    end)
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

  defp intercept(child_spec, opts \\ [])

  defp intercept({module, args}, opts) do
    interceptor_opts = Keyword.merge(opts, stage: module, args: args)
    {Etl.Stage.Interceptor, interceptor_opts}
  end

  defp intercept(module, opts) do
    interceptor_opts = Keyword.merge(opts, stage: module)
    {Etl.Stage.Interceptor, interceptor_opts}
  end
end
