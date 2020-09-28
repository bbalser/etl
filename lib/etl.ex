defmodule Etl do
  require Logger
  @type stage :: Supervisor.child_spec() | {module(), arg :: term()} | module() | Etl.Stage.t()
  @type dictionary :: term()

  @type t :: %__MODULE__{
          stages: [Etl.stage()],
          pids: [pid],
          subscriptions: [GenStage.subscription_tag()]
        }

  defstruct stages: [],
            pids: [],
            subscriptions: []

  @type global_opts :: [
          min_demand: pos_integer(),
          max_demand: pos_integer(),
          dynamic_supervisor: module()
        ]

  @spec producer(stage(), keyword()) :: Etl.Pipeline.t()
  def producer(stage, opts \\ []) do
    Etl.Pipeline.new()
    |> Etl.Pipeline.add_stage(stage, opts)
  end

  @spec to(Etl.Pipeline.t(), stage(), keyword()) :: Etl.Pipeline.t()
  def to(pipeline, function, opts \\ [])

  def to(pipeline, function, opts) when is_function(function) do
    Etl.Pipeline.add_function(pipeline, function, opts)
  end

  def to(pipeline, stage, opts) do
    Etl.Pipeline.add_stage(pipeline, stage, opts)
  end

  @type partition_opts :: [
          partitions: pos_integer() | list(),
          hash: (Etl.Message.t() -> {Etl.Message.t(), partition :: term})
        ]

  @spec partition(Etl.Pipeline.t(), partition_opts) :: Etl.Pipeline.t()
  def partition(pipeline, opts) do
    Keyword.fetch!(opts, :partitions)
    Etl.Pipeline.set_partitions(pipeline, opts)
  end

  @spec broadcast(Etl.Pipeline.t(), keyword) :: Etl.Pipeline.t()
  defdelegate broadcast(pipeline, opts \\ []), to: Etl.Pipeline, as: :set_broadcast

  @spec batch(Etl.Pipeline.t(), keyword) :: Etl.Pipeline.t()
  defdelegate batch(pipeline, opts \\ []), to: Etl.Pipeline, as: :add_batch

  @spec run(Etl.Pipeline.t(), global_opts()) :: t
  def run(%Etl.Pipeline{} = pipeline, global_opts \\ []) do
    context = Etl.Pipeline.get_context(pipeline, global_opts)

    Graph.new(type: :directed)
    |> start_steps(Etl.Pipeline.steps(pipeline), context)
    |> subscribe_stages(context)
    |> create_struct()
    |> (fn etl ->
          Etl.Tree.print(etl)
          etl
        end).()
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

  defp start_steps(graph, [], _context), do: graph

  defp start_steps(graph, [step | remaining], context) do
    starter = &start_step(&1, context, remaining == [])
    tails = tails(graph)
    subscribe_strategy = get_in(step.opts, [:subscribe_strategy]) || :mesh
    dispatcher = get_dispatcher(tails)

    graph
    |> add_to_graph(tails, step, {subscribe_strategy, dispatcher}, starter)
    |> start_steps(remaining, context)
  end

  defp add_to_graph(graph, tails, step, {:mesh, {:partitions, partitions}}, starter) do
    subscription_opts = get_in(step.opts, [:subscription_opts]) || []

    Enum.reduce(partitions, graph, fn partition, g ->
      {:ok, pid} = starter.(step)

      subscription_opts = Keyword.put(subscription_opts, :partition, partition)
      g = Graph.add_vertex(g, pid, step: step, subscription_opts: subscription_opts)

      Enum.reduce(tails, g, fn tail, g ->
        Graph.add_edge(g, tail, pid)
      end)
    end)
  end

  defp add_to_graph(graph, tails, step, {:mesh, _}, starter) do
    subscription_opts = get_in(step.opts, [:subscription_opts]) || []
    count = get_in(step.opts, [:count]) || 1

    Enum.reduce(1..count, graph, fn _, g ->
      {:ok, pid} = starter.(step)

      g = Graph.add_vertex(g, pid, step: step, subscription_opts: subscription_opts)

      Enum.reduce(tails, g, fn tail, g ->
        Graph.add_edge(g, tail, pid)
      end)
    end)
  end

  defp add_to_graph(graph, tails, step, {:per_producer, _}, starter) do
    subscription_opts = get_in(step.opts, [:subscription_opts]) || []
    count = get_in(step.opts, [:count]) || 1

    Enum.reduce(tails, graph, fn tail, g ->
      Enum.reduce(1..count, g, fn _, g ->
        {:ok, pid} = starter.(step)

        Graph.add_vertex(g, pid, step: step, subscription_opts: subscription_opts)
        |> Graph.add_edge(tail, pid)
      end)
    end)
  end

  defp get_dispatcher([]), do: :default

  defp get_dispatcher([tail | _]) do
    case GenStage.call(tail, :"$dispatcher") do
      {GenStage.PartitionDispatcher, opts} ->
        partitions = Keyword.fetch!(opts, :partitions) |> to_list()
        {:partitions, partitions}

      _ ->
        :default
    end
  end

  defp subscribe_stages(graph, context) do
    Graph.postorder(graph)
    |> Enum.reduce(graph, fn pid, g ->
      v_subscription_opts = Graph.vertex_labels(g, pid) |> Keyword.get(:subscription_opts, [])

      Graph.in_neighbors(g, pid)
      |> Enum.reduce(g, fn neighbor, g ->
        subscription_opts =
          [
            to: neighbor,
            min_demand: context.min_demand,
            max_demand: context.max_demand
          ]
          |> Keyword.merge(v_subscription_opts)

        {:ok, sub} = GenStage.sync_subscribe(pid, subscription_opts)

        Graph.label_vertex(g, pid, sub: sub)
      end)
    end)
  end

  defp create_struct(graph) do
    Graph.postorder(graph)
    |> Enum.reduce(%__MODULE__{}, fn pid, etl ->
      labels = Graph.vertex_labels(graph, pid)

      etl
      |> Map.update!(:pids, fn pids -> [pid | pids] end)
      |> Map.update!(:subscriptions, fn subs ->
        Keyword.get_values(labels, :sub) ++ subs
      end)
      |> Map.update!(:stages, fn stages -> [Keyword.get(labels, :step) | stages] end)
    end)
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

  defp intercept(%{start: {module, function, [args]}} = child_spec, opts) do
    dispatcher = Keyword.get(opts, :dispatcher)
    interceptor_args = Keyword.merge(opts, stage: module, args: args, dispatcher: dispatcher)
    %{child_spec | start: {Etl.Stage.Interceptor, function, [interceptor_args]}}
  end

  defp tails(graph) do
    graph
    |> Graph.vertices()
    |> Enum.filter(fn v -> Graph.out_degree(graph, v) == 0 end)
  end

  defp to_list(list) when is_list(list), do: list
  defp to_list(integer) when is_integer(integer), do: 0..(integer - 1)

  defp start_step(step, context, last_step) do
    interceptor_opts =
      case last_step do
        true -> [post_process: &Etl.ack/1]
        false -> []
      end
      |> Keyword.merge(dispatcher: step.dispatcher)

    intercepted_child_spec = intercept(step.child_spec, interceptor_opts)
    DynamicSupervisor.start_child(context.dynamic_supervisor, intercepted_child_spec)
  end
end
