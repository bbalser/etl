defmodule Etl do
  require Logger
  @type stage :: Supervisor.child_spec() | {module(), arg :: term()} | module()
  @type dictionary :: term()

  @type t :: %__MODULE__{
          stages: [Etl.stage()],
          pids: [pid],
          subscriptions: [GenStage.subscription_tag()]
        }

  defstruct stages: [],
            pids: [],
            subscriptions: []

  alias Etl.Pipeline

  def pipeline(stage, opts \\ []) do
    Pipeline.new(opts)
    |> to(stage)
  end

  def to(pipeline, stage) do
    Pipeline.add_stage(pipeline, stage)
  end

  def function(pipeline, fun) when is_function(fun, 1) do
    Pipeline.add_function(pipeline, fun)
  end

  def partition(pipeline, opts) do
    Keyword.fetch!(opts, :partitions)
    Pipeline.set_partitions(pipeline, opts)
  end

  def run(%Pipeline{} = pipeline) do
    Graph.new(type: :directed)
    |> start_steps(Pipeline.steps(pipeline), pipeline.context)
    |> subscribe_stages(pipeline)
    |> create_struct()
  end

  defp start_steps(graph, [], _context), do: graph

  defp start_steps(graph, [step | remaining], context) do
    case tails(graph) do
      [] ->
        {:ok, pid} = start_step(step, context, remaining == [])
        Graph.add_vertex(graph, pid, step: step)

      tails ->
        Enum.reduce(tails, graph, fn tail, g ->
          case GenStage.call(tail, :"$dispatcher") do
            {GenStage.PartitionDispatcher, opts} ->
              partitions = Keyword.fetch!(opts, :partitions) |> to_list()

              partitions
              |> Enum.reduce(g, fn partition, g ->
                {:ok, pid} = start_step(step, context, remaining == [])

                g
                |> Graph.add_vertex(pid, step: step, subscription_opts: [partition: partition])
                |> Graph.add_edge(tail, pid)
              end)

            _ ->
              {:ok, pid} = start_step(step, context, remaining == [])

              g
              |> Graph.add_vertex(pid, step: step)
              |> Graph.add_edge(tail, pid)
          end
        end)
    end
    |> start_steps(remaining, context)
  end

  defp subscribe_stages(graph, pipeline) do
    Graph.postorder(graph)
    |> Enum.reduce(graph, fn pid, g ->
      v_subscription_opts = Graph.vertex_labels(g, pid) |> Keyword.get(:subscription_opts, [])

      Graph.in_neighbors(g, pid)
      |> Enum.reduce(g, fn neighbor, g ->
        subscription_opts =
          [
            to: neighbor,
            min_demand: pipeline.context.min_demand,
            max_demand: pipeline.context.max_demand
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
