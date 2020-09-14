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

  defmodule Pipeline do
    defstruct graph: nil, context: nil

    def new(opts \\ []) do
      %__MODULE__{
        graph: Graph.new(type: :directed),
        context: %Etl.Context{
          min_demand: Keyword.get(opts, :min_demand, 500),
          max_demand: Keyword.get(opts, :max_demand, 1000),
          dynamic_supervisor: Keyword.get(opts, :dynamic_supervisor, Etl.DynamicSupervisor)
        }
      }
    end

    def heads(pipeline) do
      pipeline.graph
      |> Graph.vertices()
      |> Enum.filter(fn v -> Graph.in_degree(pipeline.graph, v) == 0 end)
    end

    def tails(pipeline) do
      pipeline.graph
      |> Graph.vertices()
      |> Enum.filter(fn v -> Graph.out_degree(pipeline.graph, v) == 0 end)
    end

    def update_graph(pipeline, fun) do
      Map.update!(pipeline, :graph, fun)
    end

    def label_stage(pipeline, stage, label) do
      update_graph(pipeline, fn g ->
        Graph.label_vertex(g, stage, label)
      end)
    end
  end

  def pipeline(stage, opts \\ []) do
    Pipeline.new(opts)
    |> to(stage)
  end

  def to(pipeline, stage) do
    case Pipeline.tails(pipeline) do
      [] ->
        child_spec = to_child_spec(stage, pipeline)
        Pipeline.update_graph(pipeline, &Graph.add_vertex(&1, child_spec))

      tails ->
        Enum.reduce(tails, pipeline, fn tail, pipeline ->
          Pipeline.update_graph(pipeline, fn g ->
            case Graph.vertex_labels(g, tail) |> Keyword.get(:dispatcher) do
              {GenStage.PartitionDispatcher, opts} ->
                partitions = Keyword.fetch!(opts, :partitions)

                partitions
                |> Enum.zip(to_child_spec(stage, pipeline, Enum.count(partitions)))
                |> Enum.reduce(g, fn {partition, child_spec}, g ->
                  g
                  |> Graph.add_vertex(child_spec, subscription_opts: [partition: partition])
                  |> Graph.add_edge(tail, child_spec)
                end)

              _ ->
                Graph.add_edge(g, tail, to_child_spec(stage, pipeline))
            end
          end)
        end)
    end
  end

  def function(pipeline, fun) when is_function(fun, 1) do
    spec = {Etl.Functions.Stage, context: pipeline.context, functions: [fun]}
    to(pipeline, spec)
  end

  def partition(pipeline, opts) do
    partitions = Keyword.fetch!(opts, :partitions)

    Pipeline.tails(pipeline)
    |> Enum.reduce(pipeline, fn tail, pipeline ->
      Pipeline.label_stage(
        pipeline,
        tail,
        {:dispatcher, {GenStage.PartitionDispatcher, partitions: to_list(partitions)}}
      )
    end)
  end

  def run(%Pipeline{} = pipeline) do
    graph =
      Graph.preorder(pipeline.graph)
      |> Enum.reduce(pipeline.graph, fn stage, graph ->
        {:ok, pid} =
          stage
          |> intercept(pipeline.graph)
          |> start_child(pipeline.context)

        Graph.label_vertex(graph, stage, {:pid, pid})
      end)

    {pids, subs} =
      Graph.postorder(graph)
      |> Enum.reduce({[], []}, fn stage, {pids, subs} ->
        labels = Graph.vertex_labels(graph, stage)
        pid = Keyword.fetch!(labels, :pid)

        new_subs =
          Graph.in_neighbors(graph, stage)
          |> Enum.map(fn incoming ->
            incoming_pid = Graph.vertex_labels(graph, incoming) |> Keyword.fetch!(:pid)

            subscription_opts =
              [
                to: incoming_pid,
                min_demand: pipeline.context.min_demand,
                max_demand: pipeline.context.max_demand
              ]
              |> Keyword.merge(Keyword.get(labels, :subscription_opts, []))

            {:ok, sub} = GenStage.sync_subscribe(pid, subscription_opts)

            sub
          end)

        {pids ++ [pid], subs ++ new_subs}
      end)

    %__MODULE__{pids: Enum.reverse(pids), subscriptions: Enum.reverse(subs)}
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

  defp intercept(%{start: {module, function, [args]}} = child_spec, graph, opts \\ []) do
    dispatcher = Graph.vertex_labels(graph, child_spec) |> Keyword.get(:dispatcher)

    case {Graph.in_degree(graph, child_spec), Graph.out_degree(graph, child_spec)} do
      {0, 0} ->
        child_spec

      {_, 0} ->
        post_processor = fn events -> Etl.ack(events) end

        interceptor_args =
          Keyword.merge(opts, stage: module, args: args, dispatcher: dispatcher, post_process: post_processor)

        %{child_spec | start: {Etl.Stage.Interceptor, function, [interceptor_args]}}

      {_, _} ->
        interceptor_args = Keyword.merge(opts, stage: module, args: args, dispatcher: dispatcher)
        %{child_spec | start: {Etl.Stage.Interceptor, function, [interceptor_args]}}
    end
  end

  defp to_list(list) when is_list(list), do: list
  defp to_list(integer) when is_integer(integer), do: 0..(integer - 1)

  defp to_child_spec(stage, pipeline, count \\ 1) do
    child_spec =
      case Etl.Stage.impl_for(stage) do
        nil -> stage
        _ -> Etl.Stage.spec(stage, pipeline.context)
      end

    case count do
      1 ->
        Supervisor.child_spec(child_spec, id: UUID.uuid4() |> String.to_atom())

      n ->
        Enum.map(1..n, fn _ ->
          Supervisor.child_spec(child_spec, id: UUID.uuid4() |> String.to_atom())
        end)
    end
  end

  defp start_child(child_spec, context) do
    DynamicSupervisor.start_child(context.dynamic_supervisor, child_spec)
  end
end
