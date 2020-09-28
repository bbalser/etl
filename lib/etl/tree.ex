defmodule Etl.Tree do
  defmodule Vertex do
    @type t :: %__MODULE__{
            pid: pid(),
            type: :producer | :producer_consumer | :consumer,
            mod: module(),
            dispatcher: module(),
            buffer_size: non_neg_integer(),
            name: atom()
          }

    defstruct [:pid, :type, :subscribers, :partition, :mod, :dispatcher, :buffer_size, :name]
  end

  def discover(%Etl{pids: [producer | _]}) do
    add_to_graph(Graph.new(type: :directed), producer)
  end

  def discover(pid) when is_pid(pid) do
    add_to_graph(Graph.new(type: :directed), pid)
  end

  def print(%Graph{} = graph, %Vertex{} = producer) do
    {graph, producer}
    |> List.wrap()
    |> Mix.Utils.print_tree(&format/1, format: "pretty")
  end

  def print(stages) do
    {graph, vertex} = discover(stages)
    print(graph, vertex)
  end

  defp add_to_graph(graph, pid) do
    state = :sys.get_state(pid)
    partitions = partition_info(state)

    vertex = %Vertex{
      pid: pid,
      type: state.type,
      mod: module(state),
      dispatcher: state.dispatcher_mod,
      buffer_size: buffer_size(state.buffer),
      name: name(pid)
    }

    graph =
      state.consumers
      |> Enum.map(fn {_, {pid, _}} -> pid end)
      |> Enum.reduce(graph, fn next_pid, graph ->
        {graph, next_vertex} = add_to_graph(graph, next_pid)
        partition = Map.get(partitions, next_vertex.pid)
        updated_next_vertex = %{next_vertex | partition: partition}
        Graph.replace_vertex(graph, next_vertex, updated_next_vertex)
        Graph.add_edge(graph, vertex, updated_next_vertex)
      end)

    {graph, vertex}
  end

  defp partition_info(%{dispatcher_mod: GenStage.PartitionDispatcher, dispatcher_state: state}) do
    {_ref, _hash, _, _, partition_map, _, _} = state

    partition_map
    |> Enum.map(fn {partition, {pid, _, _}} -> {pid, partition} end)
    |> Map.new()
  end

  defp partition_info(_state), do: %{}

  defp module(%{mod: Etl.Stage.Interceptor, state: %{stage: module}}) do
    module
  end

  defp module(%{mod: module}) do
    module
  end

  defp buffer_size(nil), do: nil

  defp buffer_size(buffer) do
    GenStage.Buffer.estimate_size(buffer)
  end

  defp format({%Graph{} = graph, %Vertex{} = stage}) do
    string =
      [
        format_type(stage.type),
        format_partition(stage.partition),
        " -> ",
        [:bright, inspect(stage.mod), :reset],
        ["(", inspect(stage.pid), ")"],
        " ",
        format_buffer_size(stage.buffer_size),
        " ",
        format_dispatcher(stage.dispatcher)
      ]
      |> IO.ANSI.format()
      |> IO.iodata_to_binary()

    subscribers =
      Graph.out_edges(graph, stage)
      |> Enum.map(fn edge ->
        {graph, %{edge.v2 | partition: edge.label}}
      end)

    {{string, ""}, subscribers}
  end

  defp format_type(:producer) do
    [:green, "producer", :reset]
  end

  defp format_type(:producer_consumer) do
    [:yellow, "producer_consumer", :reset]
  end

  defp format_type(:consumer) do
    [:red, "consumer", :reset]
  end

  defp format_partition(nil), do: ""
  defp format_partition(partition), do: ["(", to_string(partition), ")"]

  defp format_buffer_size(nil), do: ""
  defp format_buffer_size(buffer_size), do: ["buffer: ", to_string(buffer_size)]

  defp format_dispatcher(GenStage.DemandDispatcher), do: ""

  defp format_dispatcher(nil), do: ""

  defp format_dispatcher(dispatcher) do
    inspect(dispatcher)
  end

  defp name(pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, []} -> nil
      {:registered_name, name} -> name
    end
  end
end
