defmodule Etl.Tree do
  defmodule Stage do
    @type t :: %__MODULE__{
            pid: pid(),
            type: :producer | :producer_consumer | :consumer,
            subscribers: [t],
            partition: term(),
            mod: module(),
            dispatcher: module(),
            buffer_size: non_neg_integer()
          }

    defstruct [:pid, :type, :subscribers, :partition, :mod, :dispatcher, :buffer_size]
  end

  def discover(%Etl{pids: [producer | _]}) do
    generate_tree(producer)
  end

  def discover(pid) when is_pid(pid) do
    generate_tree(pid)
  end

  def print(%Stage{} = stage) do
    stage
    |> List.wrap()
    |> Mix.Utils.print_tree(&format/1, format: "pretty")
  end

  def print(stages) do
    discover(stages)
    |> print()
  end

  defp generate_tree(pid) do
    state = :sys.get_state(pid) |> IO.inspect(label: "state")
    partitions = partition_info(state)

    subscribers =
      state.consumers
      |> Enum.map(fn {_, {pid, _}} -> pid end)
      |> Enum.map(&generate_tree/1)
      |> Enum.map(fn sub ->
        %{sub | partition: Map.get(partitions, sub.pid)}
      end)

    %Stage{
      pid: pid,
      type: state.type,
      subscribers: subscribers,
      mod: module(state),
      dispatcher: state.dispatcher_mod,
      buffer_size: buffer_size(state.buffer)
    }
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

  defp format(%Stage{} = stage) do
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

    {{string, ""}, stage.subscribers}
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

  defp format_buffer_size(nil),do: ""
  defp format_buffer_size(buffer_size), do: ["buffer: ", to_string(buffer_size)]

  defp format_dispatcher(GenStage.DemandDispatcher), do: ""

  defp format_dispatcher(nil), do: ""

  defp format_dispatcher(dispatcher) do
    inspect(dispatcher)
  end
end
