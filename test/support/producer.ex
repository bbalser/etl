defmodule Etl.Support.Producer do
  defstruct pid: nil, partitions: [], hash: nil

  defmodule Stage do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(t) do
      case t.partitions do
        [] ->
          {:producer, t}

        partitions when is_integer(partitions) ->
          hash = t.hash || fn event -> {event, :erlang.phash2(event.data, partitions)} end
          opts = [partitions: partitions, hash: hash]
          {:producer, t, dispatcher: {GenStage.PartitionDispatcher, opts}}

        partitions ->
          hash = t.hash || fn event -> {event, :erlang.phash2(event, Enum.count(partitions))} end
          opts = [partitions: partitions, hash: hash]
          {:producer, t, dispatcher: {GenStage.PartitionDispatcher, opts}}
      end
    end

    def handle_cast({:events, events}, state) do
      messages = Enum.map(events, &to_etl_message(&1, state.pid))
      {:noreply, messages, state}
    end

    def handle_cast({:stop, reason}, state) do
      {:stop, reason, state}
    end

    def handle_demand(_demand, state) do
      {:noreply, [], state}
    end

    defp to_etl_message(event, source) do
      %Etl.Message{
        data: event,
        acknowledger: {Etl.TestAcknowledger, source, event}
      }
    end
  end

  def send_events(pid, events) do
    GenStage.cast(pid, {:events, events})
  end

  def stop(pid, reason \\ :normal) do
    GenStage.cast(pid, {:stop, reason})
  end

  defimpl Etl.Stage do
    def spec(t, _context) do
      {Stage, t}
    end
  end
end
