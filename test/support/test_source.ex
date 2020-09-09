defmodule Etl.TestSource do
  defstruct [:pid, partitions: []]

  defimpl Etl.Source do
    def stages(t, _context) do
      [{Etl.TestSource.Stage, t}]
    end
  end

  def send_events(pid, events) do
    GenStage.cast(pid, {:events, events})
  end

  def stop(pid, reason \\ :normal) do
    GenStage.cast(pid, {:stop, reason})
  end
end

defmodule Etl.TestSource.Stage do
  use GenStage, restart: :transient

  def start_link(t) do
    GenStage.start_link(__MODULE__, t)
  end

  def init(t) do
    case t.partitions do
      [] -> {:producer, t}
      partitions -> {:producer, t, dispatcher: {GenStage.PartitionDispatcher, partitions: partitions}}
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
