defmodule Etl.Test.PartitionTracker do
  defstruct []

  defimpl Etl.Transformation do
    use Etl.Transformation.Stage

    def stages(_t, _context) do
      [Etl.Test.PartitionTracker.Stage]
    end
  end
end

defmodule Etl.Test.PartitionTracker.Stage do
  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    {:producer_consumer, Map.new(opts)}
  end

  def handle_events(events, _from, state) do
    events = Enum.map(events, fn event ->
      Map.update!(event, :metadata, fn metadata ->
        Map.merge(metadata, state)
      end)
    end)

    {:noreply, events, state}
  end

  def handle_subscribe(:producer, opts, _from, _state) do
    {:automatic, Map.new(opts)}
  end

  def handle_subscribe(_, _, _, state)  do
    {:automatic, state}
  end
end
