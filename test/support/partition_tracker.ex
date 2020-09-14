defmodule Etl.Support.PartitionTracker do
  defstruct []

  defmodule Stage do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(opts) do
      {:producer_consumer, Map.new(opts)}
    end

    def handle_events(events, _from, state) do
      events =
        Enum.map(events, fn event ->
          Map.update!(event, :metadata, fn metadata ->
            Map.merge(metadata, state)
          end)
        end)

      {:noreply, events, state}
    end

    def handle_subscribe(:producer, opts, _from, _state) do
      {:automatic, Map.new(opts)}
    end

    def handle_subscribe(_, _, _, state) do
      {:automatic, state}
    end
  end

  defimpl Etl.Stage do
    def spec(_t, _context) do
      Etl.Support.PartitionTracker.Stage
    end
  end
end
