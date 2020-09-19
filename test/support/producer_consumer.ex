defmodule Etl.Support.ProducerConsumer do
  defstruct []

  defmodule Stage do
    use GenStage
    require Logger

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(opts) do
      {:producer_consumer, opts}
    end

    def handle_events(events, _from, state) do
      Logger.debug(fn -> "#{inspect(__MODULE__)} Events: #{inspect(events)}" end)

      events =
        Enum.map(events, fn event ->
          Map.update!(event, :metadata, fn md ->
            Map.put(md, __MODULE__, state)
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
      {Stage, []}
    end
  end
end
