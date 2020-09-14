defmodule Etl.Support.ProducerConsumer do
  defstruct []

  defmodule Stage do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(opts) do
      {:producer_consumer, opts}
    end

    def handle_events(events, _from, state) do
      {:noreply, events, state}
    end
  end

  defimpl Etl.Stage do
    def spec(_t, _context) do
      {Stage, []}
    end
  end
end
