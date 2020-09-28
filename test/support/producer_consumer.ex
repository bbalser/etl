defmodule Etl.Support.ProducerConsumer do
  defstruct [:name]

  defmodule Stage do
    use GenStage
    require Logger

    def start_link(opts) do
      server_opts = Map.take(opts, [:name]) |> Enum.filter(& &1)
      GenStage.start_link(__MODULE__, opts, Keyword.new(server_opts))
    end

    def init(opts) do
      {:producer_consumer, Map.from_struct(opts) |> Map.put(:type, :producer_consumer)}
    end

    def handle_events(events, _from, state) do
      Enum.each(events, fn event ->
        Logger.debug(fn -> "#{inspect(__MODULE__)}(#{state.name}): Event: #{event}" end)
      end)

      events =
        Enum.map(events, fn event ->
          Etl.Message.put_new_metadata(event, self(), state)
        end)

      {:noreply, events, state}
    end

    def handle_subscribe(:producer, opts, _from, state) do
      {:automatic, Map.merge(state, Map.new(opts))}
    end

    def handle_subscribe(_, _, _, state) do
      {:automatic, state}
    end
  end

  defimpl Etl.Stage do
    def spec(t, _context) do
      {Stage, t}
    end
  end
end
