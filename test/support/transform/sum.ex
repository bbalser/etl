defmodule Etl.Test.Transform.Sum do
  defstruct []

  defimpl Etl.Transformation do
    use Etl.Transformation.Stage

    def stages(_t, _context) do
      [Etl.Test.Transform.Sum.Stage]
    end
  end
end

defmodule Etl.Test.Transform.Sum.Stage do
  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    {:producer_consumer, %{sum: 0, producer: nil}}
  end

  def handle_subscribe(:producer, opts, from, state) do
    max_demand = Keyword.fetch!(opts, :max_demand)
    GenStage.ask(from, max_demand)
    {:manual, %{state | producer: from}}
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  def handle_cancel(_reason, from, %{producer: from} = state) do
    {:noreply, [state.sum], state}
  end

  def handle_events(events, _from, state) do
    new_sum =
      Enum.reduce(events, state.sum, fn event, total ->
        total + event
      end)

    GenStage.ask(state.producer, length(events))

    {:noreply, [], %{state | sum: new_sum}}
  end
end
