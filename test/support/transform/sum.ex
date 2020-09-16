defmodule Etl.Test.Transform.Sum do
  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    {:producer_consumer, %{sum: 0, producer: nil, result: nil}}
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
    result = %{state.result | data: state.sum}
    {:noreply, [result], state}
  end

  def handle_events(events, _from, state) do
    new_sum =
      Enum.reduce(events, state.sum, fn %Etl.Message{data: data}, total ->
        total + data
      end)

    result_message = List.last(events)
    GenStage.ask(state.producer, length(events))

    {:noreply, [], %{state | sum: new_sum, result: result_message}}
  end
end
