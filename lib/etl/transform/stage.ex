defmodule Etl.Transform.Stage do
  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    functions = Keyword.fetch!(opts, :functions)

    {:producer_consumer, %{functions: functions}}
  end

  def handle_events(events, _from, %{functions: functions} = state) do
    transformed_events =
      events
      |> Enum.map(&apply_functions(functions, &1))

    {:noreply, transformed_events, state}
  end

  defp apply_functions(functions, event) do
    Enum.reduce(functions, event, fn fun, data ->
      fun.(data)
    end)
  end
end
