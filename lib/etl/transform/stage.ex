defmodule Etl.Transform.Stage do
  use GenStage
  import Brex.Result.Mappers, only: [reduce_while_success: 3]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    context = Keyword.fetch!(opts, :context)
    functions = Keyword.fetch!(opts, :functions)

    {:producer_consumer, %{functions: functions, context: context}}
  end

  def handle_events(events, _from, state) do
    transformed_events =
      events
      |> Enum.reduce([], fn event, buffer ->
        case apply_functions(state, event) do
          {:ok, value} ->
            [value | buffer]

          {:error, reason} ->
            state.context.error_handler.(event, reason)
            buffer
        end
      end)
      |> Enum.reverse()

    {:noreply, transformed_events, state}
  end

  defp apply_functions(state, event) do
    reduce_while_success(state.functions, event, fn fun, data ->
      fun.(data)
    end)
  end
end
