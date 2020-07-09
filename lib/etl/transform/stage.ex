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
    start_time = System.monotonic_time()
    emit_start_metric(state.context.name, start_time, events, state.functions)

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

    emit_stop_metric(state.context.name, start_time)

    {:noreply, transformed_events, state}
  end

  defp apply_functions(state, event) do
    reduce_while_success(state.functions, event, fn fun, data ->
      fun.(data)
    end)
  end

  defp emit_start_metric(name, start_time, messages, functions) do
    metadata = %{name: name, messages: messages, functions: functions}
    measurements = %{time: start_time}
    :telemetry.execute([:etl, :transformation, :start], measurements, metadata)
  end

  defp emit_stop_metric(name, start_time) do
    stop_time = System.monotonic_time()
    measurements = %{time: stop_time, duration: stop_time - start_time}
    metadata = %{name: name}
    :telemetry.execute([:etl, :transformation, :stop], measurements, metadata)
  end
end
