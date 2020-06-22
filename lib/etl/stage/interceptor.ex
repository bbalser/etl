defmodule Etl.Stage.Interceptor do
  use GenStage

  @types [:producer, :producer_consumer, :consumer]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    stage = Keyword.fetch!(opts, :stage)
    args = Keyword.get(opts, :args, [])

    stage.init(args)
    |> wrap_response(stage)
  end

  def terminate(reason, %{stage: stage, state: state}) do
    case function_exported?(stage, :terminate, 2) do
      true ->
        stage.terminate(reason, state)

      false ->
        state
    end
  end

  def handle_call(request, from, %{stage: stage, state: state}) do
    stage.handle_call(request, from, state)
    |> wrap_response(stage)
  end

  def handle_cast(request, %{stage: stage, state: state}) do
    stage.handle_cast(request, state)
    |> wrap_response(stage)
  end

  def handle_info(request, %{stage: stage, state: state}) do
    stage.handle_info(request, state)
    |> wrap_response(stage)
  end

  def handle_subscribe(producer_or_consumer, subscription_options, from, %{
        stage: stage,
        state: state
      }) do
    case function_exported?(stage, :handle_subscribe, 4) do
      true ->
        case stage.handle_subscribe(producer_or_consumer, subscription_options, from, state) do
          {:automatic, state} -> {:automatic, wrap_state(stage, state)}
          {:manual, state} -> {:manual, wrap_state(stage, state)}
          {:stop, reason, state} -> {:stop, reason, wrap_state(stage, state)}
        end

      false ->
        {:automatic, %{stage: stage, state: state}}
    end
  end

  def handle_cancel(reason, from, %{stage: stage, state: state}) do
    case function_exported?(stage, :handle_cancel, 3) do
      true ->
        stage.handle_cancel(reason, from, state)
        |> wrap_response(stage)

      false ->
        {:noreply, [], wrap_state(stage, state)}
    end
  end

  def handle_demand(demand, %{stage: stage, state: state}) do
    stage.handle_demand(demand, state)
    |> wrap_response(stage)
  end

  def handle_events(events, from, %{stage: stage, state: state}) do
    stage.handle_events(events, from, state)
    |> wrap_response(stage)
  end

  defp wrap_response({:noreply, events, state}, stage) do
    {:noreply, events, wrap_state(stage, state)}
  end

  defp wrap_response({:noreply, events, state, opts}, stage) do
    {:noreply, events, wrap_state(stage, state), opts}
  end

  defp wrap_response({:reply, reply, events, state}, stage) do
    {:reply, reply, events, wrap_state(stage, state)}
  end

  defp wrap_response({:reply, reply, events, state, opts}, stage) do
    {:reply, reply, events, wrap_state(stage, state), opts}
  end

  defp wrap_response({:stop, reason, reply, state}, stage) do
    {:stop, reason, reply, wrap_state(stage, state)}
  end

  defp wrap_response({:stop, reason, state}, stage) do
    {:stop, reason, wrap_state(stage, state)}
  end

  defp wrap_response({:stop, reason}, _stage) do
    {:stop, reason}
  end

  defp wrap_response({type, state}, stage) when type in @types do
    {type, wrap_state(stage, state)}
  end

  defp wrap_response({type, state, opts}, stage) when type in @types do
    {type, wrap_state(stage, state), opts}
  end

  defp wrap_response(response, _stage) do
    response
  end

  defp wrap_state(stage, state) do
    %{stage: stage, state: state}
  end
end
