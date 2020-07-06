defmodule Etl.Stage.Interceptor do
  use GenStage

  @types [:producer, :producer_consumer, :consumer]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    stage = Keyword.fetch!(opts, :stage)
    pre_process = Keyword.get(opts, :pre_process, fn x -> x end)
    post_process = Keyword.get(opts, :post_process, fn x -> x end)
    args = Keyword.get(opts, :args, [])

    config = %{stage: stage, state: %{}, post_process: post_process, pre_process: pre_process}

    stage.init(args)
    |> wrap_response(config)
  end

  def terminate(reason, %{stage: stage, state: state}) do
    case function_exported?(stage, :terminate, 2) do
      true ->
        stage.terminate(reason, state)

      false ->
        state
    end
  end

  def handle_call(request, from, %{stage: stage, state: state} = config) do
    stage.handle_call(request, from, state)
    |> wrap_response(config)
  end

  def handle_cast(request, %{stage: stage, state: state} = config) do
    stage.handle_cast(request, state)
    |> wrap_response(config)
  end

  def handle_info(request, %{stage: stage, state: state} = config) do
    stage.handle_info(request, state)
    |> wrap_response(config)
  end

  def handle_subscribe(
        producer_or_consumer,
        subscription_options,
        from,
        %{
          stage: stage,
          state: state
        } = config
      ) do
    case function_exported?(stage, :handle_subscribe, 4) do
      true ->
        case stage.handle_subscribe(producer_or_consumer, subscription_options, from, state) do
          {:automatic, state} -> {:automatic, %{config | state: state}}
          {:manual, state} -> {:manual, %{config | state: state}}
          {:stop, reason, state} -> {:stop, reason, %{config | state: state}}
        end

      false ->
        {:automatic, config}
    end
  end

  def handle_cancel(reason, from, %{stage: stage, state: state} = config) do
    case function_exported?(stage, :handle_cancel, 3) do
      true ->
        stage.handle_cancel(reason, from, state)
        |> wrap_response(config)

      false ->
        {:noreply, [], config}
    end
  end

  def handle_demand(demand, %{stage: stage, state: state} = config) do
    stage.handle_demand(demand, state)
    |> wrap_response(config)
  end

  def handle_events(events, from, %{stage: stage, state: state} = config) do
    config.pre_process.(events)

    stage.handle_events(events, from, state)
    |> wrap_response(config)
  end

  defp wrap_response({:noreply, events, state}, config) do
    config.post_process.(events)
    {:noreply, events, %{config | state: state}}
  end

  defp wrap_response({:noreply, events, state, opts}, config) do
    config.post_process.(events)
    {:noreply, events, %{config | state: state}, opts}
  end

  defp wrap_response({:reply, reply, events, state}, config) do
    config.post_process.(events)
    {:reply, reply, events, %{config | state: state}}
  end

  defp wrap_response({:reply, reply, events, state, opts}, config) do
    config.post_process.(events)
    {:reply, reply, events, %{config | state: state}, opts}
  end

  defp wrap_response({:stop, reason, reply, state}, config) do
    {:stop, reason, reply, %{config | state: state}}
  end

  defp wrap_response({:stop, reason, state}, config) do
    {:stop, reason, %{config | state: state}}
  end

  defp wrap_response({:stop, reason}, _config) do
    {:stop, reason}
  end

  defp wrap_response({type, state}, config) when type in @types do
    {type, %{config | state: state}}
  end

  defp wrap_response({type, state, opts}, config) when type in @types do
    {type, %{config | state: state}, opts}
  end

  defp wrap_response(response, _stage) do
    response
  end
end
