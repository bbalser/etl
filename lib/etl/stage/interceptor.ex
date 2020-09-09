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

    config = %{stage: stage, state: %{}, type: nil, post_process: post_process, pre_process: pre_process, init_opts: []}

    stage.init(args)
    |> wrap_response([], config)
  end

  def terminate(reason, %{stage: stage, state: state}) do
    case function_exported?(stage, :terminate, 2) do
      true ->
        stage.terminate(reason, state)

      false ->
        state
    end
  end

  def handle_call(:"$dispatcher", _from, %{init_opts: init_opts} = config) do
    {:reply, Keyword.get(init_opts, :dispatcher), [], config}
  end

  def handle_call(request, from, %{stage: stage, state: state} = config) do
    stage.handle_call(request, from, state)
    |> wrap_response([], config, config.post_process)
  end

  def handle_cast(request, %{stage: stage, state: state} = config) do
    stage.handle_cast(request, state)
    |> wrap_response([], config, config.post_process)
  end

  def handle_info(request, %{stage: stage, state: state} = config) do
    stage.handle_info(request, state)
    |> wrap_response([], config, config.post_process)
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
        |> wrap_response([], config, config.post_process)

      false ->
        {:noreply, [], config}
    end
  end

  def handle_demand(demand, %{stage: stage, state: state} = config) do
    stage.handle_demand(demand, state)
    |> wrap_response([], config, config.post_process)
  end

  def handle_events(events, from, %{stage: stage, state: state} = config) do
    config.pre_process.(events)

    stage.handle_events(events, from, state)
    |> wrap_response(events, config, config.post_process)
  end

  defp wrap_response(response, events, config, post_processor \\ nil)

  defp wrap_response({:noreply, handled, state}, pre_handled, config, post_processor) do
    execute_post_processor(handled, pre_handled, config, post_processor)
    {:noreply, handled, %{config | state: state}}
  end

  defp wrap_response({:noreply, handled, state, opts}, pre_handled, config, post_processor) do
    execute_post_processor(handled, pre_handled, config, post_processor)
    {:noreply, handled, %{config | state: state}, opts}
  end

  defp wrap_response({:reply, reply, handled, state}, pre_handled, config, post_processor) do
    execute_post_processor(handled, pre_handled, config, post_processor)
    {:reply, reply, handled, %{config | state: state}}
  end

  defp wrap_response({:reply, reply, handled, state, opts}, pre_handled, config, post_processor) do
    execute_post_processor(handled, pre_handled, config, post_processor)
    {:reply, reply, handled, %{config | state: state}, opts}
  end

  defp wrap_response({:stop, reason, reply, state}, _events, config, _post_processor) do
    {:stop, reason, reply, %{config | state: state}}
  end

  defp wrap_response({:stop, reason, state}, _events, config, _post_processor) do
    {:stop, reason, %{config | state: state}}
  end

  defp wrap_response({:stop, reason}, _events, _config, _post_processor) do
    {:stop, reason}
  end

  defp wrap_response({type, state}, _events, config, _post_processor) when type in @types do
    {type, %{config | state: state, type: type}}
  end

  defp wrap_response({type, state, opts}, _events, config, _post_processor) when type in @types do
    {type, %{config | state: state, type: type, init_opts: opts}, opts}
  end

  defp wrap_response(response, _events, _stage, _post_processor), do: response

  defp execute_post_processor(_handled, [], _type, _processor), do: :no_op

  defp execute_post_processor([], pre_handled, %{type: :consumer}, processor) when is_function(processor, 1) do
    processor.(pre_handled)
  end

  defp execute_post_processor(handled, _pre_handled, _type, processor) when is_function(processor, 1) do
    processor.(handled)
  end

  defp execute_post_processor(_handled, _pre_handled, _type, _non_processor), do: :no_op
end
