defmodule Etl.Stage.Batcher do
  use GenStage

  defmodule State do
    defstruct batch_size: 100,
      batch_timeout: 10_000,
      timer: nil,
      queue: nil,
      partition: nil
  end

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    Process.flag(:trap_exit, true)

    state =
      struct(State, opts)
      |> Map.put(:queue, :queue.new())

    {:producer_consumer, state}
  end

  def handle_events(events, _from, state) do
    cancel_timer(state.timer)

    queue =
      for event <- events, reduce: state.queue do
        q -> :queue.in(event, q)
      end

    {outgoing, queue} =
      case state.batch_size <= :queue.len(queue) do
        false ->
          {[], queue}

        true ->
          {outgoing_q, queue} = :queue.split(state.batch_size, queue)
          {:queue.to_list(outgoing_q), queue}
      end

    {:noreply, outgoing, %{state | queue: queue, timer: start_timer(state.batch_timeout)}}
  end

  def handle_info({:timeout, _, :go}, state) do
    count = min(state.batch_size, :queue.len(state.queue))
    {outgoing, queue} = :queue.split(count, state.queue)

    {:noreply, :queue.to_list(outgoing), %{state | queue: queue, timer: start_timer(state.batch_timeout)}}
  end

  defp start_timer(timeout) do
    :erlang.start_timer(timeout, self(), :go)
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer) do
    :erlang.cancel_timer(timer)
  end
end
