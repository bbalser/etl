defmodule Etl.Support.BatchedConsumer do
  defstruct [:pid, :label]

  defmodule Stage do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(t) do
      {:consumer, t}
    end

    def handle_events(events, _from, state) do
      events =
        Enum.map(events, fn event ->
          Map.update!(event, :metadata, fn md ->
            Map.put(md, __MODULE__, state)
          end)
        end)

      send(state.pid, {:batch_data, Enum.map(events, &Map.get(&1, :data))})
      send(state.pid, {:batch_events, events})
      {:noreply, [], state}
    end

    def handle_subscribe(:producer, opts, _from, state) do
      state =
        Map.from_struct(state)
        |> Map.merge(Map.new(opts))

      {:automatic, state}
    end

    def handle_subscribe(_, _, _, state) do
      {:automatic, state}
    end

    defp get_label(%{label: nil}), do: :batch
    defp get_label(%{labe: label}), do: label
  end

  defimpl Etl.Stage do
    def spec(t, _context) do
      {Stage, t}
    end
  end
end
