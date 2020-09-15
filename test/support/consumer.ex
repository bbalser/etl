defmodule Etl.Support.Consumer do
  defstruct [:pid]

  defmodule Stage do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(t) do
      Process.flag(:trap_exit, true)
      {:consumer, t}
    end

    def handle_events(events, _from, state) do
      Enum.each(events, fn event ->
        send(state.pid, {:event, event})
        send(state.pid, {:data, event.data})
      end)

      {:noreply, [], state}
    end
  end

  defimpl Etl.Stage do
    def spec(t, _context) do
      {Stage, t}
    end
  end
end
