defmodule Etl.TestDestination do
  defstruct [:pid]

  defimpl Etl.Destination do
    def stages(t, _context) do
      [{Etl.TestDestination.Stage, t}]
    end
  end
end

defmodule Etl.TestDestination.Stage do
  use GenStage, restart: :transient

  def start_link(t) do
    GenStage.start_link(__MODULE__, t)
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

  def handle_cancel(reason, _from, state) do
    Process.send_after(self(), reason, 500)
    {:noreply, [], state}
  end
end
