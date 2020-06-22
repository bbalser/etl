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
    {:consumer, t}
  end

  def handle_events(events, _from, state) do
    Enum.each(events, &send(state.pid, {:event, &1}))
    {:noreply, [], state}
  end
end
