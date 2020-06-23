defmodule Etl.TestSource do
  defstruct []

  defimpl Etl.Source do
    def stages(_t, _context) do
      [Etl.TestSource.Stage]
    end
  end

  def send_events(pid, events) do
    GenStage.cast(pid, {:events, events})
  end

  def stop(pid, reason \\ :normal) do
    GenStage.cast(pid, {:stop, reason})
  end
end

defmodule Etl.TestSource.Stage do
  use GenStage, restart: :transient

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    {:producer, %{}}
  end

  def handle_cast({:events, events}, state) do
    {:noreply, events, state}
  end

  def handle_cast({:stop, reason}, state) do
    {:stop, reason, state}
  end

  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end
end
