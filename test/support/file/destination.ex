defmodule Test.File.Destination do
  use GenStage, restart: :transient

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    file = File.open!(path, [:write])

    {:consumer, %{path: path, file: file}}
  end

  def handle_events(events, _from, state) do
    Enum.each(events, fn event ->
      IO.write(state.file, event)
    end)

    {:noreply, [], state}
  end

  def handle_cancel({:down, reason}, _from, state) do
    File.close(state.file)
    {:stop, reason, state}
  end
end
