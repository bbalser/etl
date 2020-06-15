defmodule Etl.TestConsumer do
  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    pid = Keyword.fetch!(opts, :pid)
    {:consumer, %{pid: pid}}
  end

  def handle_events(events, _from, state) do
    Enum.each(events, &IO.inspect(&1, label: "event"))
    {:noreply, [], state}
  end
end
