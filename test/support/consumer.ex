defmodule Etl.Support.Consumer do
  defstruct [:pid, :name]

  defmodule Stage do
    use GenStage
    require Logger

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    def init(t) do
      Process.flag(:trap_exit, true)
      {:consumer, Map.from_struct(t) |> Map.put(:type, :consumer)}
    end

    def handle_events(events, _from, state) do
      Enum.each(events, fn event ->
        event = Etl.Message.put_new_metadata(event, self(), state)

        Logger.debug(fn -> "#{inspect(__MODULE__)}(#{state.name}): Event: #{event}" end)

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
