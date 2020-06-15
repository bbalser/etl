defmodule Test.File.Source do
  use GenStage, restart: :transient
  require Logger

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    file = File.open!(path, [:read])

    {:producer, %{path: path, file: file}}
  end

  def handle_demand(demand, state) do
    events =
      Enum.reduce_while(1..demand, [], fn _, buffer ->
        case IO.read(state.file, :line) do
          :eof ->
            File.close(state.file)
            GenStage.async_info(self(), :end_of_file)
            {:halt, buffer}

          {:error, :terminated} ->
            {:halt, buffer}

          data ->
            {:cont, [data | buffer]}
        end
      end)
      |> Enum.reverse()

    {:noreply, events, state}
  end

  def handle_info(:end_of_file, state) do
    Logger.debug(fn -> "Shutting down producer -- end of file" end)
    {:stop, :normal, state}
  end
end
