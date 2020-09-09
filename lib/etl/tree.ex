defmodule Etl.Tree do
  def show(%Etl{pids: [producer | _]}) do
    tree(producer)
  end

  defp tree(pid, indent_level \\ 0) do
    %{type: type, consumers: consumers} = :sys.get_state(pid)

    IO.puts("#{String.duplicate("\t", indent_level)}(#{inspect(pid)}):#{type}")

    consumers
    |> Enum.map(fn {_, {pid, _}} -> pid end)
    |> Enum.each(fn pid -> tree(pid, indent_level + 1) end)

    :ok
  end
end
