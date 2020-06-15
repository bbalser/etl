defmodule Etl.Test.Transform.Upcase do

  defstruct []

  defimpl Etl.Transformation do
    import Brex.Result.Base, only: [ok: 1]

    def stage_or_function(_t), do: :function

    def stages(_t, _context) do
      []
    end

    def function(_t, _context) do
      &String.upcase/1
    end

    def dictionary(dictionary) do
      ok(dictionary)
    end
  end
end
