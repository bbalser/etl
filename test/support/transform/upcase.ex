defmodule Etl.Test.Transform.Upcase do
  defstruct []

  defimpl Etl.Transformation do
    use Etl.Transformation.Function
    import Brex.Result.Base, only: [ok: 1]

    def function(_t, _context) do
      &ok(String.upcase(&1))
    end
  end
end
