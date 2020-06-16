defmodule Etl.Test.Transform.Custom do

  defstruct [function: nil]

  defimpl Etl.Transformation do
    use Etl.Transformation.Function

    def function(%{function: fun}, _context) do
      fun
    end
  end
end
