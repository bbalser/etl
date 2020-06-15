defmodule Etl.Test.Transform.Upcase do

  defstruct []

  defimpl Etl.Transformation do
    use Etl.Transformation.Function

    def function(_t, _context) do
      &String.upcase/1
    end
  end
end
