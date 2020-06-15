defmodule Test.File do

  @type t :: %__MODULE__{
    path: String.t()
  }

  defstruct path: nil

  defimpl Etl.Source do
    def stages(t, _context) do
      [{Test.File.Source, path: t.path}]
    end
  end

  defimpl Etl.Destination do
    def stages(t, _context) do
      [{Test.File.Destination, path: t.path}]
    end
  end
end
