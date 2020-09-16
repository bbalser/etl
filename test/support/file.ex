defmodule Test.File do
  @type t :: %__MODULE__{
          path: String.t()
        }

  defstruct path: nil
end
