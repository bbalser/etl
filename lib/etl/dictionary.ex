defmodule Etl.Dictionary do
  @type t :: %__MODULE__{
          types: [Etl.Type.t()]
        }

  defstruct types: []
end
