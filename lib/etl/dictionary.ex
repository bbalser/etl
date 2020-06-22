defmodule Etl.Dictionary do
  @type t :: %__MODULE__{
          types: [Etl.Type.t()]
        }

  defstruct types: []

  @spec normalize(t, map()) :: {:ok, map()} | {:error, reason :: term()}
  def normalize(dictionary, map) do
    Etl.Type.normalize(%Etl.Type.Map{dictionary: dictionary}, map)
  end
end
