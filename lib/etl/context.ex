defmodule Etl.Context do
  @type t :: %__MODULE__{
    dictionary: Etl.Dictionary.t(),
    max_demand: pos_integer(),
    min_demand: pos_integer()
  }

  defstruct dictionary: nil,
            max_demand: nil,
            min_demand: nil
end
