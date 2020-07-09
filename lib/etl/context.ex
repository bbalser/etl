defmodule Etl.Context do
  @type t :: %__MODULE__{
          name: atom(),
          dictionary: Etl.Dictionary.t(),
          max_demand: pos_integer(),
          min_demand: pos_integer(),
          error_handler: (event :: term(), reason :: term() -> no_return())
        }

  defstruct name: nil,
            dictionary: nil,
            max_demand: nil,
            min_demand: nil,
            error_handler: nil
end
