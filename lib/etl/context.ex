defmodule Etl.Context do
  @type t :: %__MODULE__{
          dictionary: Etl.Dictionary.t() | nil,
          max_demand: pos_integer(),
          min_demand: pos_integer(),
          error_handler: (event :: term(), reason :: term() -> no_return()) | nil,
          dynamic_supervisor: module()
        }

  defstruct dictionary: nil,
            max_demand: nil,
            min_demand: nil,
            error_handler: nil,
            dynamic_supervisor: Etl.DynamicSupervisor
end
