defmodule Etl.Type.Boolean do
  @type t :: %__MODULE__{
          name: String.t(),
          description: String.t(),
          nils: boolean()
        }

  defstruct name: nil, description: nil, nils: false

  defimpl Etl.Type do
    import Brex.Result.Base, only: [ok: 1, error: 1]

    def name(%{name: name}) do
      name
    end

    def normalize(%{nils: true}, value) when value in [nil, ""] do
      ok(nil)
    end

    def normalize(%{nils: false}, value) when value in [nil, ""] do
      error(:invalid_boolean)
    end

    def normalize(_t, value) when is_boolean(value) do
      ok(value)
    end

    def normalize(_t, value) when value in ["true", "false"] do
      value |> String.to_atom() |> ok()
    end

    def normalize(_t, _value) do
      error(:invalid_boolean)
    end
  end
end
