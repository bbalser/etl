defmodule Etl.Type.Float do

  @type t :: %__MODULE__{
    name: String.t(),
    description: String.t(),
    nils: boolean()
  }

  defstruct name: nil, description: nil, nils: true

  defimpl Etl.Type do
    import Brex.Result.Base, only: [ok: 1, error: 1]

    def name(%{name: name}) do
      name
    end

    def normalize(%{nils: true}, value) when value in [nil, ""] do
      ok(nil)
    end

    def normalize(_t, value) when is_float(value) do
      ok(value)
    end

    def normalize(_t, value) when is_binary(value) do
      case Float.parse(value) do
        {parsed_value, _} -> ok(parsed_value)
        :error -> error(:invalid_float)
      end
    end

    def normalize(_t, _value) do
      error(:invalid_float)
    end
  end
end
