defmodule Etl.Type.Integer do
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

    def normalize(%{nils: false}, value) when value in [nil, ""] do
      error(:invalid_integer)
    end

    def normalize(_t, value) when is_integer(value) do
      ok(value)
    end

    def normalize(_t, value) when is_binary(value) do
      case Integer.parse(value) do
        {integer, _} -> ok(integer)
        :error -> error(:invalid_integer)
      end
    end

    def normalize(_t, _value) do
      error(:invalid_integer)
    end
  end
end
