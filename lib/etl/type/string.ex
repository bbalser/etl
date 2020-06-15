defmodule Etl.Type.String do
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

    def normalize(%{nils: true}, nil) do
      ok(nil)
    end

    def normalize(%{nils: false}, nil) do
      error(:invalid_string)
    end

    def normalize(_t, value) when is_binary(value) do
      value
      |> String.trim()
      |> ok()
    end

    def normalize(_t, value) do
      case String.Chars.impl_for(value) do
        nil -> error(:invalid_string)
        _ -> value |> to_string() |> String.trim() |> ok()
      end
    end
  end
end
