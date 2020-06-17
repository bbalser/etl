defmodule Etl.Type.List do
  @type t :: %__MODULE__{
          name: String.t(),
          description: String.t(),
          nils: boolean(),
          item_type: Etl.Type.t()
        }
  defstruct name: nil, description: nil, item_type: nil, nils: true

  defimpl Etl.Type do
    import Brex.Result.Base, only: [ok: 1, error: 1]
    import Brex.Result.Mappers, only: [map_while_success: 2]
    import Brex.Result.Helpers, only: [map_error: 2]

    def name(%{name: name}) do
      name
    end

    def normalize(%{nils: true}, value) when value in [nil, ""] do
      ok(nil)
    end

    def normalize(t, value) when is_list(value) do
      map_while_success(value, &Etl.Type.normalize(t.item_type, &1))
      |> map_error(fn reason -> {:invalid_list, reason} end)
    end

    def normalize(_t, _value) do
      error(:invalid_list)
    end
  end
end
