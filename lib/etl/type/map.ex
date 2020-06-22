defmodule Etl.Type.Map do
  @type t :: %__MODULE__{
          name: String.t(),
          description: String.t(),
          dictionary: Etl.Dictionary.t(),
          nils: boolean()
        }

  defstruct name: nil, description: nil, dictionary: nil, nils: true

  defimpl Etl.Type do
    import Brex.Result.Base, only: [ok: 1, error: 1, fmap: 2]
    import Brex.Result.Mappers, only: [reduce_while_success: 3]
    import Brex.Result.Helpers, only: [map_error: 2]

    def name(%{name: name}) do
      name
    end

    def normalize(%{nils: true}, value) when value in [nil, ""] do
      ok(nil)
    end

    def normalize(t, map) when is_map(map) do
      reduce_while_success(t.dictionary.types, %{}, fn type, acc ->
        name = Etl.Type.name(type)
        value = Map.get(map, name)

        Etl.Type.normalize(type, value)
        |> fmap(&Map.put(acc, name, &1))
        |> map_error(fn r -> %{name => r} end)
      end)
    end

    def normalize(_t, _value) do
      error(:invalid_map)
    end
  end
end
