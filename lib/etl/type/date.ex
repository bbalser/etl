defmodule Etl.Type.Date do
  @type t :: %__MODULE__{
          name: String.t(),
          description: String.t(),
          nils: boolean(),
          format: String.t()
        }
  defstruct name: nil, description: "", nils: true, format: nil

  defimpl Etl.Type do
    import Brex.Result.Base, only: [ok: 1, error: 1]
    @tokenizer Timex.Parse.DateTime.Tokenizers.Strftime

    def name(%{name: name}) do
      name
    end

    def normalize(%{nils: true}, value) when value in [nil, ""] do
      ok(nil)
    end

    def normalize(_t, %Date{} = date) do
      date
      |> Date.to_iso8601()
      |> ok()
    end

    def normalize(t, value) when is_binary(value) do
      with {:ok, date} <- Timex.parse(value, t.format, @tokenizer) do
        date
        |> Date.to_iso8601()
        |> ok()
      end
    end

    def normalize(_t, _value) do
      error(:invalid_date)
    end
  end
end
