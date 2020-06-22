defmodule Etl.Type.Timestamp do
  @type t :: %__MODULE__{
    name: String.t(),
    description: String.t(),
    nils: boolean(),
    format: String.t(),
    timezone: String.t()
  }
  defstruct name: nil, description: nil, nils: true, format: "%FT%T.%f", timezone: "Etc/UTC"

  defimpl Etl.Type do
    import Brex.Result.Base, only: [ok: 1, error: 1, bind: 2, fmap: 2]

    @tokenizer Timex.Parse.DateTime.Tokenizers.Strftime
    @utc "Etc/UTC"

    def name(%{name: name}) do
      name
    end

    def normalize(%{nils: true}, value) when value in [nil, ""] do
      ok(nil)
    end

    def normalize(t, value) when is_binary(value) do
      with {:ok, datetime} <- Timex.parse(value, t.format, @tokenizer) do
        datetime
        |> attach_timezone(t.timezone)
        |> bind(&to_utc/1)
        |> fmap(&NaiveDateTime.to_iso8601/1)
      end
    end

    def normalize(_t, _value) do
      error(:invalid_timestamp)
    end

    defp attach_timezone(%NaiveDateTime{} = datetime, timezone) do
      DateTime.from_naive(datetime, timezone)
    end

    defp attach_timezone(datetime, _), do: ok(datetime)

    defp to_utc(%DateTime{} = datetime) do
      DateTime.shift_zone(datetime, @utc)
    end

    defp to_utc(datetime), do: ok(datetime)
  end
end
