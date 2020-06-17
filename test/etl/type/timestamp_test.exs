defmodule Etl.Type.TimestampTest do
  use ExUnit.Case
  import Checkov

  data_test "validates dates - #{inspect(value)} tz #{timezone} --> #{inspect(result)}" do
    type = %Etl.Type.Timestamp{name: "fake", format: format, timezone: timezone}
    assert result == Etl.Type.normalize(type, value)

    where [
      [:format, :value, :timezone, :result],
      ["%Y-%0m-%0d %0H:%0M:%0S", "2020-01-01 08:31:12", "Etc/UTC", {:ok, "2020-01-01T08:31:12"}],
      ["%0m-%0d-%Y %0S:%0M:%0H", "05-10-1989 12:21:07", "Etc/UTC", {:ok, "1989-05-10T07:21:12"}],
      [
        "%Y-%m-%dT%H:%M:%S.%f %z",
        "2010-04-03T00:17:12.023 +0400",
        "Etc/UTC",
        {:ok, "2010-04-02T20:17:12.023"}
      ],
      [
        "%Y-%m-%dT%H:%M:%S.%f",
        "2010-04-03T00:17:12.023",
        "Etc/GMT-4",
        {:ok, "2010-04-02T20:17:12.023"}
      ],
      ["%Y", "1999-05-01", "Etc/UTC", {:error, "Expected end of input at line 1, column 4"}],
      ["%Y", "", "Etc/UTC", {:ok, nil}],
      ["%Y", nil, "Etc/UTC", {:ok, nil}]
    ]
  end

  test "normalize does not allow nil if configured" do
    type = %Etl.Type.Timestamp{name: "name", nils: false}

    assert {:error, :invalid_timestamp} == Etl.Type.normalize(type, nil)
  end
end
