defmodule Etl.Type.DateTest do
  use ExUnit.Case
  import Checkov

  data_test "validates dates - #{inspect(value)} --> #{inspect(result)}" do
    type = %Etl.Type.Date{name: "fake", format: format}

    assert result == Etl.Type.normalize(type, value)

    where [
      [:format, :value, :result],
      ["%Y-%0m-%0d", "2020-01-01", {:ok, "2020-01-01"}],
      ["%0m-%0d-%Y", "05-10-1989", {:ok, "1989-05-10"}],
      ["%Y", "1999-05-01", {:error, "Expected end of input at line 1, column 4"}],
      ["%Y", "", {:ok, nil}],
      ["%Y", nil, {:ok, nil}]
    ]
  end

  test "normalize does not allow nil if configured" do
    type = %Etl.Type.Date{name: "name", nils: false}

    assert {:error, :invalid_date} == Etl.Type.normalize(type, nil)
  end
end
