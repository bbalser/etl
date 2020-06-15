defmodule Etl.Type.BooleanTest do
  use ExUnit.Case
  import Checkov

  data_test "validates booleans -- #{inspect(value)} --> #{inspect(result)}" do
    assert result == Etl.Type.normalize(%Etl.Type.Boolean{nils: true}, value)

    where [
      [:value, :result],
      [true, {:ok, true}],
      ["false", {:ok, false}],
      ["sure", {:error, :invalid_boolean}],
      [nil, {:ok, nil}],
      ["", {:ok, nil}]
    ]
  end
end
