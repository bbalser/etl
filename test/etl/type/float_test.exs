defmodule Etl.Type.FloatTest do
  use ExUnit.Case
  import Checkov

  data_test "normalized returns #{inspect(result)} for #{inspect(value)}" do
    type = %Etl.Type.Float{name: "name"}

    assert result == Etl.Type.normalize(type, value)

    where [
      [:value, :result],
      [1.0, {:ok, 1.0}],
      ["1", {:ok, 1.0}],
      ["one", {:error, :invalid_float}],
      [{:one, 1}, {:error, :invalid_float}],
      [nil, {:ok, nil}],
      ["", {:ok, nil}]
    ]
  end

  test "nils are failed if configured" do
    type = %Etl.Type.Float{name: "name", nils: false}

    assert {:error, :invalid_float} == Etl.Type.normalize(type, nil)
  end
end
