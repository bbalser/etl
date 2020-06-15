defmodule Etl.Type.IntegerTest do
  use ExUnit.Case
  import Checkov

  data_test "normalized returns #{inspect(result)} for #{inspect(value)}" do
    type = %Etl.Type.Integer{name: "name"}

    assert result == Etl.Type.normalize(type, value)

    where [
      [:value, :result],
      [1, {:ok, 1}],
      ["1", {:ok, 1}],
      ["one", {:error, :invalid_integer}],
      [{:one, 1}, {:error, :invalid_integer}],
      [nil, {:ok, nil}],
      ["", {:ok, nil}]
    ]
  end

  test "nils are failed if configured" do
    type = %Etl.Type.Integer{name: "name", nils: false}

    assert {:error, :invalid_integer} == Etl.Type.normalize(type, nil)
  end
end
