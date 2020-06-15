defmodule Etl.Type.StringTest do
  use ExUnit.Case
  import Checkov

  data_test "normalize returns #{inspect(result)} for #{inspect(value)}" do
    type = %Etl.Type.String{name: "name"}

    assert result == Etl.Type.normalize(type, value)

    where [
      [:value, :result],
      ["one", {:ok, "one"}],
      [1, {:ok, "1"}],
      [{:one, 1}, {:error, :invalid_string}],
      [nil, {:ok, nil}]
    ]
  end

  test "normalize does not allow nil if configured" do
    type = %Etl.Type.String{name: "name", nils: false}

    assert {:error, :invalid_string} == Etl.Type.normalize(type, nil)
  end
end
