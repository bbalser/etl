defmodule Etl.Type.MapTest do
  use ExUnit.Case

  test "normalize runs normalized for all sub fields" do
    type = %Etl.Type.Map{
      dictionary: %Etl.Dictionary{
        types: [
          %Etl.Type.String{name: "name"},
          %Etl.Type.Integer{name: "age"}
        ]
      }
    }

    data = %{
      "name" => "hello ",
      "age" => "1"
    }

    expected = %{
      "name" => "hello",
      "age" => 1
    }

    assert {:ok, expected} == Etl.Type.normalize(type, data)
  end

  test "returns error tuple when any sub field errors" do
    type = %Etl.Type.Map{
      dictionary: %Etl.Dictionary{
        types: [
          %Etl.Type.String{name: "name"},
          %Etl.Type.Integer{name: "age"}
        ]
}
    }

    data = %{
      "name" => "hello",
      "age" => "one"
    }

    expected = {:error, %{"age" => :invalid_integer}}

    assert expected == Etl.Type.normalize(type, data)
  end
end
