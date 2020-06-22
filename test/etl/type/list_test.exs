defmodule Etl.Type.ListTest do
  use ExUnit.Case

  test "normalizes data in list accord type item_type" do
    type = %Etl.Type.List{
      item_type: %Etl.Type.Integer{}
    }

    data = [1, "2", 3]

    assert {:ok, [1, 2, 3]} == Etl.Type.normalize(type, data)
  end

  test "an error normalizing any item causes the whole list to error" do
    type = %Etl.Type.List{
      item_type: %Etl.Type.Integer{}
    }

    data = [1, "two", 3]

    assert {:error, {:invalid_list, :invalid_integer}} == Etl.Type.normalize(type, data)
  end
end
