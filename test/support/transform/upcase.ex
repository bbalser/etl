defmodule Etl.Test.Transform.Upcase do
  import Brex.Result.Base, only: [ok: 1]

  def function(_t, _context) do
    &ok(String.upcase(&1))
  end
end
