defprotocol Etl.Stage do
  @fallback_to_any true

  @spec spec(t, Etl.Context.t()) :: Etl.stage()
  def spec(t, context)
end

defimpl Etl.Stage, for: Any do
  def spec(stage, _context) do
    stage
  end
end
