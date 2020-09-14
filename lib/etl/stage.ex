defprotocol Etl.Stage do
  @spec spec(t, Etl.Context.t()) :: Etl.stage()
  def spec(t, context)
end
