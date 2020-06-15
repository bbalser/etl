defprotocol Etl.Destination do
  @spec stages(t, Etl.Context.t()) :: [Etl.stage()]
  def stages(t, context)
end
