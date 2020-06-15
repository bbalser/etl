defprotocol Etl.Transformation do
  @spec stage_or_function(t) :: :stage | :function
  def stage_or_function(t)

  @spec stages(t, Etl.Context.t()) :: [Etl.stage()]
  def stages(t, context)

  @spec function(t, Etl.Context.t()) :: (map() -> {:ok, map()} | {:error, reason :: term()})
  def function(t, context)

  @spec dictionary(Etl.Dictionary.t()) :: {:ok, Etl.Dictionary.t()} | {:error, reason :: term()}
  def dictionary(dictionary)
end
