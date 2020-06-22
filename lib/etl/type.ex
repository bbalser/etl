defprotocol Etl.Type do
  @spec name(t) :: String.t()
  def name(t)

  @spec normalize(t, value :: term()) :: {:ok, term()} | {:error, reason :: term()}
  def normalize(t, value)
end
