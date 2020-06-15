defprotocol Etl.Transformation do
  @spec stage_or_function(t) :: :stage | :function
  def stage_or_function(t)

  @spec stages(t, Etl.Context.t()) :: [Etl.stage()]
  def stages(t, context)

  @spec function(t, Etl.Context.t()) :: (term() -> {:ok, term()} | {:error, reason :: term()})
  def function(t, context)

  @spec dictionary(Etl.Dictionary.t()) :: {:ok, Etl.Dictionary.t()} | {:error, reason :: term()}
  def dictionary(dictionary)
end

defmodule Etl.Transformation.Stage do
  defmacro __using__(_opts) do
    quote do
      def stage_or_function(_t) do
        :stage
      end

      def function(_t, _context) do
        fn x -> x end
      end

      def dictionary(dictionary) do
        {:ok, dictionary}
      end

      defoverridable dictionary: 1
    end
  end
end

defmodule Etl.Transformation.Function do
  defmacro __using__(_opts) do
    quote do
      def stage_or_function(_t) do
        :function
      end

      def stages(_t, _context) do
        []
      end

      def dictionary(dictionary) do
        {:ok, dictionary}
      end

      defoverridable dictionary: 1
    end
  end
end
