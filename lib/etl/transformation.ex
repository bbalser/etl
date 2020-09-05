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
    quote(location: :keep) do
      def stage_or_function(t) do
        Etl.Transformation.Stage.stage_or_function(t)
      end

      def function(t, context) do
        Etl.Transformation.Stage.function(t, context)
      end

      def dictionary(dictionary) do
        Etl.Transformation.Stage.dictionary(dictionary)
      end

      defoverridable dictionary: 1
    end
  end

  def stage_or_function(_t), do: :stage

  def function(_t, _context), do: fn x -> {:ok, x} end

  def dictionary(dictionary), do: {:ok, dictionary}
end

defmodule Etl.Transformation.Function do
  defmacro __using__(_opts) do
    quote(location: :keep) do
      def stage_or_function(t) do
        Etl.Transformation.Function.stage_or_function(t)
      end

      def stages(t, context) do
        Etl.Transformation.Function.stages(t, context)
      end

      def dictionary(dictionary) do
        Etl.Transformation.Function.dictionary(dictionary)
      end

      defoverridable dictionary: 1
    end
  end

  def stage_or_function(_t), do: :function

  def stages(_t, _context), do: []

  def dictionary(dictionary), do: {:ok, dictionary}
end
