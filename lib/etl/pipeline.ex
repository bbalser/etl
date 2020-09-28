defmodule Etl.Pipeline do
  defmodule Step do
    @type t :: %__MODULE__{
            child_spec: Supervisor.child_spec(),
            opts: keyword(),
            dispatcher: {module(), keyword()}
          }

    defstruct [:child_spec, :opts, :dispatcher]
  end

  @type t :: %__MODULE__{
          context: Etl.Context.t(),
          steps: list()
        }

  defstruct context: nil, steps: []

  @partition_dispatcher GenStage.PartitionDispatcher
  @broadcast_dispatcher GenStage.BroadcastDispatcher

  def new(opts \\ []) do
    %__MODULE__{
      context: %Etl.Context{
        min_demand: Keyword.get(opts, :min_demand, 500),
        max_demand: Keyword.get(opts, :max_demand, 1000),
        dynamic_supervisor: Keyword.get(opts, :dynamic_supervisor, Etl.DynamicSupervisor)
      }
    }
  end

  def get_context(pipeline, opts) do
    Map.merge(pipeline.context, Map.new(opts))
  end

  def add_stage(pipeline, stage, opts) do
    Map.update!(pipeline, :steps, fn steps ->
      step = %Step{child_spec: to_child_spec(stage, pipeline), opts: opts}
      [step | steps]
    end)
  end

  def add_batch(pipeline, opts) do
    pipeline
    |> set_broadcast([])
    |> Map.update!(:steps, fn steps ->
      batcher = {Etl.Stage.Batcher, opts} |> to_child_spec(pipeline)
      step = %Step{child_spec: batcher, opts: opts}
      [step | steps]
    end)
  end

  def add_function(%{steps: [head | tail]} = pipeline, fun, opts) do
    fun =
      case Keyword.get(opts, :receive, :data) do
        :event ->
          fun

        :data ->
          fn event ->
            case fun.(event.data) do
              {:ok, new_data} -> {:ok, %{event | data: new_data}}
              {:error, reason} -> {:error, reason}
              new_data -> {:ok, %{event | data: new_data}}
            end
          end
      end

    case head.child_spec do
      %{start: {Etl.Functions.Stage, _, [opts]}} ->
        opts =
          Keyword.update!(opts, :functions, fn funs ->
            funs ++ [fun]
          end)

        new_child_spec = {Etl.Functions.Stage, opts} |> to_child_spec(pipeline)
        new_step = %{head | child_spec: new_child_spec}
        %{pipeline | steps: [new_step | tail]}

      _ ->
        stage = {Etl.Functions.Stage, context: pipeline.context, functions: [fun]}
        add_stage(pipeline, stage, [])
    end
  end

  def set_partitions(%{steps: [step | rest]} = pipeline, dispatcher_opts) do
    step = %{step | dispatcher: {@partition_dispatcher, dispatcher_opts}}
    %{pipeline | steps: [step | rest]}
  end

  def set_broadcast(%{steps: [step | rest]} = pipeline, broadcast_opts) do
    step = %{step | dispatcher: {@broadcast_dispatcher, broadcast_opts}}
    %{pipeline | steps: [step | rest]}
  end

  def steps(pipeline) do
    Enum.reverse(pipeline.steps)
  end

  defp to_child_spec(stage, pipeline) do
    Etl.Stage.spec(stage, pipeline.context)
    |> Supervisor.child_spec([])
  end
end
