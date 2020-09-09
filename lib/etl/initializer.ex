defmodule Etl.Initializer do
  defmodule Stage do
    defstruct [:child_spec, :subscription_opts]
  end

  def start(stages, context) do
    %{pids: pids, subs: subs} = start_pipeline(stages, context, %{pids: [], subs: []})

    {Enum.reverse(pids), Enum.reverse(subs)}
  end

  defp start_pipeline([], _context, result), do: result

  defp start_pipeline([%Stage{} = stage | remaining], context, %{pids: pids, subs: subs}) do
    {:ok, pid} = start_child(stage.child_spec, context)

    {:ok, sub} = GenStage.sync_subscribe(pid, stage.subscription_opts)

    next(remaining, pid, context, %{pids: [pid | pids], subs: [sub | subs]})
  end

  defp start_pipeline([child_spec | remaining], context, %{pids: pids, subs: subs}) do
    {:ok, pid} = start_child(child_spec, context)

    next(remaining, pid, context, %{pids: [pid | pids], subs: subs})
  end

  defp next([], _pid, _context, result) do
    result
  end

  defp next([child_spec | remaining], pid, context, result) do
    case GenStage.call(pid, :"$dispatcher") do
      {GenStage.PartitionDispatcher, opts} ->
        partitions = Keyword.get(opts, :partitions)

        partitions
        |> to_list()
        |> Enum.reduce(result, fn partition, incoming_result ->
          next_stage = %Stage{child_spec: child_spec, subscription_opts: subscription_opts(pid, context, partition)}
          start_pipeline([next_stage | remaining], context, incoming_result)
        end)

      _ ->
        next_stage = %Stage{child_spec: child_spec, subscription_opts: subscription_opts(pid, context)}

        start_pipeline([next_stage | remaining], context, result)
    end
  end

  defp to_list(list) when is_list(list), do: list
  defp to_list(integer) when is_integer(integer), do: 0..(integer - 1)

  defp start_child(child_spec, context) do
    DynamicSupervisor.start_child(context.dynamic_supervisor, child_spec)
  end

  defp subscription_opts(pid, context) do
    [
      to: pid,
      max_demand: context.max_demand,
      min_demand: context.min_demand
    ]
  end

  defp subscription_opts(pid, context, partition) do
    [
      to: pid,
      max_demand: context.max_demand,
      min_demand: context.min_demand,
      partition: partition
    ]
  end
end
