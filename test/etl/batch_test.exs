defmodule Etl.BatchTest do
  use ExUnit.Case

  @supervisor Test.DynSupervisor

  setup do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: @supervisor})

    :ok
  end

  test "batching" do
    test = self()

    %{pids: [producer | _]} =
      Etl.producer(%Etl.Support.Producer{pid: test})
      |> Etl.to(%Etl.Support.ProducerConsumer{})
      |> Etl.batch(batch_size: 3, batch_timeout: 1_000)
      |> Etl.to(%Etl.Support.BatchedConsumer{pid: test})
      |> Etl.run(dynamic_supervisor: @supervisor)

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.send_events(producer, [6])
    Etl.Support.Producer.send_events(producer, [7, 8, 9, 10])

    assert_receive {:batch_data, [1, 2, 3]}, 1000
    assert_receive {:batch_data, [4, 5, 6]}, 1000
    assert_receive {:batch_data, [7, 8, 9]}, 1000
    assert_receive {:batch_data, [10]}, 2000
  end

  @tag skip: true
  test "something" do
    test = self()

    {:ok, producer} =
      DynamicSupervisor.start_child(@supervisor, {Etl.Support.Producer.Stage, %Etl.Support.Producer{pid: test}})

    {:ok, pc1} =
      DynamicSupervisor.start_child(
        @supervisor,
        {Etl.Support.ProducerConsumer.Stage, %Etl.Support.ProducerConsumer{name: :pc1}} |> intercept(dispatcher: GenStage.BroadcastDispatcher)
      )

    {:ok, pc2_1} =
      DynamicSupervisor.start_child(
        @supervisor,
        {Etl.Support.ProducerConsumer.Stage, %Etl.Support.ProducerConsumer{name: :pc2_1}} |> intercept()
      )

    {:ok, pc2_2} =
      DynamicSupervisor.start_child(
        @supervisor,
        {Etl.Support.ProducerConsumer.Stage, %Etl.Support.ProducerConsumer{name: :pc2_2}} |> intercept()
      )

    {:ok, consumer} =
      DynamicSupervisor.start_child(@supervisor, {Etl.Support.Consumer.Stage, %Etl.Support.Consumer{pid: test}})

    max_demand = 2
    min_demand = 1

    GenStage.sync_subscribe(consumer, to: pc2_1, min_demand: min_demand, max_demand: max_demand)
    GenStage.sync_subscribe(consumer, to: pc2_2, min_demand: min_demand, max_demand: max_demand)

    GenStage.sync_subscribe(pc2_1, to: pc1, min_demand: min_demand, max_demand: max_demand)
    GenStage.sync_subscribe(pc2_2, to: pc1, min_demand: min_demand, max_demand: max_demand)

    GenStage.sync_subscribe(pc1, to: producer, dispatcher: GenStage.BroadcastDispatcher)

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])

    Process.sleep(2_000)

    Etl.Tree.print(producer)
  end

  defp intercept({module, arg}, opts \\ []) do
    {Etl.Stage.Interceptor,
     Keyword.merge(opts,
       stage: module,
       args: arg
     )}
  end

  # test "batching with multiple batch types" do
  #   test = self()

  #   %{pids: [producer | _]} =
  #     Etl.producer(%Etl.Support.Producer{pid: test})
  #     |> Etl.to(%Etl.Support.ProducerConsumer{})
  #     |> Etl.batch(
  #       even: fn data -> rem(data, 2) == 0 end,
  #       odd: fn data -> rem(data, 2) == 1 end,
  #       batch_timeout: 1_000
  #     )
  #     |> Etl.to(%Etl.Support.BatchedConsumer{pid: test})
  #     |> Etl.run(dynamic_supervisor: @supervisor)

  #   Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
  #   Etl.Support.Producer.send_events(producer, [6])
  #   Etl.Support.Producer.send_events(producer, [7, 8, 9, 10])

  #   assert_receive {:batch_events, [%{data: 1}, %{data: 3}, %{data: 5}, %{data: 7}, %{data: 9}]}, 1_200
  #   assert_receive {:batch_events, [%{data: 2}, %{data: 4}, %{data: 6}, %{data: 8}, %{data: 10}]}, 1_200
  # end
end
