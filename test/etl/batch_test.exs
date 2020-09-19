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
      Etl.pipeline(%Etl.Support.Producer{pid: test}, dynamic_supervisor: @supervisor)
      |> Etl.to(%Etl.Support.ProducerConsumer{})
      |> Etl.batch(batch_size: 3, batch_timeout: 1_000)
      |> Etl.to(%Etl.Support.BatchedConsumer{pid: test})
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.send_events(producer, [6])
    Etl.Support.Producer.send_events(producer, [7, 8, 9, 10])

    assert_receive {:batch_data, [1, 2, 3]}, 1000
    assert_receive {:batch_data, [4, 5, 6]}, 1000
    assert_receive {:batch_data, [7, 8, 9]}, 1000
    assert_receive {:batch_data, [10]}, 2000
  end

  test "partion into batching" do
    test = self()

    hash = fn event ->
      case rem(event.data, 2) do
        0 -> {Map.update!(event, :metadata, &Map.put(&1, :partition, :even)), :even}
        1 -> {Map.update!(event, :metadata, &Map.put(&1, :partition, :odd)), :odd}
      end
    end

    %{pids: [producer | _]} =
      Etl.pipeline(%Etl.Support.Producer{pid: test}, dynamic_supervisor: @supervisor)
      |> Etl.to(%Etl.Support.ProducerConsumer{})
      |> Etl.partition(partitions: [:even, :odd], hash: hash)
      |> Etl.batch(batch_size: 3, batch_timeout: 1_000)
      |> Etl.to(%Etl.Support.BatchedConsumer{pid: test})
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.send_events(producer, [6])
    Etl.Support.Producer.send_events(producer, [7, 8, 9, 10])

    assert_receive {:batch_events, [%{data: 1}, %{data: 3}, %{data: 5}] = events}, 1000
    assert Enum.all?(events, fn event -> event.metadata.partition == :odd end)

    assert_receive {:batch_events, [%{data: 2}, %{data: 4}, %{data: 6}] = events}, 1000
    assert Enum.all?(events, fn event -> event.metadata.partition == :even end)

    assert_receive {:batch_events, [%{data: 7}, %{data: 9}] = events}, 2000
    assert Enum.all?(events, fn event -> event.metadata.partition == :odd end)

    assert_receive {:batch_events, [%{data: 8}, %{data: 10}] = events}, 2000
    assert Enum.all?(events, fn event -> event.metadata.partition == :even end)
  end
end
