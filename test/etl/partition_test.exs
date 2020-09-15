defmodule Etl.PartitionTest do
  use ExUnit.Case

  @supervisor Test.DynSupervisor

  setup do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: @supervisor})

    :ok
  end

  test "etl can support simple number of partitions" do
    test = self()

    %{pids: [producer | _]} =
      etl =
      Etl.pipeline(%Etl.Support.Producer{pid: test}, dynamic_supervisor: @supervisor)
      |> Etl.partition(partitions: 2)
      |> Etl.to(%Etl.Support.PartitionTracker{})
      |> Etl.function(fn x -> {:ok, x * 2} end)
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    partitions_used =
      Enum.map(1..5, fn _ ->
        assert_receive {:event, %Etl.Message{metadata: %{partition: p}}}, 2_000
        p
      end)
      |> Enum.uniq()

    assert 2 == Enum.count(partitions_used)
  end

  test "etl can support custom partitions with hash function" do
    test = self()

    hash = fn event ->
      case rem(event.data, 2) do
        0 -> {event, :even}
        1 -> {event, :odd}
      end
    end

    %{pids: [producer | _]} =
      etl =
      Etl.pipeline(%Etl.Support.Producer{pid: test}, dynamic_supervisor: @supervisor)
      |> Etl.partition(partitions: [:odd, :even], hash: hash)
      |> Etl.to(%Etl.Support.PartitionTracker{})
      |> Etl.function(fn x -> {:ok, x * 2} end)
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:event, %Etl.Message{data: 2, metadata: %{partition: :odd}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 4, metadata: %{partition: :even}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 6, metadata: %{partition: :odd}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 8, metadata: %{partition: :even}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 10, metadata: %{partition: :odd}}}, 2_000
  end

  test "etl can support partioning when stage sets partitions it self" do
    test = self()

    producer = %Etl.Support.Producer{
      pid: test,
      partitions: 2
    }

    %{pids: [producer | _]} =
      etl =
      Etl.pipeline(producer, dynamic_supervisor: @supervisor)
      |> Etl.to(%Etl.Support.PartitionTracker{})
      |> Etl.function(fn x -> {:ok, x * 2} end)
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    partitions_used =
      Enum.map(1..5, fn _ ->
        assert_receive {:event, %Etl.Message{metadata: %{partition: p}}}, 2_000
        p
      end)
      |> Enum.uniq()

    assert 2 == Enum.count(partitions_used)
  end
end
