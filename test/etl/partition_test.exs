defmodule Etl.PartitionTest do
  use ExUnit.Case

  @supervisor Test.DynSupervisor

  setup do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: @supervisor})

    :ok
  end

  test "etl can support simple number of partitions" do
    test = self()

    hash = fn event ->
      case event.data do
        x when x in [1, 2, 3] -> {Etl.Message.add_metadata(event, :partition, 0), 0}
        _ -> {Etl.Message.add_metadata(event, :partition, 1), 1}
      end
    end

    %{pids: [producer | _]} =
      etl =
      Etl.pipeline(%Etl.Support.Producer{pid: test}, dynamic_supervisor: @supervisor)
      |> Etl.partition(partitions: 2, hash: hash)
      |> Etl.function(fn x -> {:ok, x * 2} end)
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:event, %{data: 2, metadata: %{partition: 0}}} = event
    assert_receive {:event, %{data: 4, metadata: %{partition: 0}}} = event
    assert_receive {:event, %{data: 6, metadata: %{partition: 0}}} = event
    assert_receive {:event, %{data: 8, metadata: %{partition: 1}}} = event
    assert_receive {:event, %{data: 10, metadata: %{partition: 1}}} = event
  end

  test "etl can support custom partitions with hash function" do
    test = self()

    hash = fn event ->
      case rem(event.data, 2) do
        0 -> {Etl.Message.add_metadata(event, :partition, :even), :even}
        1 -> {Etl.Message.add_metadata(event, :partition, :odd), :odd}
      end
    end

    %{pids: [producer | _]} =
      etl =
      Etl.pipeline(%Etl.Support.Producer{pid: test}, dynamic_supervisor: @supervisor)
      |> Etl.partition(partitions: [:odd, :even], hash: hash)
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
      partitions: 2,
      hash: fn event ->
        case event.data do
          x when x in [1, 2, 3] -> {Etl.Message.add_metadata(event, :partition, 0), 0}
          _ -> {Etl.Message.add_metadata(event, :partition, 1), 1}
        end
      end
    }

    %{pids: [producer | _]} =
      etl =
      Etl.pipeline(producer, dynamic_supervisor: @supervisor)
      |> Etl.function(fn x -> {:ok, x * 2} end)
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:event, %Etl.Message{data: 2, metadata: %{partition: 0}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 4, metadata: %{partition: 0}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 6, metadata: %{partition: 0}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 8, metadata: %{partition: 1}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 10, metadata: %{partition: 1}}}, 2_000
  end
end
