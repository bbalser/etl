defmodule EtlTest do
  use ExUnit.Case
  import Mox
  import Brex.Result.Base, only: [ok: 1]

  @supervisor Test.DynSupervisor

  setup :verify_on_exit!

  setup do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: @supervisor})

    :ok
  end

  describe "ack/1" do
    test "groups messages by ack_ref" do
      expect(MockAck, :ack, fn 0, [_, _, _, _], [] -> :ok end)
      expect(MockAck, :ack, fn 1, [_, _, _], [] -> :ok end)
      expect(MockAck, :ack, fn 2, [_, _, _], [] -> :ok end)

      Enum.map(0..9, fn i -> %Etl.Message{data: i, acknowledger: {MockAck, rem(i, 3), i}, status: :ok} end)
      |> Etl.ack()
    end

    test "groups messages by success/failure" do
      expect(MockAck, :ack, fn 0, [_, _, _], [_, _, _] -> :ok end)

      Enum.map(0..5, fn i -> %Etl.Message{data: i, acknowledger: {MockAck, 0, i}, status: status(i)} end)
      |> Etl.ack()
    end

    test "keeps order within groupings" do
      expect(MockAck, :ack, fn 0, [%{data: 0}, %{data: 6}, %{data: 12}], [%{data: 3}, %{data: 9}] -> :ok end)
      expect(MockAck, :ack, fn 1, [%{data: 4}, %{data: 10}], [%{data: 1}, %{data: 7}, %{data: 13}] -> :ok end)
      expect(MockAck, :ack, fn 2, [%{data: 2}, %{data: 8}, %{data: 14}], [%{data: 5}, %{data: 11}] -> :ok end)

      Enum.map(0..14, fn i -> %Etl.Message{data: i, acknowledger: {MockAck, rem(i, 3), i}, status: status(i)} end)
      |> Etl.ack()
    end
  end

  test "etl can run a source to a destination" do
    test = self()

    %{pids: [producer | _]} =
      etl =
      Etl.run(
        source: %Etl.TestSource{pid: test},
        transformations: [
          %Etl.Test.Transform.Upcase{}
        ],
        destination: %Etl.TestDestination{pid: test},
        dynamic_supervisor: @supervisor
      )

    events = Enum.map(1..2, fn i -> "event-#{i}" end)

    Etl.TestSource.send_events(producer, events)
    Etl.TestSource.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    Enum.each(events, fn event ->
      transformed_event = String.upcase(event)
      assert_receive {:data, ^transformed_event}, 1_000
    end)

    assert_receive %{success: 2, fail: 0}, 2_000
  end

  test "etl can support transformations that are stages" do
    test = self()

    %{pids: [producer | _]} =
      etl =
      Etl.run(
        source: %Etl.TestSource{pid: test},
        transformations: [
          %Etl.Test.Transform.Custom{function: fn x -> ok(x * 2) end},
          %Etl.Test.Transform.Custom{function: fn x -> ok(x + 1) end},
          %Etl.Test.Transform.Sum{},
          %Etl.Test.Transform.Custom{function: fn x -> ok(x - 1) end}
        ],
        destination: %Etl.TestDestination{pid: test},
        dynamic_supervisor: @supervisor
      )

    Etl.TestSource.send_events(producer, [1, 2, 3, 4, 5])
    Etl.TestSource.send_events(producer, [6, 7, 8, 9, 10])
    Etl.TestSource.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:data, 119}, 2_000
    assert_receive %{success: 1, fail: 0}, 2_000
  end

  test "etl can support simple number of partitions" do
    test = self()

    %{pids: [producer | _]} =
      etl =
      Etl.run(
        source: %Etl.TestSource{pid: test, partitions: 2},
        transformations: [
          %Etl.Test.PartitionTracker{},
          %Etl.Test.Transform.Custom{function: fn x -> {:ok, x * 2} end}
        ],
        destination: %Etl.TestDestination{pid: test},
        dynamic_supervisor: @supervisor
      )

    Etl.TestSource.send_events(producer, [1, 2, 3, 4, 5])
    Etl.TestSource.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:event, %Etl.Message{data: 2, metadata: %{partition: 0}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 4, metadata: %{partition: 0}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 6, metadata: %{partition: 1}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 8, metadata: %{partition: 1}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 10, metadata: %{partition: 0}}}, 2_000
  end

  test "etl can support custom partitions" do
    test = self()

    hash = fn event ->
      case rem(event.data, 2) do
        0 -> {event, :even}
        1 -> {event, :odd}
      end
    end

    %{pids: [producer | _]} =
      etl =
      Etl.run(
        source: %Etl.TestSource{pid: test, partitions: [:odd, :even], hash: hash},
        transformations: [
          %Etl.Test.PartitionTracker{},
          %Etl.Test.Transform.Custom{function: fn x -> {:ok, x * 2} end}
        ],
        destination: %Etl.TestDestination{pid: test},
        dynamic_supervisor: @supervisor
      )

    Etl.TestSource.send_events(producer, [1, 2, 3, 4, 5])
    Etl.TestSource.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)


    assert_receive {:event, %Etl.Message{data: 2, metadata: %{partition: :odd}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 4, metadata: %{partition: :even}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 6, metadata: %{partition: :odd}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 8, metadata: %{partition: :even}}}, 2_000
    assert_receive {:event, %Etl.Message{data: 10, metadata: %{partition: :odd}}}, 2_000
  end

  defp status(i) when rem(i, 2) == 0, do: :ok
  defp status(_), do: {:error, "test"}
end
