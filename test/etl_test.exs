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
      Etl.producer(%Etl.Support.Producer{pid: test})
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run(dynamic_supervisor: @supervisor)

    events = Enum.map(1..2, fn i -> "event-#{i}" end)

    Etl.Support.Producer.send_events(producer, events)
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    Enum.each(events, fn event ->
      assert_receive {:data, ^event}, 1_000
    end)

    assert_receive {:ack, %{success: 2, fail: 0}}, 2_000
  end

  test "etl can run stages as normal child_specs" do
    test = self()

    %{pids: [producer | _]} =
      etl =
      Etl.producer({Etl.Support.Producer.Stage, %Etl.Support.Producer{pid: test}})
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run(dynamic_supervisor: @supervisor)

    events = Enum.map(1..2, fn i -> "event-#{i}" end)

    Etl.Support.Producer.send_events(producer, events)
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    Enum.each(events, fn event ->
      assert_receive {:data, ^event}, 1_000
    end)

    assert_receive {:ack, %{success: 2, fail: 0}}, 2_000
  end

  test "etl can support functions that are stages" do
    test = self()

    %{pids: [producer | _]} =
      etl =
      Etl.producer(%Etl.Support.Producer{pid: test})
      |> Etl.to(fn x -> ok(x * 2) end)
      |> Etl.to(fn event -> Etl.Message.update_data(event, fn x -> x + 1 end) |> ok() end, receive: :event)
      |> Etl.to(Etl.Test.Transform.Sum)
      |> Etl.to(fn x -> ok(x - 1) end)
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run(dynamic_supervisor: @supervisor)

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.send_events(producer, [6, 7, 8, 9, 10])
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:data, 119}, 2_000
    assert_receive {:ack, %{success: 1, fail: 0}}, 2_000
  end

  test "etl can support parallelism" do
    test = self()

    %{pids: [producer | _]} =
      Etl.producer(%Etl.Support.Producer{pid: test})
      |> Etl.to(%Etl.Support.ProducerConsumer{}, count: 4)
      |> Etl.to(%Etl.Support.Consumer{pid: test})
      |> Etl.run(dynamic_supervisor: @supervisor, min_demand: 0, max_demand: 1)

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.send_events(producer, [6, 7, 8, 9, 10])

    events =
      Enum.reduce(1..10, [], fn n, buffer ->
        assert_receive {:event, %{data: ^n} = event}, 5_000
        [event | buffer]
      end)

    assert 4 == count_stages_by_type(events, :producer_consumer)
    assert 1 == count_stages_by_type(events, :consumer)
  end

  test "etl can support parallelism all the way to consumer" do
    test = self()

    %{pids: [producer | _]} =
      Etl.producer(%Etl.Support.Producer{pid: test})
      |> Etl.to(%Etl.Support.ProducerConsumer{}, count: 4)
      |> Etl.to(%Etl.Support.Consumer{pid: test}, subscribe_strategy: :per_producer)
      |> Etl.run(dynamic_supervisor: @supervisor, min_demand: 0, max_demand: 1)

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.send_events(producer, [6, 7, 8, 9, 10])

    events =
      Enum.reduce(1..10, [], fn n, buffer ->
        assert_receive {:event, %{data: ^n} = event}, 5_000
        [event | buffer]
      end)

    assert 4 == count_stages_by_type(events, :producer_consumer)
    assert 4 == count_stages_by_type(events, :consumer)
  end

  test "the tree widens" do
    test = self()

    %{pids: [producer | _]} =
      Etl.producer(%Etl.Support.Producer{pid: test})
      |> Etl.to(%Etl.Support.ProducerConsumer{}, count: 4)
      |> Etl.to(%Etl.Support.Consumer{pid: test}, count: 4, subscribe_strategy: :per_producer)
      |> Etl.run(dynamic_supervisor: @supervisor, min_demand: 0, max_demand: 1)

    1..64
    |> Enum.chunk_every(5)
    |> Enum.each(fn events ->
      Etl.Support.Producer.send_events(producer, events)
    end)

    events =
      Enum.reduce(1..64, [], fn n, buffer ->
        assert_receive {:event, %{data: ^n} = event}, 5_000
        [event | buffer]
      end)

    assert 4 == count_stages_by_type(events, :producer_consumer)
    assert 16 == count_stages_by_type(events, :consumer)
  end

  defp status(i) when rem(i, 2) == 0, do: :ok
  defp status(_), do: {:error, "test"}

  defp count_stages_by_type(events, filtered_type) do
    events
    |> Enum.map(fn %{metadata: metadata} ->
      Enum.map(metadata, fn {pid, %{type: type}} -> {pid, type} end)
      |> Enum.filter(fn {_, type} -> type == filtered_type end)
    end)
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.count()
  end
end
