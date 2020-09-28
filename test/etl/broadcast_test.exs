defmodule Etl.BroadcastTest do
  use ExUnit.Case

  @supervisor Test.DynSupervisor

  setup do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: @supervisor})

    :ok
  end

  test "broadcast to all stages" do
    test = self()

    %{pids: [producer | _]} =
      Etl.producer(%Etl.Support.Producer{pid: test})
      |> Etl.broadcast()
      |> Etl.to(%Etl.Support.Consumer{pid: test}, count: 4)
      |> Etl.run(dynamic_supervisor: @supervisor)

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])

    assert_receive {:data, 5}
    assert_receive {:data, 5}
    assert_receive {:data, 5}
    assert_receive {:data, 5}
  end
end
