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
      etl =
      Etl.pipeline(%Etl.Support.Producer{pid: test}, dynamic_supervisor: @supervisor)
      |> Etl.broadcast()
      |> Etl.to(%Etl.Support.Consumer{pid: test}, count: 4)
      |> Etl.run()

    Etl.Support.Producer.send_events(producer, [1, 2, 3, 4, 5])
    Etl.Support.Producer.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:data, 5}
    assert_receive {:data, 5}
    assert_receive {:data, 5}
    assert_receive {:data, 5}
  end

end
