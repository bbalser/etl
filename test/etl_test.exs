defmodule EtlTest do
  use ExUnit.Case
  import Brex.Result.Base, only: [ok: 1]

  test "etl can run a source to a destination" do
    %{pids: [producer | _]} =
      etl =
      Etl.run(
        source: %Etl.TestSource{},
        transformations: [
          %Etl.Test.Transform.Upcase{}
        ],
        destination: %Etl.TestDestination{pid: self()}
      )

    events = Enum.map(1..100, fn i -> "event-#{i}" end)

    Etl.TestSource.send_events(producer, events)
    Etl.TestSource.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    Enum.each(events, fn event ->
      transformed_event = String.upcase(event)
      assert_receive {:event, ^transformed_event}, 1_000
    end)
  end

  test "etl can support transformations that are stages" do
    %{pids: [producer | _]} =
      etl =
      Etl.run(
        source: %Etl.TestSource{},
        transformations: [
          %Etl.Test.Transform.Custom{function: fn x -> ok(x * 2) end},
          %Etl.Test.Transform.Custom{function: fn x -> ok(x + 1) end},
          %Etl.Test.Transform.Sum{},
          %Etl.Test.Transform.Custom{function: fn x -> ok(x - 1) end}
        ],
        destination: %Etl.TestDestination{pid: self()}
      )

    Etl.TestSource.send_events(producer, [1, 2, 3, 4, 5])
    Etl.TestSource.send_events(producer, [6, 7, 8, 9, 10])
    Etl.TestSource.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    assert_receive {:event, 119}, 1_000
  end
end
