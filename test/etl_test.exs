defmodule EtlTest do
  use ExUnit.Case

  test "etl can run a source to a destination" do
    %{pids: [producer | _]} = etl = Etl.run(
      source: %Etl.TestSource{},
      destination: %Etl.TestDestination{pid: self()}
    )

    events = Enum.map(1..100, fn i -> "event-#{i}" end)

    Etl.TestSource.send_events(producer, events)
    Etl.TestSource.stop(producer)

    :ok = Etl.await(etl, delay: 100, timeout: 5_000)

    Enum.each(events, fn event ->
      assert_received {:event, ^event}
    end)

  end

end
