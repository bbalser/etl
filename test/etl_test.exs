defmodule EtlTest do
  use ExUnit.Case

  test "etl can run a source to a destination" do
    %{pids: [producer | _]} = etl = Etl.run(
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
      assert_received {:event, ^transformed_event}
    end)

  end

end
