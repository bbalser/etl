defmodule Etl.Stage.InterceptorTest do
  use ExUnit.Case

  defmodule Stage do
    use GenStage

    def start_link(_) do
      GenStage.start_link(__MODULE__, [])
    end

    def init(_) do
      {:producer_consumer, %{}}
    end

    def handle_events(events, _from, state) do
      new_events = Enum.map(events, fn x -> x * 2 end)

      {:noreply, new_events, state}
    end
  end

  test "interceptor will allow events to pass" do
    producer = start_supervised!(Etl.TestSource.Stage)
    interceptor = start_supervised!({Etl.Stage.Interceptor, stage: Stage})
    consumer = start_supervised!({Etl.TestDestination.Stage, %{pid: self()}})

    GenStage.sync_subscribe(consumer, to: interceptor)
    GenStage.sync_subscribe(interceptor, to: producer)

    Etl.TestSource.send_events(producer, [1, 2, 3])

    assert_receive {:event, 2}
    assert_receive {:event, 4}
    assert_receive {:event, 6}
  end

  test "interceptor will send events to post process handler" do
    test = self()
    post_process = fn events ->
      Enum.each(events, &send(test, {:post_process, &1}))
    end

    producer = start_supervised!(Etl.TestSource.Stage)
    interceptor = start_supervised!({Etl.Stage.Interceptor, stage: Stage, post_process: post_process})
    consumer = start_supervised!({Etl.TestDestination.Stage, %{pid: self()}})

    GenStage.sync_subscribe(consumer, to: interceptor)
    GenStage.sync_subscribe(interceptor, to: producer)

    Etl.TestSource.send_events(producer, [1, 2, 3])

    assert_receive {:event, 2}
    assert_receive {:event, 4}
    assert_receive {:event, 6}

    assert_receive {:post_process, 2}
    assert_receive {:post_process, 4}
    assert_receive {:post_process, 6}
  end

  test "interceptor will send event to pre process handler" do
    test = self()
    pre_process = fn events ->
      Enum.each(events, &send(test, {:pre_process, &1}))
    end

    producer = start_supervised!(Etl.TestSource.Stage)
    interceptor = start_supervised!({Etl.Stage.Interceptor, stage: Stage, pre_process: pre_process})
    consumer = start_supervised!({Etl.TestDestination.Stage, %{pid: self()}})

    GenStage.sync_subscribe(consumer, to: interceptor)
    GenStage.sync_subscribe(interceptor, to: producer)

    Etl.TestSource.send_events(producer, [1, 2, 3])

    assert_receive {:pre_process, 1}
    assert_receive {:pre_process, 2}
    assert_receive {:pre_process, 3}

    assert_receive {:event, 2}
    assert_receive {:event, 4}
    assert_receive {:event, 6}
  end
end
