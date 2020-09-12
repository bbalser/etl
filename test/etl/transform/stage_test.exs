defmodule Etl.Transform.StageTest do
  use ExUnit.Case
  import Brex.Result.Base, only: [ok: 1, error: 1]

  test "transform stage will call all configured functions and pass any errors to the error handler" do
    test = self()

    context = %Etl.Context{
      error_handler: fn event, reason -> send(test, {:error, event.data, reason}) end
    }

    functions = [
      fn event ->
        case rem(event, 2) do
          0 -> ok(event)
          1 -> error(:odd)
        end
      end
    ]

    producer = start_supervised!({Etl.TestSource.Stage, %Etl.TestSource{pid: test}})

    function_stage = start_supervised!({Etl.Transform.Stage, context: context, functions: functions})

    consumer = start_supervised!({Etl.TestDestination.Stage, %Etl.TestDestination{pid: test}})

    GenStage.sync_subscribe(consumer, to: function_stage)
    GenStage.sync_subscribe(function_stage, to: producer)

    Etl.TestSource.send_events(producer, [1, 2, 3, 4])

    assert_receive {:error, 1, :odd}
    assert_receive {:data, 2}
    assert_receive {:error, 3, :odd}
    assert_receive {:data, 4}
  end
end
