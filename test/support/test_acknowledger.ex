defmodule Etl.TestAcknowledger do
  @behaviour Etl.Acknowledger

  def ack(ref_pid, success, fail) do
    send(ref_pid, %{success: length(success), fail: length(fail)})
    :ok
  end
end
