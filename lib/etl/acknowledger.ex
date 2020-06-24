defmodule Etl.Acknowledger do
  @callback ack(ack_ref :: term, success :: [Etl.Message.t()], fail :: [Etl.Message.t()]) :: :ok
end
