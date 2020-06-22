defmodule Etl.Message do
  @type t :: %__MODULE__{
          data: term(),
          acknowledger: {module(), ack_ref :: term(), data :: term()},
          status: :ok | {:error, reason :: term()},
          metadata: %{optional(atom()) => term()}
        }

  defstruct data: nil,
            acknowledger: nil,
            status: :ok,
            metadata: %{}

  @spec update_data(t, (current :: term() -> new :: term())) :: t
  def update_data(%__MODULE__{data: data} = message, update_function)
      when is_function(update_function, 1) do
    %Etl.Message{message | data: update_function.(data)}
  end

  @spec mark_failed(t, reason :: term()) :: t
  def mark_failed(message, reason) do
    %Etl.Message{message | status: {:error, reason}}
  end
end
