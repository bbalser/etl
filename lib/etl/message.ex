defmodule Etl.Message do
  @type data :: term()

  @type t :: %__MODULE__{
          data: data(),
          acknowledger: {module(), ack_ref :: term(), data :: term()},
          status: :ok | {:error, reason :: term()},
          metadata: %{optional(atom()) => term()}
        }

  defstruct data: nil,
            acknowledger: nil,
            status: :ok,
            metadata: %{}

  @spec update_data(t, (current :: term() -> new :: term())) :: t
  def update_data(%__MODULE__{data: data} = message, update_function) when is_function(update_function, 1) do
    %Etl.Message{message | data: update_function.(data)}
  end

  def add_metadata(%__MODULE__{metadata: metadata} = message, key, term) do
    %Etl.Message{message | metadata: Map.put_new(metadata, key, term)}
  end

  @spec mark_failed(t, reason :: term()) :: t
  def mark_failed(message, reason) do
    %Etl.Message{message | status: {:error, reason}}
  end
end
