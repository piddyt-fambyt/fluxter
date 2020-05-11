defmodule Fluxter do
  @moduledoc """
  InfluxDB writer for Elixir that uses InfluxDB's line protocol over UDP.

  To get started with Fluxter, you have to create a module that calls `use
  Fluxter`, like this:

      defmodule MyApp.Fluxter do
        use Fluxter
      end

  This way, `MyApp.Fluxter` becomes an InfluxDB connection pool. Each Fluxter
  pool provides a `c:start_link/1` function that starts that pool and connects to
  InfluxDB; this function needs to be invoked before being able to send data to
  InfluxDB. Typically, you won't call `start_link/1` directly as you'll want to
  add Fluxter pools to your application's supervision tree. For this use case,
  pools provide a `child_spec/1` function:

      def start(_type, _args) do
        children = [
          MyApp.Fluxter.child_spec(),
          # ...
        ]
        Supervisor.start_link(children, strategy: :one_for_one)
      end

  Once a Fluxter pool is started, its `c:write/2,3`, `c:measure/2,3,4`, and other
  functions can successfully be used to send points to the data store.
  A Fluxter pool implements the `Fluxter` behaviour, so you can read documentation
  for the callbacks the behaviour provides to know more about these functions.

  ## Configuration

  Fluxter can be configured either globally or on a per-pool basis.

  The global configuration will affect all Fluxter pools; it can be specified by
  configuring the `:fluxter` application:

      config :fluxter,
        host: "metrics.example.com",
        port: 1122

  The per-pool configuration can be specified by configuring the pool module
  under the `:fluxter` application:

      config :fluxter, MyApp.Fluxter,
        host: "metrics.example.com",
        port: 1122,
        pool_size: 10

  The following is a list of all the supported options:

    * `:host` - (binary) the host to send metrics to. Defaults to `"127.0.0.1"`.
    * `:port` - (integer) the port (on `:host`) to send the metrics to. Defaults
      to `8092`.
    * `:prefix` - (binary or `nil`) all metrics sent to the data store through
      the configured Fluxter pool will be prefixed by the value of this
      option. If `nil`, metrics will not be prefixed. Defaults to `nil`.
    * `:pool_size` - (integer) the size of the connection pool for the given
      Fluxter pool. **This option can only be configured on a per-pool basis**;
      configuring it globally for the `:fluxter` application has no
      effect. Defaults to `5`.

  ## Metric aggregation

  Fluxter supports counters: a counter is a metric aggregator designed to
  locally aggregate a numeric value and flush the aggregated value only once to
  the storage, as a single metric. This is very useful when you have the need to
  write a high number of metrics in a very short amount of time. Doing so can
  have a negative impact on the speed of your code and can also cause network
  packet drops.

  For example, code like the following:

      for value <- 1..1_000_000 do
        my_operation(value)
        MyApp.Fluxter.write("my_operation_success", [host: "eu-west"], 1)
      end

  can take advantage of metric aggregation:

      counter = MyApp.Fluxter.start_counter("my_operation_success", [host: "eu-west"])
      for value <- 1..1_000_000 do
        my_operation(value)
        MyApp.Fluxter.increment_counter(counter, 1)
      end
      MyApp.Fluxter.flush_counter(counter)

  """

  @type measurement :: String.Chars.t()
  @type tags :: [{String.Chars.t(), String.Chars.t()}]
  @type field_value :: number | boolean | binary
  @type fields :: [{String.Chars.t(), field_value}]
  @type timestamp :: number | nil
  @opaque counter :: pid

  @doc """
  Should be the same as `child_spec([])`.
  """
  @callback child_spec() :: Supervisor.spec()

  @doc """
  Returns a child specification for this Fluxter pool.

  This is usually used to supervise this Fluxter pool under the supervision tree
  of your application:

      def start(_type, _args) do
        children = [
          MyApp.Fluxter.child_spec([]),
          # ...
        ]
        Supervisor.start_link(children, strategy: :one_for_one)
      end

  `options` is a list of options that will be given to `c:start_link/1`.
  """
  @callback child_spec(options :: Keyword.t()) :: Supervisor.spec()

  @doc """
  Should be the same as `start_link([])`.
  """
  @callback start_link() :: Supervisor.on_start()

  @doc """
  Starts this Fluxter pool.

  A Fluxter pool is a set of processes supervised by a supervisor; this function
  starts all those processes and that supervisor.

  The following options are supported: `:host`, `:port`, and `:prefix`.

  If you plan on having a Fluxter pool started under your application's
  supervision tree, use `c:child_spec/1`.
  """
  @callback start_link(options :: Keyword.t()) :: Supervisor.on_start()

  @doc """
  Writes a metric to the data store.

  `measurement` is the name of the metric to write.
  `tags` is a list of key-value pairs that specifies tags (as name and value)
  for the data point to write; note that tag values are converted to strings
  as InfluxDB only support string values for tags.
  `fields` can either be a list of key-value pairs, in which case it
  specifies a list of fields (as name and value), or a single value
  (specifically, a boolean, float, integer, or binary). In the latter case, the
  default field name of `value` will be used: calling `write("foo", [], 4.3)` is
  the same as calling `write("foo", [], value: 4.3)`.

  The return value is always `:ok` as writing is a *fire-and-forget* operation.

  ## Examples

  Assuming a `MyApp.Fluxter` Fluxter pool exists:

      iex> MyApp.Fluxter.write("cpu_temp", [host: "eu-west"], 68)
      :ok

  """
  @callback write(measurement, tags, field_value | fields, timestamp) :: :ok

  @doc """
  Should be the same as `write(measurement, [], fields)`.
  """
  @callback write(measurement, field_value | fields, timestamp) :: :ok


  @doc false
  defmacro __using__(_opts) do
    quote unquote: false, location: :keep do
      @behaviour Fluxter

      @pool_size Application.get_env(__MODULE__, :pool_size, 5)
      @worker_names Enum.map(0..(@pool_size - 1), &:"#{__MODULE__}-#{&1}")

      def child_spec(options \\ []) do
        {child_options, options} =
          Keyword.split(options, [:id, :start, :restart, :shutdown, :type, :modules])

        if child_options != [] do
          IO.warn("passing child specification options is deprecated")
        end

        Supervisor.Spec.supervisor(__MODULE__, [options], child_options)
      end

      def start_link(options \\ []) do
        import Supervisor.Spec

        config = Fluxter.get_config(__MODULE__, options)

        conn =
          config.host
          |> Fluxter.Conn.new(config.port)
          |> Map.update!(:header, &[&1 | config.prefix])

        @worker_names
        |> Enum.map(&worker(Fluxter.Conn, [conn, &1], id: &1))
        |> Supervisor.start_link(strategy: :one_for_one)
      end

      @compile {:inline, worker_name: 1}
      for {name, index} <- Enum.with_index(@worker_names) do
        defp worker_name(unquote(index)) do
          unquote(name)
        end
      end

      def write(measurement, tags \\ [], fields, timestamp \\ nil)

      def write(measurement, tags, fields, timestamp) when is_list(fields) do
        [:positive]
        |> System.unique_integer()
        |> rem(@pool_size)
        |> worker_name()
        |> Fluxter.Conn.write(measurement, tags, fields, timestamp)
      end

      def write(measurement, tags, value, timestamp)
          when is_float(value) or is_integer(value)
          when is_boolean(value) or is_binary(value) do
        write(measurement, tags, [value: value], timestamp)
      end
    end
  end

  @doc false
  def get_config(module, overrides) do
    {module_env, global_env} =
      :fluxter
      |> Application.get_all_env()
      |> Keyword.pop(module, [])

    env = module_env ++ global_env
    options = overrides ++ env

    %{
      prefix: build_prefix(env, overrides),
      host: Keyword.get(options, :host, "127.0.0.1"),
      port: Keyword.get(options, :port, 8125)
    }
  end

  defp build_prefix(env, overrides) do
    case Keyword.fetch(overrides, :prefix) do
      {:ok, prefix} ->
        [prefix, ?_]

      :error ->
        env
        |> Keyword.get_values(:prefix)
        |> Enum.map_join(&(&1 && [&1, ?_]))
    end
  end
end
