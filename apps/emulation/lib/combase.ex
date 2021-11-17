defmodule EmulatorError do
  defexception message: "Error in emulation"
end

defmodule ComBase do
  @moduledoc """
  ComBase emulates a distribtued application connected
  over a potentially asynchronous network.
  """
  defstruct(
    registration: nil,
    rev_registration: nil,
    fuzz_chain: nil,
    times: nil,
    unfuzzables: nil
  )

  require Fuzzers
  require Logger
  require MapSet

  @doc """
  Initialize the communication base.
  """
  @spec init() :: %ComBase{
          registration: pid(),
          rev_registration: pid(),
          fuzz_chain: pid(),
          times: pid(),
          unfuzzables: pid()
        }
  def init do
    Logger.debug("Initializing ComBase")
    {:ok, pid} = Agent.start_link(fn -> %{} end)
    {:ok, rpid} = Agent.start_link(fn -> %{} end)
    {:ok, times} = Agent.start_link(fn -> %{} end)
    {:ok, unfuzzables} = Agent.start_link(fn -> MapSet.new() end)
    # By always adding 0 delay, we make sure there is a queue but
    # messages are delayed
    {:ok, fchain} = Agent.start_link(fn -> [] end)
    Logger.debug("Registration map is #{inspect(pid)}")
    Logger.debug("Reverse Registration map is #{inspect(rpid)}")
    Logger.debug("Fuzz chain is #{inspect(fchain)}")
    Logger.debug("The set of unfuzzables is #{inspect(unfuzzables)}")

    %ComBase{
      registration: pid,
      rev_registration: rpid,
      fuzz_chain: fchain,
      times: times,
      unfuzzables: unfuzzables
    }
  end

  @spec unfuzzable?(%ComBase{unfuzzables: pid()}, atom() | pid()) :: boolean()
  defp unfuzzable?(ctx, proc) do
    Agent.get(ctx.unfuzzables, fn s -> MapSet.member?(s, proc) end)
  end

  def get_unfuzzables(ctx) do
    ctx.unfuzzables
  end

  defp unlink_and_exit(pid) do
    Process.exit(pid, :stop)
  catch
    _ -> true
  end

  defp stop_linked_agent(pid) do
    Agent.stop(pid)
  catch
    :exit, _ -> true
  end

  @doc """
  Terminate communication base.
  """
  @spec terminate(%ComBase{
          registration: pid(),
          rev_registration: pid(),
          fuzz_chain: pid(),
          times: pid()
        }) :: :ok
  def terminate(ctx) do
    rev_reg = Agent.get(ctx.rev_registration, fn m -> m end)
    keys = Map.keys(rev_reg)
    Enum.each(keys, &Process.unlink/1)
    Enum.each(keys, &unlink_and_exit/1)
    stop_linked_agent(ctx.rev_registration)
    stop_linked_agent(ctx.registration)
    stop_linked_agent(ctx.fuzz_chain)
    stop_linked_agent(ctx.times)
  end

  defp get_fuzz_chain(ctx) do
    chain = Agent.get(ctx.fuzz_chain, fn f -> f end)

    if chain == [] do
      # Add a 0 delay to queue messages
      # before delivery.
      [Fuzzers.delay(0)]
    else
      chain
    end
  end

  defp create_fuzzers(ctx, sender) do
    my_pid = self()
    my_id = whoami(ctx)

    fuzz_chain =
      if unfuzzable?(ctx, my_id) || unfuzzable?(ctx, my_pid) do
        []
      else
        get_fuzz_chain(ctx)
      end

    Fuzzers.build_fuzz_chain(my_pid, my_id, sender, fuzz_chain)
  end

  defp recv_proxy_getq(ctx, queues, sender) do
    q = Agent.get(queues, fn q -> q[sender] end)

    if q do
      q
    else
      q = create_fuzzers(ctx, sender)
      Agent.update(queues, fn m -> Map.put(m, sender, q) end)
      q
    end
  end

  defp recv_proxy_internal(ctx, proc, queues) do
    receive do
      {:control, m} ->
        q = recv_proxy_getq(ctx, queues, whoami(ctx))
        send(q, {:control_proc, m})

      {:msg, sender, msg} ->
        # The start of the chain.
        if unfuzzable?(ctx, sender) do
          # Do not fuzz messages from an unfuzzable sender
          send(proc, {sender, msg})
        else
          q = recv_proxy_getq(ctx, queues, sender)
          send(q, {:proc, sender, msg})
        end

      {:proc, sender, msg} ->
        send(proc, {sender, msg})

      {:control_proc, msg} ->
        send(proc, msg)

      m ->
        Logger.error(
          "Process #{whoami(ctx)} received message #{inspect(m)} " <>
            "that was not sent using emultion"
        )

        raise EmulatorError, message: "Message not sent using emulation"
    end

    recv_proxy_internal(ctx, proc, queues)
  end

  defp recv_proxy(ctx, proc) do
    {:ok, pid} = Agent.start(fn -> %{} end)
    recv_proxy_internal(ctx, proc, pid)
  end

  @doc """
  Get ID for the current process.
  """
  @spec whoami(%ComBase{rev_registration: pid()}) :: atom() | pid()
  def whoami(ctx) do
    pid = self()

    case Agent.get(ctx.rev_registration, fn r -> r[pid] end) do
      nil ->
        Logger.info("Process #{inspect(pid)} is not registered.")
        self()

      id ->
        id
    end
  end

  @doc """
  Add to the list of fuzzers used when messages are received. Note
  this function must be called before any messages are sent.
  """
  @spec append_fuzzers(%ComBase{fuzz_chain: pid()}, [{atom(), float() | pid()}]) ::
          :ok
  def append_fuzzers(ctx, fuzzer_list) do
    Agent.update(ctx.fuzz_chain, fn f -> f ++ fuzzer_list end)
  end

  @doc """
  Get the current fuzzer list
  """
  @spec get_fuzzers(%ComBase{fuzz_chain: pid()}) :: [{atom(), float() | pid()}]
  def get_fuzzers(ctx) do
    Agent.get(ctx.fuzz_chain, fn f -> f end)
  end

  @doc """
  Send a message to the process named proc. Message can be
  anything. ctx should be the context in which the process
  was created.
  """
  @spec send(
          %ComBase{registration: pid(), rev_registration: pid()},
          atom() | pid(),
          any()
        ) ::
          boolean()
  def send(ctx, proc, msg) do
    p =
      if is_pid(proc) do
        proc
      else
        Agent.get(ctx.registration, fn r -> r[proc] end)
      end

    src = whoami(ctx)

    internal = Agent.get(ctx.rev_registration, fn r -> r[p] != nil end)

    if p do
      if internal do
        send(p, {:msg, src, msg})
        true
      else
        send(p, msg)
        true
      end
    else
      Logger.warn(
        "Could not translate #{inspect(proc)} into a PID, " <>
          "unable to send."
      )

      false
    end
  end

  @doc """
  Mark this process as one whose messages (i.e.,
  those sent or received by the process) should not
  be fuzzed. This is meant as an aid to testing, and
  should not be used by code not within a test.
  """
  @spec mark_unfuzzable(%ComBase{unfuzzables: pid()}) :: :ok
  def mark_unfuzzable(ctx) do
    p = whoami(ctx)
    Agent.update(ctx.unfuzzables, fn s -> MapSet.put(s, p) end)
  end

  @doc """
  Get a list of all registed processes
  """
  @spec list_proc(%ComBase{registration: pid()}) :: [atom()]
  def list_proc(ctx) do
    Agent.get(ctx.registration, fn m -> Map.keys(m) end)
  end

  @doc """
  Send message to all active processes.
  """
  @spec broadcast(%ComBase{registration: pid(), rev_registration: pid()}, any()) ::
          boolean()
  def broadcast(ctx, msg) do
    src = whoami(ctx)

    #  We use send_after here to more closely resemble
    #  the execution model presented in class.
    Agent.get(ctx.registration, fn
      r ->
        Enum.map(Map.values(r), fn dst ->
          Process.send_after(dst, {:msg, src, msg}, 0)
        end)
    end)
  end

  # We need to wait until the Spawned process
  # has finished registration, else there is a
  # race condition.
  defp spawn_helper(func) do
    receive do
      :go ->
        func.()

      _ ->
        # Messages sent before the process is active
        # just get dropped.
        spawn_helper(func)
    end
  end

  @doc """
  Spawn a process with supplied name and function.
  """
  @spec spawn(
          %ComBase{registration: pid(), rev_registration: pid(), times: pid()},
          atom(),
          (() -> any())
        ) :: pid()
  def spawn(ctx, name, f) do
    %{registration: p, rev_registration: r, times: t} = ctx

    if Agent.get(p, fn m -> Map.has_key?(m, name) end) do
      Logger.error(
        "Tried to spawn process with name #{inspect(name)} which is " <>
          " already registered."
      )

      raise EmulatorError, message: "Cannot spawn processes with the same name"
    else
      pid = spawn_link(fn -> spawn_helper(f) end)
      proxy_pid = spawn_link(fn -> recv_proxy(ctx, pid) end)
      Agent.update(p, fn m -> Map.put(m, name, proxy_pid) end)

      Agent.update(r, fn m ->
        Map.put(Map.put(m, proxy_pid, name), pid, name)
      end)

      Agent.update(t, fn m ->
        Map.put(m, name, System.monotonic_time())
      end)

      # Now we are registered, it is safe to start.
      send(pid, :go)

      Logger.debug(
        "Spawned #{inspect(name)} with " <>
          "main process: #{inspect(pid)} " <>
          "proxy: #{inspect(proxy_pid)}"
      )

      pid
    end
  end

  @doc """
  Set a timer for the given number of milliseconds, and send a message
  with the atom :timer when done.
  """
  @spec timer(
          %ComBase{
            rev_registration: pid(),
            registration: pid()
          },
          non_neg_integer()
        ) :: reference()
  def timer(ctx, ms) do
    timer(ctx, ms, :timer)
  end

  @doc """
  Set a timer for the given number of milliseconds, and send a message
  with the atom `message` when done.
  """
  @spec timer(
          %ComBase{
            rev_registration: pid(),
            registration: pid()
          },
          non_neg_integer(),
          atom()
        ) :: reference()
  def timer(ctx, ms, atom) do
    src = whoami(ctx)
    p = Agent.get(ctx.registration, fn r -> r[src] end)

    if p do
      Process.send_after(p, {:control, atom}, ms)
    else
      Logger.error(
        "Process #{inspect(self())} outside emultion tried setting " <>
          "timer"
      )

      raise EmulatorError, message: "Timer set by process not in emulations"
    end
  end

  # Get current time at process `process`.
  @spec time_at_process(%ComBase{times: pid()}, atom()) :: number()
  defp time_at_process(ctx, process) do
    System.monotonic_time() - Agent.get(ctx.times, fn m -> m[process] end)
  end

  @doc """
  Translate `time` (gotten from `System.monotonic_time()`) to time
  at process `p`. This should only be used for testing.
  """
  @spec translate_time(%ComBase{times: pid()}, atom(), number()) :: number()
  def translate_time(ctx, p, time) do
    time - Agent.get(ctx.times, fn m -> m[p] end)
  end

  @doc """
  Get current time.
  """
  @spec now(%ComBase{times: pid()}) :: number()
  def now(ctx) do
    me = whoami(ctx)

    if is_pid(me) do
      raise(EmulatorError, message: "Process #{inspect(me)} " <> "
             outside emulation asking for time")
    else
      time_at_process(ctx, me)
    end
  end

  @doc """
  Set current time.
  """
  @spec set_time(%ComBase{times: pid()}, number()) :: :ok
  def set_time(ctx, time) do
    me = whoami(ctx)

    if is_pid(me) do
      raise(EmulatorError, message: "Process #{inspect(me)} " <> "
             outside emulation asking for time")
    else
      base = System.monotonic_time() - time
      Agent.update(ctx.times, fn m -> Map.put(m, me, base) end)
      :ok
    end
  end
end