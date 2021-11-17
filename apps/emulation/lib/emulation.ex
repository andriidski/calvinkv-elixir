defmodule Emulation do
    @moduledoc """
    This module is a wrapper around ComBase that simplifies
    its use in student code. It does so by saving the context
    as a named Agent, and retrieving it when necessary.
    """
    require ComBase
    require Fuzzers
    @context :emu_context
  
    @doc """
    Setup the emulation
    """
    @spec init() :: {:ok, pid()}
    def init do
      try do
        Agent.stop(@context)
      catch
        :exit, _ -> true
      end
  
      Agent.start_link(fn -> ComBase.init() end, name: @context)
    end
  
    @doc """
    Teardown emulation
    """
    def terminate do
      ComBase.terminate(get_context())
  
      try do
        Agent.stop(@context)
      catch
        :exit, _ -> true
      end
    end
  
  
    def get_unfuzzables do
      ComBase.get_unfuzzables(get_context())
    end
  
    defp get_context do
      Agent.get(@context, fn c -> c end)
    end
  
    @doc """
    Get ID for the current process.
    """
    @spec whoami() :: atom() | pid()
    def whoami do
      ComBase.whoami(get_context())
    end
  
    @doc """
    Add to the list of fuzzers used when messages are received. Note
    this function must be called before any messages are sent.
    """
    @spec append_fuzzers([{atom(), float() | pid()}]) ::
            :ok
    def append_fuzzers(fuzzer_list) do
      ComBase.append_fuzzers(get_context(), fuzzer_list)
    end
  
    @doc """
    Get the current fuzzer list
    """
    @spec get_fuzzers() :: [{atom(), float() | pid()}]
    def get_fuzzers do
      ComBase.get_fuzzers(get_context())
    end
  
    @doc """
    Send a message to the process named proc. Message can be
    anything. ctx should be the context in which the process
    was created.
    """
    @spec send(
            atom() | pid(),
            any()
          ) ::
            boolean()
    def send(proc, msg) do
      ComBase.send(get_context(), proc, msg)
    end
  
    @doc """
    Get a list of all registed processes
    """
    @spec list_proc() :: [atom()]
    def list_proc do
      ComBase.list_proc(get_context())
    end
  
    @doc """
    Send message to all active processes.
    """
    @spec broadcast(any()) :: boolean()
    def broadcast(msg) do
      ComBase.broadcast(get_context(), msg)
    end
  
    @doc """
    Spawn a process with supplied name and function.
    """
    @spec spawn(
            atom(),
            (() -> any())
          ) :: pid()
    def spawn(name, f) do
      ComBase.spawn(get_context(), name, f)
    end
  
    @doc """
    Set a timer for `ms` milliseconds, and send a message
    to the calling process with the atom `:timer` when done.
    """
    @spec timer(non_neg_integer()) :: reference()
    def timer(ms) do
      ComBase.timer(get_context(), ms)
    end
  
    @doc """
    Set a timer for `ms` milliseconds, and send a message to
    the calling process  with `atom` when done.
    """
    @spec timer(non_neg_integer(), atom()) :: reference()
    def timer(ms, atom) do
      ComBase.timer(get_context(), ms, atom)
    end
  
    @doc """
    Get current time.
    """
    @spec now() :: number()
    def now do
      ComBase.now(get_context())
    end
  
    @doc """
    Set current time.
    """
    @spec set_time(number()) :: :ok
    def set_time(time) do
      ComBase.set_time(get_context(), time)
    end
  
    @doc """
    Mark this process as one whose messages (i.e.,
    those sent or received by the process) should not
    be fuzzed. This is meant as an aid to testing, and
    should not be used by code not within a test.
    """
    @spec mark_unfuzzable() :: :ok
    def mark_unfuzzable do
      ComBase.mark_unfuzzable(get_context())
    end
  
    @doc """
    Convert emulation time units (e.g., from succesive calls to `now`)
    into milliseconds. This is useful for human readable output.
    """
    @spec emu_to_millis(number()) :: number()
    def emu_to_millis(time) do
      System.convert_time_unit(round(time), :native, :millisecond)
    end
  
    @doc """
    Convert milliseconds to emulation time units. This is useful when
    providing estimates.
    """
    @spec millis_to_emu(number()) :: number()
    def millis_to_emu(time) do
      System.convert_time_unit(round(time), :millisecond, :native)
    end
  
    @doc """
    Convert emulation time units (e.g., from succesive calls to `now`)
    into microsecons. This is useful for human readable output.
    """
    @spec emu_to_micros(number()) :: number()
    def emu_to_micros(time) do
      System.convert_time_unit(round(time), :native, :microsecond)
    end
  
    @doc """
    Translate `time` (gotten from `System.monotonic_time()`) to time
    at process `p`. This should only be used for testing.
    """
    @spec translate_time(atom(), number()) :: number()
    def translate_time(p, time) do
      ComBase.translate_time(get_context(), p, time)
    end
  
    @doc """
    Cancel a timer previously set using either `timer/1` or
    `timer/2`. `timer` is the reference returned by those
    calls. Returns the number of milliseconds left on the
    timer or `false` if the timer had already expired. 
    """
    @spec cancel_timer(reference()) :: number() | false
    def cancel_timer(timer) do
      Process.cancel_timer(timer)
    end
  end