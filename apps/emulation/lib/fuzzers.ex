defmodule Fuzzers do
    @moduledoc """
    Fuzzers is a set of network fuzzing tools.
    """
    @delay :delay
    @drop :drop
    @record_drop :rec_drop
    @fail :fail
    @schedule :schedule
    @linearize :linearize
    @guarantee_delay :always_delay
  
    alias Statistics.Distributions.Exponential
  
    @spec random_delay(number()) :: number()
    defp random_delay(mean) do
      if mean > 0 do
        Exponential.rand(1.0 / mean)
        |> Float.round()
        |> trunc
      else
        0
      end
    end
  
    @spec always_delay(pid(), atom() | pid(), any(), number()) :: no_return()
    defp always_delay(recver, id, q, amount) do
      receive do
        {:proc, ^id, msg} ->
          nq = :queue.in(msg, q)
  
          if :queue.is_empty(q) do
            Process.send_after(self(), :time, amount)
          end
  
          inject_delay(recver, id, nq, amount)
  
        {:control_proc, msg} ->
          send(recver, {:control_proc, msg})
          inject_delay(recver, id, q, amount)
  
        :time ->
          case :queue.out(q) do
            {{:value, msg}, nq} ->
              send(recver, {:proc, id, msg})
  
              unless :queue.is_empty(nq) do
                Process.send_after(self(), :time, amount)
              end
  
              inject_delay(recver, id, nq, amount)
          end
  
        m ->
          raise EmulatorError,
            message: "Message not sent using emulation #{inspect(m)}"
      end
    end
  
    defp always_delay(receiver, id, amount) do
      always_delay(receiver, id, :queue.new(), amount)
    end
  
    @spec inject_delay(pid(), atom() | pid(), any(), number()) :: no_return()
    defp inject_delay(recver, id, q, mean) do
      receive do
        {:proc, ^id, msg} ->
          nq = :queue.in(msg, q)
  
          if :queue.is_empty(q) do
            Process.send_after(self(), :time, random_delay(mean))
          end
  
          inject_delay(recver, id, nq, mean)
  
        {:control_proc, msg} ->
          send(recver, {:control_proc, msg})
          inject_delay(recver, id, q, mean)
  
        :time ->
          case :queue.out(q) do
            {{:value, msg}, nq} ->
              send(recver, {:proc, id, msg})
  
              unless :queue.is_empty(nq) do
                Process.send_after(self(), :time, random_delay(mean))
              end
  
              inject_delay(recver, id, nq, mean)
          end
  
        m ->
          raise EmulatorError,
            message: "Message not sent using emulation #{inspect(m)}"
      end
    end
  
    defp inject_delay(receiver, id, mean) do
      inject_delay(receiver, id, :queue.new(), mean)
    end
  
    defp random_drop(recver, id, p) do
      receive do
        {:proc, ^id, msg} ->
          if :rand.uniform() >= p do
            send(recver, {:proc, id, msg})
          end
  
        {:control_proc, msg} ->
          send(recver, {:control_proc, msg})
  
        m ->
          raise EmulatorError,
            message: "Message not sent using emulation #{inspect(m)}"
      end
  
      random_drop(recver, id, p)
    end
  
    defp random_drop_record(recver, recver_id, sender_id, p, recorder) do
      receive do
        {:proc, ^sender_id, msg} ->
          if :rand.uniform() >= p do
            send(recver, {:proc, sender_id, msg})
          else
            send(recorder, {:drop, recver_id, sender_id, msg})
          end
  
        {:control_proc, msg} ->
          send(recver, {:control_proc, msg})
  
        m ->
          raise EmulatorError,
            message: "Message not sent using emulation #{inspect(m)}"
      end
  
      random_drop_record(recver, recver_id, sender_id, p, recorder)
    end
  
    defp check_fail(recver, recver_id, sender_id, status) do
      receive do
        {:proc, ^sender_id, msg} ->
          if Agent.get(status, fn s -> s[recver_id] end) do
            IO.puts("Dropping message to failed node")
          else
            send(recver, {:proc, sender_id, msg})
          end
  
        {:control_proc, msg} ->
          send(recver, {:control_proc, msg})
  
        m ->
          raise EmulatorError,
            message: "Message not sent using emulation #{inspect(m)}"
      end
  
      check_fail(recver, recver_id, sender_id, status)
    end
  
    defp schedule_messages(recver, recver_id, sender_id, checker, tokens, msgs) do
      receive do
        {:proc, ^sender_id, msg} ->
          if tokens >= 1 do
            send(recver, {:proc, sender_id, msg})
            send(checker, {:consumed_token, recver_id, sender_id})
  
            schedule_messages(
              recver,
              recver_id,
              sender_id,
              checker,
              tokens - 1,
              msgs
            )
          else
            schedule_messages(
              recver,
              recver_id,
              sender_id,
              checker,
              tokens,
              :queue.in(msg, msgs)
            )
          end
  
        {:control_proc, msg} ->
          send(recver, {:control_proc, msg})
          schedule_messages(recver, recver_id, sender_id, checker, tokens, msgs)
  
        {:inc_token, recver, ^sender_id} ->
          if :queue.is_empty(msgs) do
            schedule_messages(
              recver,
              recver_id,
              sender_id,
              checker,
              tokens + 1,
              msgs
            )
          else
            {{:value, msg}, new_msgs} = :queue.out(msgs)
            send(recver, {:proc, sender_id, msg})
            send(checker, {:consumed_token, recver_id, sender_id})
  
            schedule_messages(
              recver,
              recver_id,
              sender_id,
              checker,
              tokens,
              new_msgs
            )
          end
  
        m ->
          raise EmulatorError,
            message: "Message not sent through emulation #{inspect(m)}"
      end
    end
  
    # Linearize message delivery.
    defp linearize_messages(recver, recver_id, sender_id, checker, msgs) do
      receive do
        {:proc, ^sender_id, msg} ->
          # Make sure we send after adding to the queue
          Process.send_after(
            checker,
            {:recved, self(), recver_id, sender_id, msg},
            0
          )
  
          linearize_messages(
            recver,
            recver_id,
            sender_id,
            checker,
            :queue.in({:msg, msg}, msgs)
          )
  
        {:release, ^sender_id} ->
          {{:value, msg}, new_msgs} = :queue.out(msgs)
  
          case msg do
            {:msg, msg} -> send(recver, {:proc, sender_id, msg})
            {:ctl, msg} -> send(recver, {:control_proc, msg})
          end
  
          linearize_messages(recver, recver_id, sender_id, checker, new_msgs)
  
        {:control_proc, msg} ->
          Process.send_after(
            checker,
            {:recved, self(), recver_id, sender_id, msg},
            0
          )
  
          linearize_messages(
            recver,
            recver_id,
            sender_id,
            checker,
            :queue.in({:ctl, msg}, msgs)
          )
  
        m ->
          raise EmulatorError,
            message: "Message not sent through emulation #{inspect(m)}"
      end
    end
  
    defp message_logger(q) do
      receive do
        {:register, _, _, _, _} ->
          message_logger(q)
  
        {:recved, ctrl, recver_id, sender_id, msg} ->
          # Send after recording
          Process.send_after(ctrl, {:release, sender_id}, 0)
          message_logger(:queue.in({:recved, recver_id, sender_id, msg}, q))
  
        {:drop, recver_id, sender_id, msg} ->
          message_logger(:queue.in({:dopped, recver_id, sender_id, msg}, q))
  
        {:dump, where} ->
          send(where, {:logs, :queue.to_list(q)})
          message_logger(q)
  
        m ->
          raise EmulatorError,
            message: "Message logger received unexpected message #{inspect(m)}"
      end
    end
  
    defp linearize_messages(recver, recver_id, sender_id, checker) do
      send(checker, {:register, recver, recver_id, sender_id, self()})
      linearize_messages(recver, recver_id, sender_id, checker, :queue.new())
    end
  
    defp schedule_messages(recver, recver_id, sender_id, checker) do
      send(checker, {:register, recver_id, sender_id, self()})
      schedule_messages(recver, recver_id, sender_id, checker, 0, :queue.new())
    end
  
    defp select_fuzz(f, recver, recver_id, sender_id) do
      case f do
        {@delay, t} when is_number(t) ->
          spawn_link(fn -> inject_delay(recver, sender_id, t) end)
  
        {@guarantee_delay, t} when is_number(t) ->
          spawn_link(fn -> always_delay(recver, sender_id, t) end)
  
        {@drop, p} when is_number(p) ->
          spawn_link(fn -> random_drop(recver, sender_id, p) end)
  
        {@record_drop, {p, pid}} ->
          spawn_link(fn ->
            random_drop_record(recver, recver_id, sender_id, p, pid)
          end)
  
        {@fail, s} when is_pid(s) ->
          spawn_link(fn -> check_fail(recver, recver_id, sender_id, s) end)
  
        {@schedule, c} when is_pid(c) ->
          spawn_link(fn -> schedule_messages(recver, recver_id, sender_id, c) end)
  
        {@linearize, c} when is_pid(c) ->
          spawn_link(fn -> linearize_messages(recver, recver_id, sender_id, c) end)
  
        _ ->
          raise EmulatorError, message: "Unrecognized fuzzer #{inspect(f)}"
      end
    end
  
    @doc """
    Return a list of message logs from a recording.
    """
    @spec get_logs({:linearize, pid()}) :: [
            {atom() | pid(), atom() | pid(), any()}
          ]
    def get_logs({@linearize, proc}) do
      send(proc, {:dump, self()})
  
      receive do
        {:logs, log} ->
          log
  
        o ->
          raise EmulatorError,
            message: "Unexpected message while waiting for logs. #{inspect(o)}"
      end
    end
  
    @doc """
    Add a fuzzer that logs messages. Use get_logs/1 to
    retrieve the messages.
    """
    @spec log_messages() :: {:linearize, pid()}
    def log_messages do
      logger = spawn_link(fn -> message_logger(:queue.new()) end)
      {@linearize, logger}
    end
  
    @doc """
    Fuzzer for delaying messages. Guaranteed delays
    """
    @spec guarantee_delay(number()) :: {:guarantee_delay, float()}
    def guarantee_delay(t) do
      {@guarantee_delay, t}
    end
  
    @doc """
    Fuzzer for delaying messages. Delays are drawn from
    an exponential distribution with the mean given here.
    """
    @spec delay(number()) :: {:delay, float()}
    def delay(t) do
      {:delay, t}
    end
  
    @doc """
    Fuzzer for dropping packets. Drops are uniformly random
    with supplied probability.
    """
    @spec drop(float()) :: {:drop, float()}
    def drop(prob) do
      {@drop, prob}
    end
  
    @doc """
    Add a fuzzer that drops messages, and records
    them in the supplied logger.
    """
    @spec drop_record(float(), {:linearize, pid()}) ::
            {:rec_drop, {float(), pid()}}
    def drop_record(p, {@linearize, pid}) do
      {@record_drop, {p, pid}}
    end
  
    @doc """
    Build a chain of network fuzzing steps.
    """
    @spec build_fuzz_chain(pid(), pid() | atom(), pid() | atom(), [
            {atom(), float() | pid() | {float(), pid()}}
          ]) ::
            pid()
    def build_fuzz_chain(recver, recver_id, sender_id, chain) do
      chain
      |> Enum.reverse()
      |> Enum.reduce(recver, fn x, acc ->
        select_fuzz(x, acc, recver_id, sender_id)
      end)
    end
  end