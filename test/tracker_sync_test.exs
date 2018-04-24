defmodule Swarm.TrackerTests do
  use ExUnit.Case, async: false

  import Swarm.Entry
  alias Swarm.IntervalTreeClock, as: Clock
  alias Swarm.Registry, as: Registry

  @moduletag :capture_log

  setup_all do
    :rand.seed(:exs64)
    Application.ensure_all_started(:swarm)
    {:ok, _} = MyApp.WorkerSup.start_link()
    :ok
  end

  setup do
    {:ok, pid} = MyApp.WorkerSup.register()
    meta = %{mfa: {MyApp.WorkerSup, :register, []}}
    name = random_name()

    {lclock, rclock} = Clock.fork(Clock.seed())
    send_sync_request(lclock, [])

    entry(name: _, pid: _, ref: _, meta: _, clock: lclock) = call_track(name, pid, meta)
    
    # fake handle_replica_event which would join the clocks so that they are in sync
    rclock = Clock.join(rclock, Clock.peek(lclock))

    [name: name, pid: pid, meta: meta, lclock: lclock, rclock: rclock]
  end

  test ":sync should add missing registration", %{pid: pid, meta: meta, rclock: rclock} do
    name = random_name()
    remote_registry = [
      entry(name: name, pid: pid, ref: nil, meta: meta, clock: Clock.peek(rclock))
    ]
    send_sync_request(rclock, remote_registry)
    
    assert entry(name: ^name, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
  end

  test ":sync with same pid and remote clock dominates should update the meta", %{name: name, pid: pid, meta: meta, rclock: rclock} do
    rclock = Clock.event(rclock)
    new_meta = %{new: "meta"}
    remote_registry = [
      entry(name: name, pid: pid, ref: nil, meta: new_meta, clock: Clock.peek(rclock))
    ]
    send_sync_request(rclock, remote_registry)
    
    assert entry(name: ^name, pid: ^pid, ref: _, meta: ^new_meta, clock: _) = Registry.get_by_name(name)
  end

  test ":sync with same pid and local clock dominates should ignore entry", %{name: name, pid: pid, meta: meta, rclock: rclock} do
    Swarm.Tracker.remove_meta(:mfa, pid)

    remote_registry = [
      entry(name: name, pid: pid, ref: nil, meta: meta, clock: Clock.peek(rclock))
    ]
    send_sync_request(rclock, remote_registry)
    
    assert entry(name: ^name, pid: ^pid, ref: _, meta: %{}, clock: _) = Registry.get_by_name(name)
  end

  test ":sync with same pid and clock should ignore entry", %{name: name, pid: pid, meta: meta, rclock: rclock} do
    new_meta = %{new: "meta"}
    remote_registry = [
      entry(name: name, pid: pid, ref: nil, meta: new_meta, clock: Clock.peek(rclock))
    ]
    send_sync_request(rclock, remote_registry)
    
    assert entry(name: ^name, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
  end

  test ":sync with same pid and concurrent changes should merge data", %{name: name, pid: pid, meta: meta, rclock: rclock} do
    Swarm.Tracker.add_meta(:new_local, "meta_local", pid)

    rclock = Clock.event(rclock)
    remote_registry = [
      entry(name: name, pid: pid, ref: nil, meta: %{new_remote: "remote_meta"}, clock: Clock.peek(rclock))
    ]
    send_sync_request(rclock, remote_registry)
    
    assert entry(name: ^name, pid: ^pid, ref: _, meta: %{}, clock: _) = Registry.get_by_name(name)
  end

  test ":sync with different pid and local clock dominates should kill remote pid", %{name: name, pid: pid, meta: meta, rclock: rclock} do
    Swarm.Tracker.remove_meta(:mfa, pid)

    {:ok, remote_pid} = MyApp.WorkerSup.register()
    remote_registry = [
      entry(name: name, pid: remote_pid, ref: nil, meta: meta, clock: Clock.peek(rclock))
    ]
    send_sync_request(rclock, remote_registry)
    
    assert Process.alive?(pid)
    refute Process.alive?(remote_pid)
  end

  test ":sync with different pid and remote clock dominates should kill local pid", %{name: name, pid: pid, meta: meta, rclock: rclock} do
    {:ok, remote_pid} = MyApp.WorkerSup.register()
    rclock = Clock.event(rclock)
    remote_registry = [
      entry(name: name, pid: remote_pid, ref: nil, meta: meta, clock: Clock.peek(rclock))
    ]
    send_sync_request(rclock, remote_registry)
    
    refute Process.alive?(pid)
    assert Process.alive?(remote_pid)
  end

  defp random_name() do
    :rand.uniform()
  end

  defp call_track(name, pid, meta) do
    Swarm.Tracker.track(name, pid)
    Enum.each meta, fn {k, v} ->
      Swarm.Tracker.add_meta(k, v, pid)
    end
    Registry.get_by_name(name)
  end

  defp send_sync_request(clock, registry) do  
    GenServer.cast(Swarm.Tracker, {:sync, self(), clock})
    GenServer.cast(Swarm.Tracker, {:sync_ack, self(), clock, registry})
    # get_state to wait for the sync to be completed
    :sys.get_state(Swarm.Tracker)
  end
end
