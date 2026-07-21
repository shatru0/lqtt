defmodule Lqtt.ClusterTest do
  use ExUnit.Case

  setup do
    # Cluster is already started by the application; fetch its pid
    pid = Process.whereis(Lqtt.Cluster)
    %{cluster_pid: pid}
  end

  test "cluster process is running", %{cluster_pid: pid} do
    assert is_pid(pid)
    assert Process.alive?(pid)
  end

  test "handles nodedown gracefully", %{cluster_pid: pid} do
    send(pid, {:nodedown, :"ghost@nowhere"})
    :timer.sleep(10)
    assert Process.alive?(pid)
  end

  test "handles nodeup gracefully", %{cluster_pid: pid} do
    send(pid, {:nodeup, :"ghost@nowhere"})
    :timer.sleep(10)
    assert Process.alive?(pid)
  end
end

