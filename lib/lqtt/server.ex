defmodule Lqtt.Server do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    port = Keyword.fetch!(opts, :port)

    listen_opts = [:binary, packet: :raw, active: false, reuseaddr: true]

    case :gen_tcp.listen(port, listen_opts) do
      {:ok, listen_sock} ->
        Logger.info("MQTT broker listening on port #{port}")
        state = %{
          listen_sock: listen_sock,
          authenticator: Lqtt.Authenticator.allow_all(),
          acl: Lqtt.ACL.allow_all(),
          clients: %{}
        }
        start_acceptor(self(), listen_sock)
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp start_acceptor(server_pid, listen_sock) do
    spawn_link(fn ->
      case :gen_tcp.accept(listen_sock) do
        {:ok, client_sock} ->
          pid = spawn_link(fn -> Lqtt.ClientHandler.handle(client_sock, server_pid) end)
          :gen_tcp.controlling_process(client_sock, pid)
          send(server_pid, :accept_done)
          start_acceptor(server_pid, listen_sock)

        {:error, _reason} ->
          :ok
      end
    end)
  end

  @impl true
  def handle_info(:accept_done, state) do
    {:noreply, state}
  end

  @impl true
  def handle_call({:register_client, client_id, sock_pid}, _from, state) do
    clients = Map.put(state.clients, client_id, sock_pid)
    {:reply, :ok, %{state | clients: clients}}
  end

  @impl true
  def handle_call({:unregister_client, client_id}, _from, state) do
    clients = Map.delete(state.clients, client_id)
    {:reply, :ok, %{state | clients: clients}}
  end

  @impl true
  def handle_call({:forward, client_id, packet}, _from, state) do
    case Map.fetch(state.clients, client_id) do
      {:ok, sock_pid} ->
        send(sock_pid, {:send, packet})
        {:reply, :ok, state}

      :error ->
        forward_remote(client_id, packet)
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:subscribe, topic, client_id, qos}, _from, state) do
    Lqtt.Route.Mnesia.subscribe(topic, {node(), client_id}, qos)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:unsubscribe, topic, client_id}, _from, state) do
    Lqtt.Route.Mnesia.unsubscribe(topic, {node(), client_id})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:unsubscribe_all, client_id}, _from, state) do
    Lqtt.Route.Mnesia.delete_client_routes(client_id)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:match, topic}, _from, state) do
    {:atomic, subs} = Lqtt.Route.Mnesia.match_routes(topic)
    {:reply, subs, state}
  end

  @impl true
  def handle_call({:authenticate, username, password}, _from, state) do
    result = state.authenticator.(username, password)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:can_publish, username, topic}, _from, state) do
    result = state.acl.(:publish, username, topic)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:can_subscribe, username, topic}, _from, state) do
    result = state.acl.(:subscribe, username, topic)
    {:reply, result, state}
  end

  @impl true
  def handle_cast({:forward_local, client_id, packet}, state) do
    Logger.info("forward_local: client_id=#{client_id} clients=#{inspect(Map.keys(state.clients))}")
    case Map.fetch(state.clients, client_id) do
      {:ok, sock_pid} ->
        send(sock_pid, {:send, packet})
      :error ->
        :ok
    end

    {:noreply, state}
  end

  defp forward_remote(client_id, packet) do
    nodes = Node.list()
    Logger.info("forward_remote: client_id=#{client_id} nodes=#{inspect(nodes)}")
    for node <- nodes do
      :rpc.cast(node, __MODULE__, :forward_local, [client_id, packet])
    end
  end

  def forward_local(client_id, packet) do
    GenServer.cast(__MODULE__, {:forward_local, client_id, packet})
  end
end
