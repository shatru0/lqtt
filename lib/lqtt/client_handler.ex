defmodule Lqtt.ClientHandler do
  require Logger

  def handle(sock, server_pid) do
    :inet.setopts(sock, active: :once, packet: :raw)

    receive do
      {:tcp, ^sock, data} ->
        case Mqttc.Packet.decode(data) do
          {:ok, %Mqttc.Packet.Connect{} = connect_packet, rest} ->
            handle_connect(sock, server_pid, connect_packet, rest)

          {:ok, _other} ->
            Logger.error("expected CONNECT, got something else")
            :gen_tcp.close(sock)

          {:error, reason} ->
            Logger.error("read connect packet: #{inspect(reason)}")
            :gen_tcp.close(sock)
        end
    end
  end

  defp handle_connect(sock, server_pid, connect_packet, rest) do
    client_id = connect_packet.client_id
    username = connect_packet.username

    Logger.info(
      "client #{client_id} connected (clean=#{connect_packet.clean_start}, keepalive=#{connect_packet.keep_alive})"
    )

    auth_result =
      GenServer.call(server_pid, {:authenticate, username, connect_packet.password})

    unless auth_result do
      Logger.info("client #{client_id} authentication failed")
      connack = %Mqttc.Packet.Connack{
        session_present: false,
        reason_code: {:refused, :bad_user_name_or_password}
      }
      send_packet(sock, connack)
      :gen_tcp.close(sock)
      :ok
    else
      if connect_packet.clean_start do
        GenServer.call(server_pid, {:unsubscribe_all, client_id})
      end

      connack = %Mqttc.Packet.Connack{
        session_present: false,
        reason_code: :success
      }
      send_packet(sock, connack)

      GenServer.call(server_pid, {:register_client, client_id, self()})

      process_buffer(sock, server_pid, client_id, username, rest)
    end
  end

  defp loop(sock, server_pid, client_id, username, buffer) do
    receive do
      {:tcp, ^sock, data} ->
        buffer = buffer <> data
        process_buffer(sock, server_pid, client_id, username, buffer)

      {:tcp_closed, ^sock} ->
        Logger.info("client #{client_id} disconnected")
        :ok

      {:send, packet} ->
        send_packet(sock, packet)
        :inet.setopts(sock, active: :once)
        loop(sock, server_pid, client_id, username, buffer)
    end
  end

  defp process_buffer(sock, server_pid, client_id, username, buffer) do
    case Mqttc.Packet.decode(buffer) do
      {:ok, packet, rest} ->
        handle_packet(sock, server_pid, client_id, username, packet)
        process_buffer(sock, server_pid, client_id, username, rest)

      {:error, :incomplete, _acc} ->
        :inet.setopts(sock, active: :once)
        loop(sock, server_pid, client_id, username, buffer)

      {:error, reason} ->
        Logger.error("client #{client_id} decode error: #{inspect(reason)}")
        :gen_tcp.close(sock)
    end
  end

  defp handle_packet(sock, server_pid, client_id, username, packet)

  defp handle_packet(sock, server_pid, client_id, username, %Mqttc.Packet.Publish{} = pub) do
    allowed = GenServer.call(server_pid, {:can_publish, username, pub.topic})

    unless allowed do
      Logger.info("client #{client_id} publish to #{pub.topic} denied by ACL")
      :ok
    else
      case pub.qos do
        1 ->
          puback = %Mqttc.Packet.Puback{identifier: pub.identifier}
          send_packet(sock, puback)

        2 ->
          pubrec = %Mqttc.Packet.Pubrec{identifier: pub.identifier}
          send_packet(sock, pubrec)

          receive do
            {:tcp, ^sock, data} ->
              case Mqttc.Packet.decode(data) do
                {:ok, %Mqttc.Packet.Pubrel{identifier: id}, _rest} when id == pub.identifier ->
                  pubcomp = %Mqttc.Packet.Pubcomp{identifier: pub.identifier}
                  send_packet(sock, pubcomp)

                _ ->
                  :ok
              end
          end

        0 ->
          :ok
      end

      subs = GenServer.call(server_pid, {:match, pub.topic})
      Logger.info("publish match: topic=#{pub.topic} subs=#{inspect(subs)}")

      for sub <- subs do
        {_node, sub_client_id} = sub.dest

        if sub_client_id != client_id do
          qos = min(pub.qos, sub.qos)

          msg = %Mqttc.Packet.Publish{
            topic: pub.topic,
            payload: pub.payload,
            qos: qos,
            retain: pub.retain,
            identifier: if(qos > 0, do: next_message_id(), else: nil)
          }

          Logger.info("forwarding to client_id=#{sub_client_id} on node=#{_node}")
          GenServer.call(server_pid, {:forward, sub_client_id, msg})
        end
      end
    end
  end

  defp handle_packet(sock, server_pid, client_id, username, %Mqttc.Packet.Subscribe{} = sub) do
    return_codes =
      Enum.map(sub.payload, fn {topic, _retain_handling, _retain_as_published, _no_local, qos} ->
        allowed = GenServer.call(server_pid, {:can_subscribe, username, topic})

        cond do
          not allowed ->
            0x80

          qos > 2 ->
            0x80

          true ->
            GenServer.call(server_pid, {:subscribe, topic, client_id, qos})
            Logger.info("client #{client_id} subscribed to #{topic} (qos #{qos})")
            qos
        end
      end)

    suback = %Mqttc.Packet.Suback{
      identifier: sub.identifier,
      reason_codes: return_codes
    }

    send_packet(sock, suback)
  end

  defp handle_packet(sock, server_pid, client_id, _username, %Mqttc.Packet.Unsubscribe{} = unsub) do
    for topic <- unsub.payload do
      GenServer.call(server_pid, {:unsubscribe, topic, client_id})
      Logger.info("client #{client_id} unsubscribed from #{topic}")
    end

    unsuback = %Mqttc.Packet.Unsuback{identifier: unsub.identifier}
    send_packet(sock, unsuback)
  end

  defp handle_packet(_sock, _server_pid, _client_id, _username, %Mqttc.Packet.Puback{}) do
    :ok
  end

  defp handle_packet(_sock, _server_pid, _client_id, _username, %Mqttc.Packet.Pubrec{}) do
    :ok
  end

  defp handle_packet(_sock, _server_pid, _client_id, _username, %Mqttc.Packet.Pubrel{}) do
    :ok
  end

  defp handle_packet(_sock, _server_pid, _client_id, _username, %Mqttc.Packet.Pubcomp{}) do
    :ok
  end

  defp handle_packet(sock, _server_pid, _client_id, _username, %Mqttc.Packet.Pingreq{}) do
    send_packet(sock, %Mqttc.Packet.Pingresp{})
  end

  defp handle_packet(sock, server_pid, client_id, _username, %Mqttc.Packet.Disconnect{}) do
    Logger.info("client #{client_id} disconnected")
    GenServer.call(server_pid, {:unregister_client, client_id})
    :gen_tcp.close(sock)
    :ok
  end

  defp handle_packet(_sock, _server_pid, client_id, _username, packet) do
    Logger.warning("client #{client_id}: unexpected packet #{inspect(packet)}")
  end

  defp send_packet(sock, packet) do
    :gen_tcp.send(sock, Mqttc.Packet.encode(packet))
  end

  defp next_message_id do
    :rand.uniform(65_535)
  end
end
