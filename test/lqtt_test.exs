defmodule LqttTest do
  use ExUnit.Case

  test "connect and disconnect" do
    port = 18_831
    {:ok, _srv} = GenServer.start_link(Lqtt.Server, port: port)
    Process.sleep(50)

    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    connect = %Mqttc.Packet.Connect{
      client_id: "test-client",
      clean_start: true,
      keep_alive: 60
    }

    :ok = :gen_tcp.send(sock, Mqttc.Packet.encode(connect))

    {:ok, data} = :gen_tcp.recv(sock, 0, 3000)
    assert {:ok, %Mqttc.Packet.Connack{reason_code: :success}, _rest} = Mqttc.Packet.decode(data)

    disconnect = %Mqttc.Packet.Disconnect{}
    :gen_tcp.send(sock, Mqttc.Packet.encode(disconnect))
    :gen_tcp.close(sock)
  end

  test "publish and subscribe qos 0" do
    port = 18_832
    {:ok, _srv} = GenServer.start_link(Lqtt.Server, port: port)
    Process.sleep(50)

    {:ok, sub_sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    {:ok, pub_sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    sub_connect = %Mqttc.Packet.Connect{client_id: "subscriber", clean_start: true}
    :gen_tcp.send(sub_sock, Mqttc.Packet.encode(sub_connect))

    {:ok, data} = :gen_tcp.recv(sub_sock, 0, 3000)
    assert {:ok, %Mqttc.Packet.Connack{reason_code: :success}, _rest} = Mqttc.Packet.decode(data)

    subscribe = %Mqttc.Packet.Subscribe{
      identifier: 1,
      payload: [{"test/topic", 0, 0, 0, 0}]
    }
    :gen_tcp.send(sub_sock, Mqttc.Packet.encode(subscribe))
    {:ok, data} = :gen_tcp.recv(sub_sock, 0, 3000)
    assert {:ok, %Mqttc.Packet.Suback{identifier: 1, reason_codes: [0]}, _rest} = Mqttc.Packet.decode(data)

    pub_connect = %Mqttc.Packet.Connect{client_id: "publisher", clean_start: true}
    :gen_tcp.send(pub_sock, Mqttc.Packet.encode(pub_connect))
    {:ok, data} = :gen_tcp.recv(pub_sock, 0, 3000)
    assert {:ok, %Mqttc.Packet.Connack{reason_code: :success}, _rest} = Mqttc.Packet.decode(data)

    publish = %Mqttc.Packet.Publish{
      topic: "test/topic",
      payload: "hello from integration test",
      qos: 0
    }
    :gen_tcp.send(pub_sock, Mqttc.Packet.encode(publish))

    {:ok, data} = :gen_tcp.recv(sub_sock, 0, 3000)
    assert {:ok, %Mqttc.Packet.Publish{topic: "test/topic", payload: "hello from integration test"}, _rest} =
             Mqttc.Packet.decode(data)

    :gen_tcp.close(sub_sock)
    :gen_tcp.close(pub_sock)
  end
end
