package main

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestIntegrationPubSub(t *testing.T) {
	srv := NewServer(":18831")
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	received := make(chan string, 1)

	subOpts := mqtt.NewClientOptions()
	subOpts.AddBroker("tcp://127.0.0.1:18831")
	subOpts.SetClientID("subscriber")
	subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	subOpts.SetCleanSession(true)

	subClient := mqtt.NewClient(subOpts)
	if tok := subClient.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatalf("sub connect: %v", tok.Error())
	}
	defer subClient.Disconnect(250)

	if tok := subClient.Subscribe("test/topic", 0, nil); tok.Wait() && tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pubOpts := mqtt.NewClientOptions()
	pubOpts.AddBroker("tcp://127.0.0.1:18831")
	pubOpts.SetClientID("publisher")
	pubOpts.SetCleanSession(true)

	pubClient := mqtt.NewClient(pubOpts)
	if tok := pubClient.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatalf("pub connect: %v", tok.Error())
	}
	defer pubClient.Disconnect(250)

	payload := "hello from integration test"
	if tok := pubClient.Publish("test/topic", 0, false, payload); tok.Wait() && tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}

	select {
	case msg := <-received:
		if msg != payload {
			t.Errorf("got %q, want %q", msg, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
