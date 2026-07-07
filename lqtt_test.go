package main

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestAuthDisabled(t *testing.T) {
	srv := NewServer(":18832")
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://127.0.0.1:18832")
	opts.SetClientID("auth-disabled")
	opts.SetCleanSession(true)

	client := mqtt.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(3 * time.Second) {
		t.Fatal("connect timeout")
	}
	if tok.Error() != nil {
		t.Fatalf("connect should succeed with auth disabled: %v", tok.Error())
	}
	client.Disconnect(250)
}

func TestAuthEnabled(t *testing.T) {
	auth := NewMapAuthenticator()
	auth.AddUser("user1", "pass1")

	srv := NewServer(":18833")
	srv.SetAuthenticator(auth)
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	t.Run("correct_credentials", func(t *testing.T) {
		opts := mqtt.NewClientOptions()
		opts.AddBroker("tcp://127.0.0.1:18833")
		opts.SetClientID("auth-good")
		opts.SetCleanSession(true)
		opts.SetUsername("user1")
		opts.SetPassword("pass1")

		client := mqtt.NewClient(opts)
		tok := client.Connect()
		if !tok.WaitTimeout(3 * time.Second) {
			t.Fatal("connect timeout")
		}
		if tok.Error() != nil {
			t.Fatalf("connect should succeed with valid credentials: %v", tok.Error())
		}
		client.Disconnect(250)
	})

	t.Run("wrong_password", func(t *testing.T) {
		opts := mqtt.NewClientOptions()
		opts.AddBroker("tcp://127.0.0.1:18833")
		opts.SetClientID("auth-wrong")
		opts.SetCleanSession(true)
		opts.SetUsername("user1")
		opts.SetPassword("wrongpass")

		client := mqtt.NewClient(opts)
		tok := client.Connect()
		if !tok.WaitTimeout(3 * time.Second) {
			t.Fatal("connect timeout")
		}
		if tok.Error() == nil {
			t.Fatal("connect should fail with wrong password")
		}
	})

	t.Run("unknown_user", func(t *testing.T) {
		opts := mqtt.NewClientOptions()
		opts.AddBroker("tcp://127.0.0.1:18833")
		opts.SetClientID("auth-unknown")
		opts.SetCleanSession(true)
		opts.SetUsername("nobody")
		opts.SetPassword("pass")

		client := mqtt.NewClient(opts)
		tok := client.Connect()
		if !tok.WaitTimeout(3 * time.Second) {
			t.Fatal("connect timeout")
		}
		if tok.Error() == nil {
			t.Fatal("connect should fail for unknown user")
		}
	})

	t.Run("no_credentials", func(t *testing.T) {
		opts := mqtt.NewClientOptions()
		opts.AddBroker("tcp://127.0.0.1:18833")
		opts.SetClientID("auth-nocred")
		opts.SetCleanSession(true)

		client := mqtt.NewClient(opts)
		tok := client.Connect()
		if !tok.WaitTimeout(3 * time.Second) {
			t.Fatal("connect timeout")
		}
		if tok.Error() == nil {
			t.Fatal("connect should fail without credentials when auth is enabled")
		}
	})
}

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
