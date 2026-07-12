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

func TestACLDisabled(t *testing.T) {
	srv := NewServer(":18834")
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	received := make(chan string, 1)

	subOpts := mqtt.NewClientOptions()
	subOpts.AddBroker("tcp://127.0.0.1:18834")
	subOpts.SetClientID("acl-sub")
	subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	subOpts.SetCleanSession(true)

	subClient := mqtt.NewClient(subOpts)
	if tok := subClient.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatalf("sub connect: %v", tok.Error())
	}
	defer subClient.Disconnect(250)

	if tok := subClient.Subscribe("test/acl", 0, nil); tok.Wait() && tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pubOpts := mqtt.NewClientOptions()
	pubOpts.AddBroker("tcp://127.0.0.1:18834")
	pubOpts.SetClientID("acl-pub")
	pubOpts.SetCleanSession(true)

	pubClient := mqtt.NewClient(pubOpts)
	if tok := pubClient.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatalf("pub connect: %v", tok.Error())
	}
	defer pubClient.Disconnect(250)

	if tok := pubClient.Publish("test/acl", 0, false, "acl-disabled"); tok.Wait() && tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}

	select {
	case msg := <-received:
		if msg != "acl-disabled" {
			t.Errorf("got %q, want %q", msg, "acl-disabled")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message with ACL disabled")
	}
}

func TestACLEnabled(t *testing.T) {
	acl := NewMapACL()
	acl.AllowPublish("pubuser", "sensor/#")
	acl.AllowSubscribe("subuser", "sensor/+/temp")

	auth := NewMapAuthenticator()
	auth.AddUser("pubuser", "pubpass")
	auth.AddUser("subuser", "subpass")

	srv := NewServer(":18835")
	srv.SetAuthenticator(auth)
	srv.SetACL(acl)
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	t.Run("allowed_publish_and_subscribe", func(t *testing.T) {
		received := make(chan string, 1)

		subOpts := mqtt.NewClientOptions()
		subOpts.AddBroker("tcp://127.0.0.1:18835")
		subOpts.SetClientID("acl-sub-good")
		subOpts.SetUsername("subuser")
		subOpts.SetPassword("subpass")
		subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
			received <- string(msg.Payload())
		})
		subOpts.SetCleanSession(true)

		subClient := mqtt.NewClient(subOpts)
		if tok := subClient.Connect(); tok.Wait() && tok.Error() != nil {
			t.Fatalf("sub connect: %v", tok.Error())
		}
		defer subClient.Disconnect(250)

		if tok := subClient.Subscribe("sensor/room1/temp", 0, nil); tok.Wait() && tok.Error() != nil {
			t.Fatalf("subscribe: %v", tok.Error())
		}

		pubOpts := mqtt.NewClientOptions()
		pubOpts.AddBroker("tcp://127.0.0.1:18835")
		pubOpts.SetClientID("acl-pub-good")
		pubOpts.SetUsername("pubuser")
		pubOpts.SetPassword("pubpass")
		pubOpts.SetCleanSession(true)

		pubClient := mqtt.NewClient(pubOpts)
		if tok := pubClient.Connect(); tok.Wait() && tok.Error() != nil {
			t.Fatalf("pub connect: %v", tok.Error())
		}
		defer pubClient.Disconnect(250)

		if tok := pubClient.Publish("sensor/room1/temp", 0, false, "acl-allowed"); tok.Wait() && tok.Error() != nil {
			t.Fatalf("publish: %v", tok.Error())
		}

		select {
		case msg := <-received:
			if msg != "acl-allowed" {
				t.Errorf("got %q, want %q", msg, "acl-allowed")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for allowed publish")
		}
	})

	t.Run("publish_denied", func(t *testing.T) {
		received := make(chan string, 1)

		subOpts := mqtt.NewClientOptions()
		subOpts.AddBroker("tcp://127.0.0.1:18835")
		subOpts.SetClientID("acl-sub-dpub")
		subOpts.SetUsername("subuser")
		subOpts.SetPassword("subpass")
		subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
			received <- string(msg.Payload())
		})
		subOpts.SetCleanSession(true)

		subClient := mqtt.NewClient(subOpts)
		if tok := subClient.Connect(); tok.Wait() && tok.Error() != nil {
			t.Fatalf("sub connect: %v", tok.Error())
		}
		defer subClient.Disconnect(250)

		if tok := subClient.Subscribe("other/topic", 0, nil); tok.Wait() && tok.Error() != nil {
			t.Fatalf("subscribe: %v", tok.Error())
		}

		pubOpts := mqtt.NewClientOptions()
		pubOpts.AddBroker("tcp://127.0.0.1:18835")
		pubOpts.SetClientID("acl-pub-dpub")
		pubOpts.SetUsername("pubuser")
		pubOpts.SetPassword("pubpass")
		pubOpts.SetCleanSession(true)

		pubClient := mqtt.NewClient(pubOpts)
		if tok := pubClient.Connect(); tok.Wait() && tok.Error() != nil {
			t.Fatalf("pub connect: %v", tok.Error())
		}
		defer pubClient.Disconnect(250)

		if tok := pubClient.Publish("other/topic", 0, false, "should-not-arrive"); tok.Wait() && tok.Error() != nil {
			t.Fatalf("publish: %v", tok.Error())
		}

		select {
		case <-received:
			t.Fatal("message should not be delivered when publish denied by ACL")
		case <-time.After(500 * time.Millisecond):
		}
	})

	t.Run("subscribe_denied", func(t *testing.T) {
		received := make(chan string, 1)

		subOpts := mqtt.NewClientOptions()
		subOpts.AddBroker("tcp://127.0.0.1:18835")
		subOpts.SetClientID("acl-sub-dsub")
		subOpts.SetUsername("subuser")
		subOpts.SetPassword("subpass")
		subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
			received <- string(msg.Payload())
		})
		subOpts.SetCleanSession(true)

		subClient := mqtt.NewClient(subOpts)
		if tok := subClient.Connect(); tok.Wait() && tok.Error() != nil {
			t.Fatalf("sub connect: %v", tok.Error())
		}
		defer subClient.Disconnect(250)

		if tok := subClient.Subscribe("sensor/#", 0, nil); tok.Wait() && tok.Error() != nil {
			t.Fatalf("subscribe should not error even when denied, but got: %v", tok.Error())
		}

		pubOpts := mqtt.NewClientOptions()
		pubOpts.AddBroker("tcp://127.0.0.1:18835")
		pubOpts.SetClientID("acl-pub-dsub")
		pubOpts.SetUsername("pubuser")
		pubOpts.SetPassword("pubpass")
		pubOpts.SetCleanSession(true)

		pubClient := mqtt.NewClient(pubOpts)
		if tok := pubClient.Connect(); tok.Wait() && tok.Error() != nil {
			t.Fatalf("pub connect: %v", tok.Error())
		}
		defer pubClient.Disconnect(250)

		if tok := pubClient.Publish("sensor/room1/temp", 0, false, "denied-sub"); tok.Wait() && tok.Error() != nil {
			t.Fatalf("publish: %v", tok.Error())
		}

		select {
		case <-received:
			t.Fatal("message should not be delivered when subscribe denied by ACL")
		case <-time.After(500 * time.Millisecond):
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
