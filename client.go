package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

var (
	msgIDCounter uint32
	clientMu     sync.RWMutex
	clients      = make(map[string]net.Conn)
)

func nextMessageID() uint16 {
	return uint16(atomic.AddUint32(&msgIDCounter, 1) & 0xFFFF)
}

func handleClient(conn net.Conn, tm *TopicManager, auth Authenticator, acl ACL) {
	defer conn.Close()

	cp, err := packets.ReadPacket(conn)
	if err != nil {
		log.Printf("read connect packet: %v", err)
		return
	}

	connectPacket, ok := cp.(*packets.ConnectPacket)
	if !ok {
		log.Printf("expected CONNECT, got %T", cp)
		return
	}

	clientID := connectPacket.ClientIdentifier
	if clientID == "" {
		clientID = fmt.Sprintf("auto-%d", nextMessageID())
	}

	username := connectPacket.Username

	log.Printf("client %s connected (clean=%v, keepalive=%d)", clientID, connectPacket.CleanSession, connectPacket.Keepalive)

	if !auth.Authenticate(username, string(connectPacket.Password)) {
		log.Printf("client %s authentication failed", clientID)
		connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
		connack.ReturnCode = packets.ErrRefusedBadUsernameOrPassword
		connack.Write(conn)
		return
	}

	clientMu.Lock()
	if old, ok := clients[clientID]; ok {
		old.Close()
	}
	clients[clientID] = conn
	clientMu.Unlock()

	if connectPacket.CleanSession {
		tm.UnsubscribeAll(clientID)
	}

	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.SessionPresent = false
	connack.ReturnCode = packets.Accepted
	if err := connack.Write(conn); err != nil {
		log.Printf("write connack: %v", err)
		return
	}

	for {
		cp, err := packets.ReadPacket(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("client %s read: %v", clientID, err)
			}
			break
		}

		switch p := cp.(type) {
		case *packets.PublishPacket:
			handlePublish(conn, clientID, username, p, tm, acl)
		case *packets.SubscribePacket:
			handleSubscribe(conn, clientID, username, p, tm, acl)
		case *packets.UnsubscribePacket:
			handleUnsubscribe(conn, clientID, p, tm)
		case *packets.PubackPacket: // TODO: PubAck needs to be handled to acknowledge the subscriber ack to avoid resend, and also handle resend on no Ack
		case *packets.PubrecPacket:
		case *packets.PubrelPacket:
		case *packets.PubcompPacket:
		case *packets.PingreqPacket:
			resp := packets.NewControlPacket(packets.Pingresp)
			resp.Write(conn)
		case *packets.DisconnectPacket:
			log.Printf("client %s disconnected", clientID)
			return
		default:
			log.Printf("client %s: unexpected packet %T", clientID, cp)
		}
	}
}

func handlePublish(conn net.Conn, clientID string, username string, pub *packets.PublishPacket, tm *TopicManager, acl ACL) {
	if !acl.CanPublish(username, pub.TopicName) {
		log.Printf("client %s publish to %s denied by ACL", clientID, pub.TopicName)
		return
	}

	switch pub.FixedHeader.Qos {
	case 0:

	case 1:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = pub.MessageID
		puback.Write(conn)
	case 2:
		pubrec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
		pubrec.MessageID = pub.MessageID
		pubrec.Write(conn)

		rel, err := packets.ReadPacket(conn)
		if err != nil {
			return
		}
		if _, ok := rel.(*packets.PubrelPacket); !ok {
			return
		}
		pubcomp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		pubcomp.MessageID = pub.MessageID
		pubcomp.Write(conn)
	}

	subs := tm.Match(pub.TopicName)
	for _, sub := range subs {
		if sub.ClientID == clientID {
			continue
		}
		clientMu.RLock()
		subConn, ok := clients[sub.ClientID]
		clientMu.RUnlock()
		if !ok {
			continue
		}

		qos := sub.QoS
		if pub.FixedHeader.Qos < qos {
			qos = pub.FixedHeader.Qos
		}

		msg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		msg.TopicName = pub.TopicName
		msg.Payload = pub.Payload
		msg.FixedHeader.Qos = qos
		msg.FixedHeader.Retain = pub.FixedHeader.Retain
		if qos > 0 {
			msg.MessageID = nextMessageID()
			msg.Dup = false
		}

		if err := msg.Write(subConn); err != nil {
			log.Printf("publish to %s: %v", sub.ClientID, err)
		}
	}
}

func handleSubscribe(conn net.Conn, clientID string, username string, sub *packets.SubscribePacket, tm *TopicManager, acl ACL) {
	returnCodes := make([]byte, len(sub.Topics))
	for i, topic := range sub.Topics {
		if !acl.CanSubscribe(username, topic) {
			log.Printf("client %s subscribe to %s denied by ACL", clientID, topic)
			returnCodes[i] = 0x80
			continue
		}
		qos := sub.Qoss[i]
		if qos > 2 {
			returnCodes[i] = 0x80
		} else {
			returnCodes[i] = qos
		}
		tm.Subscribe(topic, clientID, qos)
		log.Printf("client %s subscribed to %s (qos %d)", clientID, topic, qos)
	}

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = sub.MessageID
	suback.ReturnCodes = returnCodes
	suback.Write(conn)
}

func handleUnsubscribe(conn net.Conn, clientID string, unsub *packets.UnsubscribePacket, tm *TopicManager) {
	for _, topic := range unsub.Topics {
		tm.Unsubscribe(topic, clientID)
		log.Printf("client %s unsubscribed from %s", clientID, topic)
	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = unsub.MessageID
	unsuback.Write(conn)
}

func writeUint16(w io.Writer, v uint16) error {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	_, err := w.Write(b)
	return err
}
