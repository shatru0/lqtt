package main

import (
	"strings"
	"sync"
)

type Subscription struct {
	ClientID string
	QoS      byte
}

type trieNode struct {
	children map[string]*trieNode
	plus     *trieNode
	subs     []Subscription
	hashSubs []Subscription
}

type TopicManager struct {
	mu   sync.RWMutex
	root *trieNode
}

func NewTopicManager() *TopicManager {
	return &TopicManager{root: &trieNode{}}
}

func (tm *TopicManager) Subscribe(topicFilter string, clientID string, qos byte) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	parts := strings.Split(topicFilter, "/")
	tm.root.insert(parts, 0, Subscription{ClientID: clientID, QoS: qos})
}

func (n *trieNode) insert(parts []string, idx int, sub Subscription) {
	if idx >= len(parts) {
		n.subs = append(n.subs, sub)
		return
	}
	part := parts[idx]
	if part == "#" {
		n.hashSubs = append(n.hashSubs, sub)
		return
	}
	if part == "+" {
		if n.plus == nil {
			n.plus = &trieNode{}
		}
		n.plus.insert(parts, idx+1, sub)
		return
	}
	if n.children == nil {
		n.children = make(map[string]*trieNode)
	}
	child, ok := n.children[part]
	if !ok {
		child = &trieNode{}
		n.children[part] = child
	}
	child.insert(parts, idx+1, sub)
}

func (tm *TopicManager) Unsubscribe(topicFilter string, clientID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	parts := strings.Split(topicFilter, "/")
	tm.root.remove(parts, 0, clientID)
}

func (n *trieNode) remove(parts []string, idx int, clientID string) {
	if idx >= len(parts) {
		n.subs = removeClient(n.subs, clientID)
		return
	}
	part := parts[idx]
	if part == "#" {
		n.hashSubs = removeClient(n.hashSubs, clientID)
		return
	}
	if part == "+" {
		if n.plus != nil {
			n.plus.remove(parts, idx+1, clientID)
		}
		return
	}
	if child, ok := n.children[part]; ok {
		child.remove(parts, idx+1, clientID)
	}
}

func (tm *TopicManager) UnsubscribeAll(clientID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.root.removeAll(clientID)
}

func (n *trieNode) removeAll(clientID string) {
	n.subs = removeClient(n.subs, clientID)
	n.hashSubs = removeClient(n.hashSubs, clientID)
	if n.plus != nil {
		n.plus.removeAll(clientID)
	}
	for _, child := range n.children {
		child.removeAll(clientID)
	}
}

func matchTopics(topicParts, filterParts []string) bool {
	tlen, flen := len(topicParts), len(filterParts)
	for i := 0; i < tlen || i < flen; i++ {
		if i >= flen {
			return false
		}
		if filterParts[i] == "#" {
			return true
		}
		if i >= tlen {
			return false
		}
		if filterParts[i] == "+" {
			continue
		}
		if filterParts[i] != topicParts[i] {
			return false
		}
	}
	return tlen == flen
}

func removeClient(subs []Subscription, clientID string) []Subscription {
	n := 0
	for _, s := range subs {
		if s.ClientID != clientID {
			subs[n] = s
			n++
		}
	}
	return subs[:n]
}

func (tm *TopicManager) Match(topic string) []Subscription {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	parts := strings.Split(topic, "/")
	return tm.root.match(parts, 0)
}

func (n *trieNode) match(parts []string, idx int) []Subscription {
	var result []Subscription
	result = append(result, n.hashSubs...)
	if idx >= len(parts) {
		result = append(result, n.subs...)
		return result
	}
	part := parts[idx]
	if child, ok := n.children[part]; ok {
		result = append(result, child.match(parts, idx+1)...)
	}
	if n.plus != nil {
		result = append(result, n.plus.match(parts, idx+1)...)
	}
	return result
}
