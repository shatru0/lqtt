package main

import "strings"

type ACL interface {
	CanPublish(username, topic string) bool
	CanSubscribe(username, topic string) bool
}

type AllowAllACL struct{}

func (AllowAllACL) CanPublish(username, topic string) bool  { return true }
func (AllowAllACL) CanSubscribe(username, topic string) bool { return true }

type MapACL struct {
	publishAllowed   map[string][]string
	subscribeAllowed map[string][]string
}

func NewMapACL() *MapACL {
	return &MapACL{
		publishAllowed:   make(map[string][]string),
		subscribeAllowed: make(map[string][]string),
	}
}

func (a *MapACL) AllowPublish(username, topicFilter string) {
	a.publishAllowed[username] = append(a.publishAllowed[username], topicFilter)
}

func (a *MapACL) AllowSubscribe(username, topicFilter string) {
	a.subscribeAllowed[username] = append(a.subscribeAllowed[username], topicFilter)
}

func (a *MapACL) CanPublish(username, topic string) bool {
	for _, filter := range a.publishAllowed[username] {
		if matchTopics(strings.Split(topic, "/"), strings.Split(filter, "/")) {
			return true
		}
	}
	return false
}

func (a *MapACL) CanSubscribe(username, topic string) bool {
	for _, filter := range a.subscribeAllowed[username] {
		if matchTopics(strings.Split(topic, "/"), strings.Split(filter, "/")) {
			return true
		}
	}
	return false
}
