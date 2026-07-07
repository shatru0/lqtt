package main

type Authenticator interface {
	Authenticate(username string, password string) bool
}

type AllowAllAuthenticator struct{}

func (AllowAllAuthenticator) Authenticate(username, password string) bool {
	return true
}

type MapAuthenticator struct {
	users map[string]string
}

func NewMapAuthenticator() *MapAuthenticator {
	return &MapAuthenticator{users: make(map[string]string)}
}

func (a *MapAuthenticator) AddUser(username, password string) {
	a.users[username] = password
}

func (a *MapAuthenticator) Authenticate(username, password string) bool {
	if len(a.users) == 0 {
		return false
	}
	pass, ok := a.users[username]
	return ok && pass == password
}
