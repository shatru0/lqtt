package main

import (
	"testing"
)

func TestTopicMatch(t *testing.T) {
	tm := NewTopicManager()
	tm.Subscribe("sensor/+/temp", "client1", 1)

	tests := []struct {
		topic   string
		matched int
	}{
		{"sensor/room1/temp", 1},
		{"sensor/room2/temp", 1},
		{"sensor/room1/humidity", 0},
		{"other/topic", 0},
	}
	for _, tc := range tests {
		subs := tm.Match(tc.topic)
		if len(subs) != tc.matched {
			t.Errorf("Match(%q) got %d subs, want %d", tc.topic, len(subs), tc.matched)
		}
	}

	tm.Subscribe("sensor/#", "client2", 2)
	subs := tm.Match("sensor/room1/temp")
	if len(subs) != 2 {
		t.Errorf("after wildcard, Match got %d subs, want 2", len(subs))
	}

	tm.UnsubscribeAll("client1")
	subs = tm.Match("sensor/room1/temp")
	if len(subs) != 1 {
		t.Errorf("after unsub client1, Match got %d subs, want 1", len(subs))
	}
}

func TestTopicMatchMultiLevel(t *testing.T) {
	tm := NewTopicManager()

	tm.Subscribe("#", "client1", 0)
	if len(tm.Match("anything/at/all")) != 1 {
		t.Error("'#' should match everything")
	}

	tm = NewTopicManager()
	tm.Subscribe("a/#", "client1", 0)
	if len(tm.Match("a/b/c")) != 1 {
		t.Error("'a/#' should match 'a/b/c'")
	}
	if len(tm.Match("b")) != 0 {
		t.Error("'a/#' should not match 'b'")
	}
}

func TestTopicMatchSingleLevel(t *testing.T) {
	tm := NewTopicManager()
	tm.Subscribe("+/+", "client1", 0)

	if len(tm.Match("a/b")) != 1 {
		t.Error("'+ / +' should match 'a/b'")
	}
	if len(tm.Match("a/b/c")) != 0 {
		t.Error("'+' should not match multi-level")
	}
}

func TestTriePlusWildcard(t *testing.T) {
	t.Run("single_plus_matches_one_level", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("+", "c1", 0)
		if len(tm.Match("a")) != 1 {
			t.Error("'+' should match 'a'")
		}
		if len(tm.Match("a/b")) != 0 {
			t.Error("'+' should NOT match 'a/b'")
		}
	})

	t.Run("plus_at_each_position", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("+/mid/end", "c1", 0)
		tm.Subscribe("start/+/end", "c2", 0)
		tm.Subscribe("start/mid/+", "c3", 0)

		if len(tm.Match("x/mid/end")) != 1 {
			t.Errorf("only '+/mid/end' should match 'x/mid/end', got %d subs", len(tm.Match("x/mid/end")))
		}
		if len(tm.Match("start/y/end")) != 1 {
			t.Errorf("only 'start/+/end' should match 'start/y/end', got %d subs", len(tm.Match("start/y/end")))
		}
		if len(tm.Match("start/mid/z")) != 1 {
			t.Errorf("only 'start/mid/+' should match 'start/mid/z', got %d subs", len(tm.Match("start/mid/z")))
		}
		if len(tm.Match("start/mid/end")) != 3 {
			t.Errorf("all three should match 'start/mid/end', got %d subs", len(tm.Match("start/mid/end")))
		}
	})

	t.Run("multiple_plus_wildcards", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("+/+/+", "c1", 0)
		tm.Subscribe("a/+/c", "c2", 0)

		if len(tm.Match("x/y/z")) != 1 {
			t.Error("'+/+/+' should match 'x/y/z'")
		}
		if len(tm.Match("a/b/c")) != 2 {
			t.Error("'+/+/+' and 'a/+/c' should both match 'a/b/c'")
		}
		if len(tm.Match("a/b/c/d")) != 0 {
			t.Error("three-level filters should NOT match 'a/b/c/d'")
		}
	})

	t.Run("plus_does_not_match_extra_levels", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("a/+", "c1", 0)
		if len(tm.Match("a/b/c")) != 0 {
			t.Error("'a/+' should NOT match 'a/b/c'")
		}
	})
}

func TestTrieHashWildcard(t *testing.T) {
	t.Run("bare_hash_matches_everything", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("#", "c1", 0)

		cases := []string{"a", "a/b", "a/b/c", "x/y/z", "single", ""}
		for _, tc := range cases {
			if len(tm.Match(tc)) != 1 {
				t.Errorf("'#' should match %q", tc)
			}
		}
	})

	t.Run("hash_at_depth", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("a/#", "c1", 0)

		if len(tm.Match("a")) != 1 {
			t.Error("'a/#' should match 'a'")
		}
		if len(tm.Match("a/b")) != 1 {
			t.Error("'a/#' should match 'a/b'")
		}
		if len(tm.Match("a/b/c/d/e")) != 1 {
			t.Error("'a/#' should match 'a/b/c/d/e'")
		}
		if len(tm.Match("b")) != 0 {
			t.Error("'a/#' should NOT match 'b'")
		}
		if len(tm.Match("b/a")) != 0 {
			t.Error("'a/#' should NOT match 'b/a'")
		}
	})

	t.Run("multiple_hash_subscribers", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("#", "c1", 0)
		tm.Subscribe("#", "c2", 1)
		tm.Subscribe("a/#", "c3", 0)

		if len(tm.Match("anything")) != 2 {
			t.Errorf("two '#' subs should match 'anything', got %d", len(tm.Match("anything")))
		}
		if len(tm.Match("a/b")) != 3 {
			t.Errorf("two '#' subs + 'a/#' should match 'a/b', got %d", len(tm.Match("a/b")))
		}
		if len(tm.Match("x/y")) != 2 {
			t.Errorf("two '#' subs should match 'x/y', got %d", len(tm.Match("x/y")))
		}
	})
}

func TestTrieCombinedWildcards(t *testing.T) {
	t.Run("plus_then_hash", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("+/#", "c1", 0)

		if len(tm.Match("a")) != 1 {
			t.Error("'+/#' should match 'a'")
		}
		if len(tm.Match("a/b")) != 1 {
			t.Error("'+/#' should match 'a/b'")
		}
		if len(tm.Match("a/b/c/d")) != 1 {
			t.Error("'+/#' should match 'a/b/c/d'")
		}
	})

	t.Run("mixed_literal_plus_hash", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("sensor/+/temp", "c1", 0)
		tm.Subscribe("sensor/#", "c2", 0)
		tm.Subscribe("+/status/#", "c3", 0)

		subs := tm.Match("sensor/room1/temp")
		if len(subs) != 2 {
			t.Errorf("'sensor/+/temp' and 'sensor/#' should match, got %d", len(subs))
		}

		subs = tm.Match("device/status/running")
		if len(subs) != 1 {
			t.Errorf("'+/status/#' should match 'device/status/running', got %d", len(subs))
		}

		subs = tm.Match("device/status")
		if len(subs) != 1 {
			t.Errorf("'+/status/#' should match 'device/status', got %d", len(subs))
		}

		subs = tm.Match("other/status/a/b/c")
		if len(subs) != 1 {
			t.Errorf("'+/status/#' should match 'other/status/a/b/c', got %d", len(subs))
		}
	})
}

func TestTrieUnsubscribe(t *testing.T) {
	t.Run("unsubscribe_one_client", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("a/b/c", "c1", 0)
		tm.Subscribe("a/b/c", "c2", 1)
		tm.Subscribe("a/#", "c1", 0)

		subs := tm.Match("a/b/c")
		if len(subs) != 3 {
			t.Fatalf("want 3 subs, got %d", len(subs))
		}

		tm.Unsubscribe("a/b/c", "c1")
		subs = tm.Match("a/b/c")
		if len(subs) != 2 {
			t.Errorf("after unsub c1 from 'a/b/c', want 2 subs, got %d", len(subs))
		}
	})

	t.Run("unsubscribe_hash", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("a/#", "c1", 0)
		tm.Subscribe("a/#", "c2", 1)

		tm.Unsubscribe("a/#", "c1")
		subs := tm.Match("a/b")
		if len(subs) != 1 {
			t.Fatalf("want 1 sub, got %d", len(subs))
		}
		if subs[0].ClientID != "c2" {
			t.Error("remaining sub should be c2")
		}
	})

	t.Run("unsubscribe_plus", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("+/+", "c1", 0)
		tm.Subscribe("+/+", "c2", 1)

		tm.Unsubscribe("+/+", "c1")
		subs := tm.Match("a/b")
		if len(subs) != 1 {
			t.Fatalf("want 1 sub, got %d", len(subs))
		}
		if subs[0].ClientID != "c2" {
			t.Error("remaining sub should be c2")
		}
	})

	t.Run("unsubscribe_all", func(t *testing.T) {
		tm := NewTopicManager()
		tm.Subscribe("a/b", "c1", 0)
		tm.Subscribe("a/#", "c1", 0)
		tm.Subscribe("+/x", "c1", 0)
		tm.Subscribe("a/b", "c2", 0)

		tm.UnsubscribeAll("c1")
		subs := tm.Match("a/b")
		if len(subs) != 1 {
			t.Fatalf("want 1 sub (c2), got %d", len(subs))
		}
		if subs[0].ClientID != "c2" {
			t.Error("remaining sub should be c2")
		}
		if len(tm.Match("a/b/c")) != 0 {
			t.Error("all c1 hash subs removed")
		}
		if len(tm.Match("y/x")) != 0 {
			t.Error("all c1 plus subs removed")
		}
	})
}
