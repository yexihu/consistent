package consistent

import "testing"

func TestConsistent(t *testing.T) {
	c := NewConsistent()
	c.Add("Memcache_server01")
	c.Add("Memcache_server02")
	c.Add("Memcache_server03")

	t.Log(c.Get("key10"))
	t.Log(c.Get("key2"))
	t.Log(c.Get("key23"))
	t.Log(c.Get("key15"))

	c.Add("Memcache_server04")

	t.Log(c.Get("key10"))
	t.Log(c.Get("key2"))
	t.Log(c.Get("key23"))
	t.Log(c.Get("key15"))
}
