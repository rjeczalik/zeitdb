package zeitdb

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var now = time.Now()

func f(t *testing.T) (string, func()) {
	dir, err := ioutil.TempDir("", "zeitdb")
	if err != nil {
		t.Fatal(err)
	}
	return dir, func() { os.RemoveAll(dir) }
}

func TestZeitdb(t *testing.T) {
	cases := []struct {
		k time.Time
		v []byte
	}{{
		k: now.Add(5 * time.Second),
		v: []byte("value 1"),
	}, {
		k: now.Add(10 * time.Second),
		v: []byte("value 2"),
	}, {
		k: now.Add(15 * time.Second),
		v: []byte("value 3"),
	}, {
		k: now.Add(20 * time.Second),
		v: []byte("value 4"),
	}, {
		k: now.Add(25 * time.Second),
		v: []byte("value 5"),
	}}
	span := []time.Duration{
		0 * time.Second,
		1 * time.Second,
		2 * time.Second,
		3 * time.Second,
		4 * time.Second,
	}
	dir, clr := f(t)
	defer clr()
	db, err := Create(dir)
	if err != nil {
		t.Fatalf("expected err=nil; was %q", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("expected err=nil; was %q", err)
	}
	if db, err = Open(dir); err != nil {
		t.Fatalf("expected err=nil; was %q", err)
	}
	var v []byte
	for i, kv := range cases {
		if err = db.Put(kv.k, kv.v); err != nil {
			t.Errorf("expected err=nil; was %q (i=%d)", err, i)
			continue
		}
		for j, d := range span {
			if v, err = db.Get(kv.k.Add(d)); err != nil {
				t.Errorf("expected err=nil; was %q (i=%d, j=%d)", err, i, j)
				continue
			}
			if !bytes.Equal(v, kv.v) {
				t.Errorf("expected v=%q; was %q (i=%d, j=%d)", kv.v, v, i, j)
			}
		}
	}
	if err = db.Close(); err != nil {
		t.Fatalf("expected err=nil; was %q", err)
	}
}
