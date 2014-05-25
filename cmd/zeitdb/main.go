package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rjeczalik/zeitdb"
)

const usage = `NAME:
	zeitdb - command line interface to zeitdb

USAGE:
	zeitdb init [repo_path]
	zeitdb put file
	zeitdb get [time|n]
	zeitdb delete [time_start]..[time_end]|[n_start]..[n_end]
	zeitdb list [len]`

var now time.Time

func init() {
	now = time.Now()
	if env := os.Getenv("TIME"); env != "" {
		if t, err := time.Parse(time.RFC1123Z, env); err == nil {
			now = t
		}
	}
}

func die(v ...interface{}) {
	for _, v := range v {
		fmt.Fprintln(os.Stderr, v)
	}
	os.Exit(1)
}

func parse(db zeitdb.DB, arg string) (t time.Time) {
	if m, err := strconv.Atoi(arg); err == nil {
		if m < 0 {
			die("invalid argument: "+arg, usage)
		}
		l := db.List(m)
		if l == nil {
			die("db is empty")
		}
		if m > len(l) {
			die("invalid index: " + arg)
		}
		return l[m-1]
	} else if t, err = time.Parse(time.RFC1123Z, arg); err != nil {
		die(err)
	}
	return
}

func main() {
	n := len(os.Args)
	if n == 1 {
		die(usage)
	}
	n, args := n-2, os.Args[2:]
	switch os.Args[1] {
	case "init":
		if n > 1 {
			die("unexpected "+args[1], usage)
		}
		repo := "."
		if n == 1 && len(args[0]) != 0 {
			repo = args[0]
		}
		r, err := zeitdb.Create(repo)
		if err != nil {
			die(err)
		}
		r.Close()
	case "put":
		if n > 1 {
			die("unexpected "+args[1], usage)
		}
		if n == 0 || len(args[0]) == 0 {
			die("expected file name", usage)
		}
		db, err := zeitdb.Open(".")
		if err != nil {
			die(err)
		}
		defer db.Close()
		f, err := os.Open(args[0])
		if err != nil {
			die(err)
		}
		defer f.Close()
		p, err := ioutil.ReadAll(f)
		if err != nil {
			die(err)
		}
		if err = db.Put(now, p); err != nil {
			die(err)
		}
	case "get":
		if n > 1 {
			die("unexpected "+args[1], usage)
		}
		db, err := zeitdb.Open(".")
		if err != nil {
			die(err)
		}
		defer db.Close()
		var t time.Time
		if n == 1 && len(args[0]) != 0 {
			t = parse(db, args[0])
		} else {
			l := db.List(1)
			if l == nil {
				die("db is empty")
			}
			t = l[0]
		}
		p, err := db.Get(t)
		if err != nil {
			die(err)
		}
		if _, err = io.Copy(os.Stdout, bytes.NewBuffer(p)); err != nil {
			die(err)
		}
	case "delete":
		if n > 1 {
			die("unexpected "+args[1], usage)
		}
		if n == 0 || len(args[0]) == 0 {
			die("expected time key", usage)
		}
		m := strings.Index(args[0], "..")
		if m < 0 {
			die("invalid argument", usage)
		}
		db, err := zeitdb.Open(".")
		if err != nil {
			die(err)
		}
		defer db.Close()
		var s, e time.Time
		if m != 0 {
			s = parse(db, args[0][:m])
		}
		if m+2 < len(args[0]) {
			e = parse(db, args[0][m+2:])
		}
		if err = db.Delete(s, e); err != nil {
			die(err)
		}
	case "list":
		if n > 1 {
			die("unexpected "+args[1], usage)
		}
		db, err := zeitdb.Open(".")
		if err != nil {
			die(err)
		}
		defer db.Close()
		var m int
		if n == 1 && len(args[0]) != 0 {
			if m, err = strconv.Atoi(args[0]); err != nil {
				die(err)
			}
			if m < 0 {
				die("invalid argument: "+args[0], usage)
			}
		}
		l := db.List(m)
		if l == nil {
			die("db is empty")
		}
		for i := range l {
			fmt.Println(l[i].Format(time.RFC1123Z))
		}
	default:
		die("unrecognized flag "+os.Args[1], usage)
	}
}
