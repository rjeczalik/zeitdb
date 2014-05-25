package zeitdb

import (
	"errors"
	"time"

	"github.com/libgit2/git2go"
)

// TODO
// - (BLOCKER) handle dups, right now value for the most recent key is returned -> amend
// - replace buildkv with k -> commit cache
// - allow consecutive puts with the same content
// - zeitdb repo check (message, author, e-mail of the 1st commit message == "zeitdb")
// - repository locking/sharing - atm zeitdb assumes it owns repository
// - add gc - excplicit? implicit once per x puts/deletes?

// DB TODO(rjeczalik): document
type DB interface {
	// Put TODO(rjeczalik): document
	Put(key time.Time, value []byte) error

	// Get TODO(rjeczalik): document
	Get(key time.Time) ([]byte, error)

	// Delete TODO(rjeczalik): document
	Delete(start, end time.Time) error

	// List TODO(rjeczalik): document
	List(limit int) []time.Time

	// Compact TODO(rjeczalik): document
	Compact() error

	// Close TODO(rjeczalik): document
	Close() error
}

const (
	msgcreate = "zeitdb.Create"
	msgput    = "zeitdb.DB.Put"
)

func sign(t time.Time) *git.Signature {
	return &git.Signature{
		Name:  "zeitdb",
		Email: "zeitdb@localhost",
		When:  t,
	}
}

type zeitdb struct {
	r *git.Repository
	k []time.Time
	v []*git.Commit
}

func newZeitdb(r *git.Repository) (*zeitdb, error) {
	db := &zeitdb{
		r: r,
		k: make([]time.Time, 0),
		v: make([]*git.Commit, 0),
	}
	if err := db.buildkv(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Create TODO(rjeczalik): document
func Create(repo string) (DB, error) {
	var err error
	r, err := git.InitRepository(repo, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			r.Free()
		}
	}()
	idx, err := r.Index()
	if err != nil {
		return nil, err
	}
	defer idx.Free()
	oid, err := idx.WriteTree()
	if err != nil {
		return nil, err
	}
	tree, err := r.LookupTree(oid)
	if err != nil {
		return nil, err
	}
	defer tree.Free()
	s := sign(time.Now())
	if _, err = r.CreateCommit("HEAD", s, s, msgcreate, tree); err != nil {
		return nil, err
	}
	cfg, err := r.Config()
	if err != nil {
		return nil, err
	}
	defer cfg.Free()
	if err = cfg.SetInt32("core.compression", 9); err != nil {
		return nil, err
	}
	return newZeitdb(r)
}

// Open TODO(rjeczalik): document
func Open(repo string) (DB, error) {
	r, err := git.OpenRepository(repo)
	if err != nil {
		return nil, err
	}
	return newZeitdb(r)
}

func (db *zeitdb) Put(k time.Time, v []byte) (err error) {
	if v == nil || len(v) == 0 {
		return errors.New("value is empty")
	}
	oid, err := db.r.CreateBlobFromBuffer(v)
	if err != nil {
		return
	}
	blob, err := db.r.LookupBlob(oid)
	if err != nil {
		return
	}
	defer blob.Free()
	tb, err := db.r.TreeBuilder()
	if err != nil {
		return
	}
	if err = tb.Insert("value", oid, git.FilemodeBlob); err != nil {
		return
	}
	if oid, err = tb.Write(); err != nil {
		return
	}
	tree, err := db.r.LookupTree(oid)
	if err != nil {
		return
	}
	defer tree.Free()
	// TODO Use zeitdb.Create commit as parent instead?
	ref, err := db.r.Head()
	if err != nil {
		return
	}
	defer ref.Free()
	parent, err := db.r.LookupCommit(ref.Target())
	if err != nil {
		return
	}
	s := sign(k)
	if oid, err = db.r.CreateCommit("HEAD", s, s, msgput, tree, parent); err == nil {
		// TODO(rjeczalik): remove
		var c *git.Commit
		if c, err = db.r.LookupCommit(oid); err != nil {
			return
		}
		db.k, db.v = append(db.k, k), append(db.v, c)
		sortkv(db.k, db.v)
	}
	return
}

// TODO(rjeczalik): allow k > max(db.k) || k < min(db.k)?
//                  * for justyfing implicit time.Now() for put and argument-less get
//                  * return Get(db.k[0]) if k.After(db.k[0])
//                  * return Get(db.k[len(db.k)-1] if k.Before(db.k[len(db.k)-1])
func (db zeitdb) Get(k time.Time) ([]byte, error) {
	n := Search(db.k, k)
	if n == len(db.k) {
		return nil, errors.New("no value for " + k.Format(time.RFC1123Z))
	}
	tree, err := db.v[n].Tree()
	if err != nil {
		return nil, err
	}
	defer tree.Free()
	entry, err := tree.EntryByPath("value")
	if err != nil {
		return nil, err
	}
	blob, err := db.r.LookupBlob(entry.Id)
	if err != nil {
		return nil, err
	}
	defer blob.Free()
	return blob.Contents(), nil
}

func (db zeitdb) Delete(s, e time.Time) error {
	return errors.New("not implemented")
}

func (db zeitdb) List(n int) []time.Time {
	if len(db.k) == 0 {
		return nil
	}
	if n == 0 {
		n = len(db.k)
	}
	return db.k[:min(n, len(db.k))]
}

func (db zeitdb) Compact() error {
	return errors.New("not implemented")
}

func (db zeitdb) Close() (err error) {
	for _, c := range db.v {
		c.Free()
	}
	db.r.Free()
	return
}

func (db zeitdb) foreach(fn git.RevWalkIterator) (err error) {
	ref, err := db.r.Head()
	if err != nil {
		return
	}
	defer ref.Free()
	walk, err := db.r.Walk()
	if err != nil {
		return
	}
	walk.Sorting(git.SortTime)
	walk.Push(ref.Target())
	err = walk.Iterate(fn)
	walk.Free()
	return
}

// TODO(rjeczalik): remove
func (db *zeitdb) buildkv() error {
	fn := func(c *git.Commit) bool {
		if c.Message() == msgput {
			t := c.Author().When
			sortkv(db.k, db.v)
			if n := SearchExact(db.k, t); n == -1 {
				db.k = append(db.k, t)
				db.v = append(db.v, c)
			} else {
				c.Free()
			}
		}
		return true
	}
	if err := db.foreach(fn); err != nil {
		return err
	}
	sortkv(db.k, db.v)
	return nil
}
