package zeitdb

import (
	"sort"
	"time"

	"github.com/libgit2/git2go"
)

func min(i, j int) int {
	if i > j {
		return j
	}
	return i
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

// TimeSlice TODO(rjeczalik): document
// decreasing order
type TimeSlice []time.Time

func (p TimeSlice) Len() int {
	return len(p)
}

func (p TimeSlice) Less(i, j int) bool {
	return p[i].After(p[j])
}

func (p TimeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Search TODO(rjeczalik): document
func Search(a []time.Time, x time.Time) int {
	return sort.Search(len(a), func(i int) bool { return a[i].Before(x) || a[i].Equal(x) })
}

// SearchExact TODO(rjeczalik): document
func SearchExact(a []time.Time, x time.Time) int {
	n := Search(a, x)
	if n != len(a) && a[n].Equal(x) {
		return n
	}
	return -1
}

// Sort TODO(rjeczalik): document
func Sort(a []time.Time) {
	sort.Sort(TimeSlice(a))
}

type kvslice struct {
	k []time.Time
	v []*git.Commit
}

func (p kvslice) Len() int {
	return len(p.k)
}

func (p kvslice) Less(i, j int) bool {
	return p.k[i].After(p.k[j])
}

func (p kvslice) Swap(i, j int) {
	p.k[i], p.k[j] = p.k[j], p.k[i]
	p.v[i], p.v[j] = p.v[j], p.v[i]
}

func sortkv(k []time.Time, v []*git.Commit) {
	sort.Sort(kvslice{k: k, v: v})
}
