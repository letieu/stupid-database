package main

import (
	"encoding/binary"
	"fmt"
)

const fnvOffSetBasis uint64 = 14695981039346656037
const fnvPrime uint64 = 1099511628211
const loadFactorThreshold = 2

// FNV-1a
func hashValue(v PreHashable, limit int) uint {
	hash := fnvOffSetBasis

	for _, b := range v.HashBytes() {
		hash = hash ^ uint64(b)
		hash = hash * fnvPrime
	}

	return uint(hash % uint64(limit))
}

type IntKey int
type StringKey string

type PreHashable interface {
	HashBytes() []byte
	Equal(PreHashable) bool
}

func (i IntKey) HashBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, (int64(i)))
	return buf[:n]
}

func (i IntKey) Equal(other PreHashable) bool {
	v, ok := other.(IntKey)
	return ok && i == v
}

func (s StringKey) HashBytes() []byte {
	return []byte(s)
}

func (s StringKey) Equal(other PreHashable) bool {
	v, ok := other.(StringKey)
	return ok && s == v
}

type Table struct {
	length  int
	buckets [][]entry
}

type entry struct {
	key   PreHashable
	value any
}

func NewSized(size int) *Table {
	return &Table{
		buckets: make([][]entry, size),
	}
}

func (t *Table) Set(k PreHashable, v any) {
	hash := hashValue(k, len(t.buckets))

	for i, e := range t.buckets[hash] {
		if e.key == k {
			t.buckets[hash][i].value = v
			return
		}
	}

	t.buckets[hash] = append(t.buckets[hash], entry{key: k, value: v})
	t.length += 1

	if t.needExpandBuckets() {
		t.expandBuckets()
	}
}

func (t *Table) Get(k PreHashable) (any, bool) {
	hash := hashValue(k, len(t.buckets))
	for _, v := range t.buckets[hash] {
		if v.key == k {
			return v.value, true
		}
	}

	return nil, false
}

func (t *Table) Delete(k PreHashable) error {
	hash := hashValue(k, len(t.buckets))

	for i, e := range t.buckets[hash] {
		if e.key == k {
			current := t.buckets[hash]
			// remove item
			current[i] = current[len(current)-1]
			current = current[:len(current)-1]

			t.buckets[hash] = current
			t.length -= 1
			return nil
		}
	}

	return fmt.Errorf("key not found")
}

func (t *Table) needExpandBuckets() bool {
	loadFactor := float32(t.length) / float32(len(t.buckets))
	return loadFactor > loadFactorThreshold
}

func (t *Table) expandBuckets() {
	newCapacity := len(t.buckets) * 2
	newBuckets := make([][]entry, newCapacity)

	for _, bucket := range t.buckets {
		for _, e := range bucket {
			newHash := hashValue(e.key, newCapacity)
			newBuckets[newHash] = append(newBuckets[newHash], e)
		}
	}

	t.buckets = newBuckets
}

func main() {
	t := NewSized(10)
	t.Set(StringKey("a"), 1)
	t.Set(StringKey("b"), 2)
	t.Set(StringKey("c"), 3)

	e := t.Delete(StringKey("a"))
	if e != nil {
		panic(e)
	}

	fmt.Println(t.Get(StringKey("b")))
}
