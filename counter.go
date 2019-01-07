// Copyright 2017 XUEQIU.COM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/garyburd/redigo/redis"
	"sort"
	"strconv"
	"strings"
)

// NewCounter return a pointer of Counter
func NewCounter() *Counter {
	h := &entryHeap{}
	heap.Init(h)
	p := &prefixHeap{}
	heap.Init(p)
	return &Counter{
		largestEntries:     h,
		largestKeyPrefixes: p,
		lengthLevel0:       100,
		lengthLevel1:       1000,
		lengthLevel2:       10000,
		lengthLevel3:       100000,
		lengthLevel4:       1000000,
		lengthLevelBytes:   map[typeKey]uint64{},
		lengthLevelNum:     map[typeKey]uint64{},
		keyPrefixBytes:     map[typeKey]uint64{},
		keyPrefixNum:       map[typeKey]uint64{},
		typeBytes:          map[string]uint64{},
		typeNum:            map[string]uint64{},
		separators:         ":;,_-& ",
	}
}

// Counter for redis memory useage
type Counter struct {
	largestEntries     *entryHeap
	largestKeyPrefixes *prefixHeap
	lengthLevel0       uint64
	lengthLevel1       uint64
	lengthLevel2       uint64
	lengthLevel3       uint64
	lengthLevel4       uint64
	lengthLevelBytes   map[typeKey]uint64
	lengthLevelNum     map[typeKey]uint64
	keyPrefixBytes     map[typeKey]uint64
	keyPrefixNum       map[typeKey]uint64
	separators         string
	typeBytes          map[string]uint64
	typeNum            map[string]uint64
}

// Count by various dimensions
func (c *Counter) Count(in <-chan *Entry) {
	for e := range in {
		c.count(e)
	}
	// get largest prefixes
	c.calcuLargestKeyPrefix(1000)
}

// GetLargestEntries from heap, num max is 500
func (c *Counter) GetLargestEntries(num int) []*Entry {
	res := []*Entry{}

	// get a copy of c.largestEntries
	for i := 0; i < c.largestEntries.Len(); i++ {
		entries := *c.largestEntries
		res = append(res, entries[i])
	}
	sort.Sort(sort.Reverse(entryHeap(res)))
	if num < len(res) {
		res = res[:num]
	}
	return res
}

//GetLargestKeyPrefixes from heap
func (c *Counter) GetLargestKeyPrefixes() []*PrefixEntry {
	res := []*PrefixEntry{}

	// get a copy of c.largestKeyPrefixes
	for i := 0; i < c.largestKeyPrefixes.Len(); i++ {
		entries := *c.largestKeyPrefixes
		res = append(res, entries[i])
	}
	sort.Sort(sort.Reverse(prefixHeap(res)))
	return res
}

// GetLenLevelCount from map
func (c *Counter) GetLenLevelCount() []*PrefixEntry {
	res := []*PrefixEntry{}

	// get a copy of lengthLevelBytes and lengthLevelNum
	for key := range c.lengthLevelBytes {
		entry := &PrefixEntry{}
		entry.Type = key.Type
		entry.Key = key.Key
		entry.Bytes = c.lengthLevelBytes[key]
		entry.Num = c.lengthLevelNum[key]
		res = append(res, entry)
	}
	return res
}

func (c *Counter) count(e *Entry) {
	c.countLargestEntries(e, 500)
	c.countByType(e)
	c.countByLength(e)
	c.countByKeyPrefix(e)
}

func (c *Counter) countLargestEntries(e *Entry, num int) {
	heap.Push(c.largestEntries, e)
	l := c.largestEntries.Len()
	if l > num {
		heap.Pop(c.largestEntries)
	}
}

func (c *Counter) countByLength(e *Entry) {
	key := typeKey{
		Type: e.Type,
		Key:  strconv.FormatUint(c.lengthLevel0, 10),
	}

	add := func(c *Counter, key typeKey, e *Entry) {
		c.lengthLevelBytes[key] += e.Bytes
		c.lengthLevelNum[key]++
	}

	// must lengthLevel4 > lengthLevel3 > lengthLevel2 ...
	if e.NumOfElem > c.lengthLevel4 {
		key.Key = strconv.FormatUint(c.lengthLevel4, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel3 {
		key.Key = strconv.FormatUint(c.lengthLevel3, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel2 {
		key.Key = strconv.FormatUint(c.lengthLevel2, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel1 {
		key.Key = strconv.FormatUint(c.lengthLevel1, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel0 {
		key.Key = strconv.FormatUint(c.lengthLevel0, 10)
		add(c, key, e)
	}
}

func (c *Counter) countByType(e *Entry) {
	c.typeNum[e.Type]++
	c.typeBytes[e.Type] += e.Bytes
}

func (c *Counter) countByKeyPrefix(e *Entry) {
	// reset all numbers to 0
	k := strings.Map(func(c rune) rune {
		if c >= 48 && c <= 57 { //48 == "0" 57 == "9"
			return '0'
		}
		return c
	}, e.Key)
	prefixes := getPrefixes(k, c.separators)
	key := typeKey{
		Type: e.Type,
	}
	for _, prefix := range prefixes {
		if len(prefix) == 0 {
			continue
		}
		key.Key = prefix
		c.keyPrefixBytes[key] += e.Bytes
		c.keyPrefixNum[key]++
	}
}

func (c *Counter) calcuLargestKeyPrefix(num int) {
	for key := range c.keyPrefixBytes {
		k := &PrefixEntry{}
		k.Type = key.Type
		k.Key = key.Key
		k.Bytes = c.keyPrefixBytes[key]
		k.Num = c.keyPrefixNum[key]
		delete(c.keyPrefixBytes, key)
		delete(c.keyPrefixNum, key)

		heap.Push(c.largestKeyPrefixes, k)
		l := c.largestKeyPrefixes.Len()
		if l > num {
			heap.Pop(c.largestKeyPrefixes)
		}
	}
}

func (c *Counter) persist(instance string) {
	c.persistLargestEntries(instance)
	c.persistLargestKeyPrefixesByType(instance)
	c.persistTypeBytesAndTypeNum(instance)
	c.persistTotalBytesAndNum(instance)
	c.persistLenLevelCount(instance)
}

func (c *Counter) persistLargestEntries(instance string) {
	rds, err := redis.Dial("tcp", "localhost:6328")
	if err != nil {
		fmt.Println("error when connect with redis")
	}
	defer rds.Close()
	item := map[string]interface{}{}
	for _, e := range c.GetLargestEntries(100) {
		item["key"] = e.Key
		item["type"] = e.Type
		item["bytes"] = e.Bytes
		item["human_size"] = humanize.Bytes(e.Bytes)
		item["num_of_elem"] = e.NumOfElem
		item["len_of_largest_elem"] = e.LenOfLargestElem
		item["field_of_largest_elem"] = e.FieldOfLargestElem
		stringBytes, _ := json.Marshal(item)
		rdsKeyName := fmt.Sprintf("%s:LargestEntries", instance)
		rds.Do("rpush", rdsKeyName, string(stringBytes))
	}

}

func (c *Counter) persistLargestKeyPrefixesByType(instance string) {
	rds, err := redis.Dial("tcp", "localhost:6328")
	if err != nil {
		fmt.Println("error when connect with redis")
	}
	defer rds.Close()
	largestKeyPrefixesByType := map[string][]*PrefixEntry{}
	types := map[string]bool{}
	item := map[string]interface{}{}
	for _, entry := range c.GetLargestKeyPrefixes() {
		// mem use less than 1M, and list is long enough, not necessary to add
		if entry.Bytes < 1000*1000 && len(largestKeyPrefixesByType[entry.Type]) > 50 {
			continue
		}
		item["key"] = entry.Key
		item["bytes"] = entry.Bytes
		item["human_size"] = humanize.Bytes(entry.Bytes)
		item["numOfKey"] = entry.Num
		stringBytes, _ := json.Marshal(item)
		types[entry.Type] = true
		rdsKeyName := fmt.Sprintf("%s:LargestkeyPrefixesByType:%s", instance, entry.Type)
		rds.Do("hset", rdsKeyName, entry.Type, string(stringBytes))
	}
	for t := range types {
		rdsKeyName := fmt.Sprintf("%s:LargestkeyPrefixesAllTypes", instance)
		rds.Do("SADD", rdsKeyName, t)
	}
}

func (c *Counter) persistTypeBytesAndTypeNum(instance string) {
	rds, err := redis.Dial("tcp", "localhost:6328")
	if err != nil {
		fmt.Println("error when connect with redis")
	}
	defer rds.Close()
	var data = make(map[string]map[string]interface{})
	for k := range c.typeBytes {
		if data[k] == nil {
			data[k] = make(map[string]interface{})
		}
		data[k]["num"] = c.typeNum[k]
		data[k]["bytes"] = c.typeBytes[k]
		data[k]["human_size"] = humanize.Bytes(c.typeBytes[k])
	}
	stringBytes, _ := json.Marshal(data)
	rdsKeyName := fmt.Sprintf("%s:TypeAndBytes", instance)
	rds.Do("set", rdsKeyName, string(stringBytes))

}

func (c *Counter) persistTotalBytesAndNum(instance string) {
	rds, err := redis.Dial("tcp", "localhost:6328")
	if err != nil {
		fmt.Println("error when connect with redis")
	}
	defer rds.Close()
	totleNum := uint64(0)
	for _, v := range c.typeNum {
		totleNum += v
	}
	totleBytes := uint64(0)
	for _, v := range c.typeBytes {
		totleBytes += v
	}
	data := map[string]uint64{}
	data["totleNum"] = totleNum
	data["totleBytes"] = totleBytes
	stringBytes, _ := json.Marshal(data)
	rdsKeyName := fmt.Sprintf("%s:TotalBytesAndNum", instance)
	rds.Do("set", rdsKeyName, string(stringBytes))
}

func (c *Counter) persistLenLevelCount(instance string) {
	rds, err := redis.Dial("tcp", "localhost:6328")
	if err != nil {
		fmt.Println("error when connect with redis")
	}
	defer rds.Close()
	data := make(map[string]map[string]map[string]interface{})
	for _, e := range c.GetLenLevelCount() {
		if data[e.typeKey.Type] == nil {
			data[e.typeKey.Type] = make(map[string]map[string]interface{})
			if data[e.typeKey.Type][e.typeKey.Key] == nil {
				data[e.typeKey.Type][e.typeKey.Key] = make(map[string]interface{})

			}
		}
		subMap := make(map[string]interface{})
		subMap["num"] = e.Num
		subMap["bytes"] = e.Bytes
		subMap["human_size"] = humanize.Bytes(e.Num)
		data[e.typeKey.Type][e.typeKey.Key] = subMap
	}
	stringBytes, _ := json.Marshal(data)
	rdsKeyName := fmt.Sprintf("%s:LenLevelCount", instance)
	rds.Do("set", rdsKeyName, string(stringBytes))
}

type entryHeap []*Entry

func (h entryHeap) Len() int {
	return len(h)
}
func (h entryHeap) Less(i, j int) bool {
	return h[i].Bytes < h[j].Bytes
}
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *entryHeap) Push(e interface{}) {
	*h = append(*h, e.(*Entry))
}

type typeKey struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

type prefixHeap []*PrefixEntry

// PrefixEntry record value by prefix
type PrefixEntry struct {
	typeKey `json:"type_key"`
	Bytes   uint64 `json:"bytes"`
	Num     uint64 `json:"num"`
}

func (h prefixHeap) Len() int {
	return len(h)
}
func (h prefixHeap) Less(i, j int) bool {
	if h[i].Bytes < h[j].Bytes {
		return true
	} else if h[i].Bytes == h[j].Bytes {
		if h[i].Num < h[j].Num {
			return true
		} else if h[i].Num == h[j].Num {
			if h[i].Key > h[j].Key {
				return true
			}
		}
	}
	return false

}
func (h prefixHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *prefixHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *prefixHeap) Push(k interface{}) {
	*h = append(*h, k.(*PrefixEntry))
}

func appendIfMissing(slice []int, i int) []int {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

func removeDuplicatesUnordered(elements []string) []string {
	encountered := map[string]bool{}

	// Create a map of all unique elements.
	for v := range elements {
		encountered[elements[v]] = true
	}

	// Place all keys from the map into a slice.
	result := []string{}
	for key := range encountered {
		result = append(result, key)
	}
	return result
}

func getPrefixes(s, sep string) []string {
	res := []string{}
	sepIdx := strings.IndexAny(s, sep)
	if sepIdx < 0 {
		res = append(res, s)
	}
	for sepIdx > -1 {
		r := s[:sepIdx+1]
		if len(res) > 0 {
			r = res[len(res)-1] + s[:sepIdx+1]
		}
		res = append(res, r)
		s = s[sepIdx+1:]
		sepIdx = strings.IndexAny(s, sep)
	}
	// Trim all suffix of separators
	for i := range res {
		for hasAnySuffix(res[i], sep) {
			res[i] = res[i][:len(res[i])-1]
		}
	}
	res = removeDuplicatesUnordered(res)
	return res
}

func hasAnySuffix(s, suffix string) bool {
	for _, c := range suffix {
		if strings.HasSuffix(s, string(c)) {
			return true
		}
	}
	return false
}
