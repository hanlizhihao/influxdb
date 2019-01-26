package consistent

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

const DefaultReplicas = 1000

type HashRing []uint32

func (c HashRing) Len() int {
	return len(c)
}

func (c HashRing) Less(i, j int) bool {
	return c[i] < c[j]
}

func (c HashRing) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type Node struct {
	Id       uint64
	Ip       string
	Port     int
	HostName string
	Weight   int
}

func NewNode(id uint64, ip string, port int, name string, weight int) *Node {
	return &Node{
		Id:       id,
		Ip:       ip,
		Port:     port,
		HostName: name,
		Weight:   weight,
	}
}

type Consistent struct {
	Nodes     map[uint32]Node
	numReps   int
	Resources map[uint64]bool
	ring      HashRing
	sync.RWMutex
	MasterNodes []*Node
}

func NewConsistent() *Consistent {
	return &Consistent{
		Nodes:     make(map[uint32]Node),
		numReps:   DefaultReplicas,
		Resources: make(map[uint64]bool),
		ring:      HashRing{},
	}
}

func (c *Consistent) Add(node *Node) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.Id]; ok {
		return false
	}

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(uint64(i), node)
		c.Nodes[c.hashStr(str)] = *(node)
	}
	c.Resources[node.Id] = true
	c.sortHashRing()
	c.MasterNodes = append(c.MasterNodes, node)
	return true
}

func (c *Consistent) sortHashRing() {
	c.ring = HashRing{}
	for k := range c.Nodes {
		c.ring = append(c.ring, k)
	}
	sort.Sort(c.ring)
}

func (c *Consistent) joinStr(i uint64, node *Node) string {
	return node.Ip + "*" + strconv.Itoa(node.Weight) +
		"-" + strconv.FormatUint(i, 10) +
		"-" + strconv.FormatUint(i, 10)
}

// MurMurHash算法 :https://github.com/spaolacci/murmur3
func (c *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) Get(key string) Node {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashStr(key)
	i := c.search(hash)

	return c.Nodes[c.ring[i]]
}

func (c *Consistent) search(hash uint32) int {

	i := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= hash })
	if i < len(c.ring) {
		if i == len(c.ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.ring) - 1
	}
}

func (c *Consistent) Remove(node *Node) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.Id]; !ok {
		return
	}

	delete(c.Resources, node.Id)

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(uint64(i), node)
		delete(c.Nodes, c.hashStr(str))
	}
	c.sortHashRing()
}

func main() {

	cHashRing := NewConsistent()

	for i := 0; i < 10; i++ {
		si := fmt.Sprintf("%d", i)
		cHashRing.Add(NewNode(uint64(i), "172.18.1."+si, 8080, "host_"+si, 1))
	}

	for k, v := range cHashRing.Nodes {
		fmt.Println("Hash:", k, " IP:", v.Ip)
	}

	ipMap := make(map[string]int, 0)
	for i := 0; i < 1000; i++ {
		si := fmt.Sprintf("key%d", i)
		k := cHashRing.Get(si)
		if _, ok := ipMap[k.Ip]; ok {
			ipMap[k.Ip] += 1
		} else {
			ipMap[k.Ip] = 1
		}
	}

	for k, v := range ipMap {
		fmt.Println("Node IP:", k, " count:", v)
	}

}