package consistent

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// 使用IEEE 多项式返回数据的CRC-32校验和
// groupcache默认也是使用crc32.ChecksumIEEE
func hashKey(key string) uint32 {
	if len(key) < 64 {
		//声明一个数组长度为64
		var srcatch [64]byte
		//拷贝数据到数组中
		copy(srcatch[:], key)
		//使用IEEE 多项式返回数据的CRC-32校验和
		return crc32.ChecksumIEEE(srcatch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func genVirtualKey(index int, node string) uint32 {
	return hashKey(fmt.Sprintf("%s#%d", node, index))
}

type Consistent struct {
	virtualNodeNum  int
	sortedHashNodes []uint32
	circle          map[uint32]string
	nodes           map[string]bool
	sync.RWMutex
}

func NewConsistent() *Consistent {
	return &Consistent{
		virtualNodeNum: 20,
		circle:         make(map[uint32]string),
		nodes:          make(map[string]bool),
	}
}

//更新排序，方便查找
func (c *Consistent) updateSortedHashNodes() {
	c.sortedHashNodes = nil
	//添加hashes
	for k := range c.circle {
		c.sortedHashNodes = append(c.sortedHashNodes, k)
	}

	//对所有节点hash值进行排序，
	//方便之后进行二分查找
	sort.Slice(c.sortedHashNodes, func(i, j int) bool {
		return c.sortedHashNodes[i] < c.sortedHashNodes[j]
	})
}

// 添加节点
func (c *Consistent) Add(node string) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	if _, ok := c.nodes[node]; ok {
		return errors.New("node already existed")
	}
	c.nodes[node] = true

	for i := 0; i < c.virtualNodeNum; i++ {
		virtualKey := genVirtualKey(i, node)
		c.circle[virtualKey] = node
		//c.sortedHashNodes = append(c.sortedHashNodes, virtualKey)
	}

	c.updateSortedHashNodes()
	return nil
}

// 删除节点
func (c *Consistent) Remove(node string) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	if _, ok := c.nodes[node]; !ok {
		return errors.New("node is not existed")
	}
	delete(c.nodes, node)

	for i := 0; i < c.virtualNodeNum; i++ {
		virtualKey := genVirtualKey(i, node)
		delete(c.circle, virtualKey)
	}

	c.updateSortedHashNodes()
	return nil
}

// 获取最近的服务器节点信息
func (c *Consistent) Get(name string) (string, error) {
	c.RWMutex.RLock()
	defer c.RWMutex.RUnlock()
	if len(c.nodes) == 0 {
		return "", errors.New("no node added")
	}
	key := hashKey(name)
	nearbyIndex := c.searchNearbyIndex(key)
	nearNode := c.circle[c.sortedHashNodes[nearbyIndex]]
	return nearNode, nil
}

// 顺时针查找最近的节点
func (c *Consistent) searchNearbyIndex(key uint32) int {
	//使用"二分查找"算法来搜索指定切片满足条件的最小值
	targetIndex := sort.Search(len(c.sortedHashNodes), func(i int) bool {
		return c.sortedHashNodes[i] >= key
	})
	//如果超出范围则设置targetIndex=0
	if targetIndex >= len(c.sortedHashNodes) {
		targetIndex = 0
	}
	return targetIndex
}
