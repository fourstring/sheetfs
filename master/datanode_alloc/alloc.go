package datanode_alloc

import (
	"sheetfs/master/errors"
	"sync"
)

type dataNode struct {
	address string
}

type DataNodeAllocator struct {
	mu           sync.RWMutex
	dataNodes    []*dataNode
	dataNodesSet map[string]struct{}
	curPos       uint
}

var allocator *DataNodeAllocator

func init() {
	allocator = &DataNodeAllocator{
		dataNodes:    []*dataNode{},
		dataNodesSet: map[string]struct{}{},
		curPos:       0,
	}
}

func (c *DataNodeAllocator) addDataNode(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.dataNodesSet[address]
	if ok {
		return
	}
	c.dataNodes = append(c.dataNodes, &dataNode{address: address})
	c.curPos = uint(len(c.dataNodes)) - 1
	c.dataNodesSet[address] = struct{}{}
}

func (c *DataNodeAllocator) allocateNode() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.dataNodes) == 0 {
		return ""
	}
	node := c.dataNodes[c.curPos].address
	c.curPos = (c.curPos + 1) % uint(len(c.dataNodes))
	return node
}

func AllocateNode() (string, error) {
	node := allocator.allocateNode()
	if node == "" {
		return "", &errors.NoDataNodeError{}
	}
	return node, nil
}

func AddDataNode(address string) {
	allocator.addDataNode(address)
}
