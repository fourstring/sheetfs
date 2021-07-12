package datanode_alloc

import (
	"sync"
)

type dataNode struct {
	address string
}

type DataNodeAllocator struct {
	mu           sync.Mutex
	dataNodes    []*dataNode
	dataNodesSet map[string]struct{}
	curPos       uint
}

func NewDataNodeAllocator() *DataNodeAllocator {
	return &DataNodeAllocator{
		dataNodes: []*dataNode{{
			address: "node1",
		}},
		dataNodesSet: map[string]struct{}{
			"node1": {},
		},
		curPos: 0,
	}
}

func (c *DataNodeAllocator) AddDataNode(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.dataNodesSet[address]
	if ok {
		return
	}
	c.dataNodes = append(c.dataNodes, &dataNode{address: address})
	// Simple strategy: use newly added node first for better load balancing
	c.curPos = uint(len(c.dataNodes)) - 1
	c.dataNodesSet[address] = struct{}{}
}

func (c *DataNodeAllocator) AllocateNode() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.dataNodes) == 0 {
		return "", &NoDataNodeError{}
	}
	node := c.dataNodes[c.curPos].address
	c.curPos = (c.curPos + 1) % uint(len(c.dataNodes))
	return node, nil
}
