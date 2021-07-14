package node

import (
	"errors"
	"fmt"
	"github.com/fourstring/sheetfs/datanode/config"
	"github.com/go-zookeeper/zk"
	. "github.com/smartystreets/goconvey/convey"
	"log"
	"testing"
	"time"
)

type testNode struct {
	node *DataNode
	cfg  *DataNodeConfig
}

func newTestNode(id string, port uint, caddr string, name string) (*testNode, error) {
	cfg := &DataNodeConfig{
		NodeID:           id,
		Port:             port,
		ForClientAddr:    caddr,
		ZookeeperServers: config.ElectionServers,
		ZookeeperTimeout: config.ElectionTimeout,
		ElectionZnode:    config.ElectionZnodePrefix + name,
		ElectionPrefix:   config.ElectionPrefix,
		ElectionAck:      config.ElectionAckPrefix + name,
		KafkaServer:      config.KafkaServer,
		KafkaTopic:       config.KafkaTopicPrefix + name,
	}
	mnode, err := NewDataNode(cfg)
	if err != nil {
		return nil, err
	}
	go func() {
		err := mnode.Run()
		if err != nil {
			log.Printf("error happens when %s is running: %s\n", id, err)
		}
	}()
	return &testNode{node: mnode, cfg: cfg}, nil
}

func newTestNodesSet(startPort uint, num int) (map[string]*testNode, error) {
	set := map[string]*testNode{}
	for i := 0; i < num; i++ {
		port := startPort + uint(i)
		id := fmt.Sprintf("mnode%d", i)
		caddr := fmt.Sprintf("127.0.0.1:%d", port)
		n, err := newTestNode(id, port, caddr, "node1")
		if err != nil {
			return nil, err
		}
		set[caddr] = n
	}
	return set, nil
}

func checkPrimaryNode(conn *zk.Conn, nodes map[string]*testNode, ackName string, maxRetry int) (*testNode, []*testNode, error) {
	var primary *testNode
	secondaries := make([]*testNode, 0)
	for i := 0; i < maxRetry; i++ {
		caddr, _, err := conn.Get(ackName)
		if err != nil {
			return nil, nil, err
		}
		if n, ok := nodes[string(caddr)]; ok {
			primary = n
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if primary == nil {
		return nil, nil, errors.New("election failed")
	}
	for _, node := range nodes {
		if node != primary {
			secondaries = append(secondaries, node)
		}
	}
	return primary, secondaries, nil
}

func TestDataNodeReplication(t *testing.T) {
	Convey("Construct test nodes", t, func() {
		nodesSet, err := newTestNodesSet(9375, 3)
		So(err, ShouldBeNil)
		zkConn, _, err := zk.Connect(config.ElectionServers, config.ElectionTimeout)
		So(err, ShouldBeNil)
		_, _, err = checkPrimaryNode(zkConn, nodesSet, config.ElectionAckPrefix+"node1", 20)
		So(err, ShouldBeNil)
	})
}
