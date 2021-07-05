package datanode_alloc

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"sheetfs/master/errors"
	"sync"
	"testing"
)

func TestDataNodeAllocation(t *testing.T) {
	Convey("Test for allocate directly", t, func() {
		_, err := AllocateNode()
		So(err, ShouldBeError, &errors.NoDataNodeError{})
	})

	Convey("Add some nodes", t, func() {
		AddDataNode("1")
		AddDataNode("2")
		AddDataNode("3")
		Convey("Test round rubin allocation", func() {
			node, err := AllocateNode()
			So(err, ShouldBeNil)
			So(node, ShouldEqual, "3")
			node, err = AllocateNode()
			So(node, ShouldEqual, "1")
			node, err = AllocateNode()
			So(node, ShouldEqual, "2")
		})
	})
}

func TestConcurrentAllocation(t *testing.T) {
	Convey("Add some nodes", t, func() {
		nodesCount := 20
		for i := 0; i < nodesCount; i++ {
			AddDataNode(fmt.Sprintf("%d", i))
		}
		Convey("Test concurrent allocation", func(c C) {
			var wg sync.WaitGroup
			nodesMap := map[string]struct{}{}
			var mu sync.Mutex
			doWork := func() {
				defer wg.Done()
				node, err := AllocateNode()
				c.So(err, ShouldBeNil)
				mu.Lock()
				defer mu.Unlock()
				nodesMap[node] = struct{}{}
			}
			for i := 0; i < nodesCount; i++ {
				wg.Add(1)
				go doWork()
			}
			wg.Wait()
			c.So(len(nodesMap), ShouldEqual, nodesCount)
		})
	})
}
