package client

import (
	stdctx "context"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	datanode "sheetfs/datanode/server"
	masternode "sheetfs/master/server"
	fs_rpc "sheetfs/protocol"
	"sheetfs/tests"
	"sync"
	"testing"
)

var FileLockMap sync.Map

func TestUtils(t *testing.T) {
	data := connect([]byte("{\n            \"c\": 1,\n            \"r\": 2,\n            \"v\": {\n                \"ct\": {\n                    \"fa\": \"General\",\n                    \"t\": \"g\"\n                },\n                \"m\": \"ww\",\n                \"v\": \"ww\"\n            }\n        },"),
		[]byte("\"index\": \"sheet_1\",\n    \"color\": \"\",\n    \"name\": \"1\",\n    \"order\": 0,\n    \"status\": 1"))
	print(data)
}

var maxRetry = 10
var masterSrv, datanodeSrv *grpc.Server

/*
Start a MasterNode and a DataNode for testing. The db used by MasterNode is sqlite
per-connection independent in-memory one, and all chunks on disk will be removed in
advance. So a fresh MasterNode and DataNode are booted every time to avoid coupling
between tests.

Both of nodes are working on their separate goroutines, listening on different ports.
This implies that they are at the same address space with testing routine, not running
as standalone processes.

To avoid running out of local ports, caller should remember to stop nodes created by
this function unconditionally, which is implemented by stopNodes. For the purpose of
stopping, this function will set global variable masterSrv and datanodeSrv, and stopNodes
calls their Stop() method(if not nil).

@return
	string: address of MasterNode, can be used to connect to master
	string: address of DataNode, can be used to register dataNode
	error:
		* This function tries to seek for a usable port for starting node server for at
		most maxRetry times. If it's unable to find one, errors from net.Listen will be
		returned.
		* errors while connecting or migrating sqlite tables for initializing MasterNode
*/
func startNodes() (string, string, error) {
	masterAddr := ""
	datanodeAddr := ""
	// retry for at most maxRetry times to search for a usable port
	for i := 0; i < maxRetry; i++ {
		// generate
		masterPort := tests.RandInt(30000, 40000)
		masterAddr = fmt.Sprintf("127.0.0.1:%d", masterPort)
		lis, err := net.Listen("tcp", masterAddr)
		if err != nil {
			if i == maxRetry-1 {
				return "", "", err
			}
			continue
		}
		// Listen to port successfully, initialize MasterNode
		db, err := tests.GetTestDB()
		if err != nil {
			return "", "", err
		}
		ms, err := masternode.NewServer(db)
		if err != nil {
			return "", "", err
		}
		masterSrv = grpc.NewServer()
		fs_rpc.RegisterMasterNodeServer(masterSrv, ms)
		// Make the new masterSrv Serving in a independent goroutine
		// to avoid blocking main goroutine where testing logic is executed.
		go func() {
			if err := masterSrv.Serve(lis); err != nil {
				log.Fatal(err)
			}
		}()
		break
	}

	for i := 0; i < maxRetry; i++ {
		datanodePort := tests.RandInt(40000, 50000)
		datanodeAddr = fmt.Sprintf("127.0.0.1:%d", datanodePort)
		lis, err := net.Listen("tcp", datanodeAddr)
		if err != nil {
			if i == maxRetry-1 {
				return "", "", err
			}
			continue
		}
		// delete all the files first for a fresh DataNode
		dir, _ := ioutil.ReadDir("../data")
		for _, d := range dir {
			os.RemoveAll(path.Join([]string{"../data", d.Name()}...))
		}
		ds := datanode.NewServer()
		datanodeSrv = grpc.NewServer()
		fs_rpc.RegisterDataNodeServer(datanodeSrv, ds)
		go func() {
			if err := datanodeSrv.Serve(lis); err != nil {
				log.Fatal(err)
			}
		}()
		break
	}
	return masterAddr, datanodeAddr, nil
}

/*
Register a DataNode to Master by creating a grpc connection to Master and
calling RegisterDataNode.

This function is separated for testing routines where registering of DataNode
manually is desired.

@para
	masterAddr: address of MasterNode
	datanodeAddr: address of DataNode
@return
	fs_rpc.Status: when registered successfully, it will be fs_rpc.Status_OK, or
	fs_rpc.Status_Unavailable will be returned.
*/
func registerDataNode(masterAddr, datanodeAddr string) fs_rpc.Status {
	conn, err := grpc.Dial(masterAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("%s", err)
		return fs_rpc.Status_Unavailable
	}
	defer conn.Close()
	mc := fs_rpc.NewMasterNodeClient(conn)
	rep, err := mc.RegisterDataNode(stdctx.Background(), &fs_rpc.RegisterDataNodeRequest{Addr: datanodeAddr})
	if err != nil {
		fmt.Printf("%s", err)
		return fs_rpc.Status_Unavailable
	}
	return rep.Status
}

/*
Stop the MasterNode and DataNode created by startNodes.

This function should be called(e.g. by defer) unconditionally because sometimes one of the
two servers has been booted up, leaving the other one uninitialized. The booted one must be
stopped too.
*/
func stopNodes() {
	if masterSrv != nil {
		masterSrv.Stop()
	}
	if datanodeSrv != nil {
		datanodeSrv.Stop()
	}

}

func TestCreate(t *testing.T) {
	Convey("Start test servers", t, func() {
		// Booting up testing nodes
		masterAddr, datanodeAddr, err := startNodes()
		// call stopNodes unconditionally (although err!=nil)
		defer stopNodes()
		So(err, ShouldBeNil)
		// register DataNode created to MasterNode
		status := registerDataNode(masterAddr, datanodeAddr)
		So(status, ShouldEqual, fs_rpc.Status_OK)
		// Init client library
		Init(masterAddr)
		Convey("Create test file", func() {
			file, err := Create("test file")
			So(err, ShouldEqual, nil)
			So(file.Fd, ShouldEqual, 0)
		})
	})
}
