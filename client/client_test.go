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
var maxRetry = 10
var masterSrv, datanodeSrv *grpc.Server

func constructData(col uint32, row uint32) []byte {
	return []byte("{\n" +
		"\"c\": " + fmt.Sprint(col) + ",\n" +
		"\"r\": " + fmt.Sprint(row) + ",\n" +
		"\"v\": {\n" +
		"\"ct\": {\"fa\": \"General\",\"t\": \"g\"},\n" +
		"\"m\": \"ww\",\n" +
		"\"v\": \"ww\"\n" +
		"}\n" +
		"}")
}

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

func TestOpen(t *testing.T) {
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
		Convey("Open exist test file", func() {
			Create("test file")
			file, err := Open("test file")
			So(err, ShouldEqual, nil)
			So(file.Fd, ShouldEqual, 1) // create fd 0
		})

		Convey("Open non-exist test file", func() {
			file, err := Open("non-exist file")
			So(err, ShouldNotBeNil)
			So(file, ShouldEqual, nil)
		})
	})
}

func TestDelete(t *testing.T) {
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
		Convey("Delete test file", func() {
			// TODO
		})

		Convey("Delete non-exist test file", func() {
			// TODO
		})
	})
}

func TestReadAndWrite(t *testing.T) {
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

		// var file File
		Convey("Read empty file after create", func() {
			file, err := Create("test file")

			read := make([]byte, 1024)
			_, err = file.Read(read) // must call this before write

			header := []byte("{\"celldata\": []}")
			So(read[:len(header)], ShouldResemble, header)
			So(err, ShouldBeNil)

			// read := make([]byte, 1024)
			b := []byte("this is test")

			size, err := file.WriteAt(b, 0, 0, " ")
			So(size, ShouldEqual, len(b))
			So(err, ShouldBeNil)

			size, err = file.ReadAt(read, 0, 0)
			So(read[:len(b)], ShouldResemble, b)
			So(size, ShouldEqual, 2048)
			So(err, ShouldBeNil)
		})
	})
}

func TestComplicatedReadAndWrite(t *testing.T) {
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

		// var file File
		Convey("Read empty file after create", func() {
			file, err := Create("test file")
			file.Read([]byte{}) // must call this before write

			// read := make([]byte, 1024)
			for col := 0; col < 10; col++ {
				for row := 0; row < 10; row++ {
					b := constructData(uint32(col), uint32(row))
					file.WriteAt(b, uint32(col), uint32(row), " ")
				}
			}

			read := make([]byte, 1024*100)
			_, err = file.Read(read) // must call this before write

			So(err, ShouldBeNil)
		})
	})
}

func TestConcurrentWrite(t *testing.T) {
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

		// var file File
		Convey("Read empty file after create", func() {
			file, err := Create("test file")
			file.Read([]byte{}) // must call this before write

			// read := make([]byte, 1024)
			for col := 0; col < 10; col++ {
				for row := 0; row < 10; row++ {
					row := row
					col := col
					go func() {
						b := constructData(uint32(col), uint32(row))
						file.WriteAt(b, uint32(col), uint32(row), " ")
					}()
				}
			}

			read := make([]byte, 1024*100)
			_, err = file.Read(read) // must call this before write

			So(err, ShouldBeNil)
		})
	})
}
