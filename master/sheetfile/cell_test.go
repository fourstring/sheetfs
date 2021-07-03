package sheetfile

import (
	. "github.com/smartystreets/goconvey/convey"
	"math"
	"testing"
)

func TestGetCellID(t *testing.T) {
	Convey("Should compute cell ID correctly", t, func() {
		So(GetCellID(0, 0), ShouldEqual, 0)
		So(GetCellID(math.MaxUint32, math.MaxUint32), ShouldEqual, uint64(math.MaxUint64))
		So(GetCellID(0xdeadbeef, 0xdeadbaaf), ShouldEqual, uint64(0xdeadbeefdeadbaaf))
	})
}
