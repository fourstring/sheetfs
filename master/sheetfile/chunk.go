package sheetfile

import (
	"gorm.io/gorm"
	"sheetfs/master/config"
	"sheetfs/master/model"
)

/*
Chunk
Represent a fixed-size block of data stored on some DataNode.
The size of a Chunk is given by config.BytesPerChunk.

A Version is maintained by MasterNode and DataNode separately. Latest
Version of a Chunk is stored in MasterNode, and the actual Version is placed
on DataNode. Version is necessary for serializing write operations to a Chunk.
When a client issues a write operation, MasterNode will increase the Version by
1 and return it to client. Client must send both data to write and the Version
to DataNode which actually stores the Chunk. This operation success iff version
in request is equal to Version in DataNode plus 1, by which we achieve serialization
of write operations.
Version can also be utilized to select correct replication of a Chunk when quorums
were introduced.

As to other metadata datastructures, Chunk should be maintained in memory, with the
aid of journaling to tolerate fault, and flushed to sqlite during checkpointing only.
*/
type Chunk struct {
	model.Model
	DataNode string
	Version  uint64
	Cells    []*Cell
}

/*
IsAvailable
Returns true if c is available to store more Cell, or false on the contrary.
A Chunk contains config.MaxCellsPerChunk slots, each for one Cell.
*/
func (c *Chunk) IsAvailable() bool {
	return len(c.Cells) < config.MaxCellsPerChunk
}

/*
Persistent
Flush Chunk data in memory into sqlite.
This method should be used only for checkpointing, and is supposed to be called
in a transaction for atomicity.
*/
func (c *Chunk) Persistent(tx *gorm.DB) {
	tx.Save(c)
}

/*
Snapshot
Returns a *Chunk points to the copy of c.
See SheetFile for the necessity of Snapshot.
Cells is removed in the snapshot.

@return
	*Chunk points to the copy of c.
*/
func (c *Chunk) Snapshot() *Chunk {
	var nc Chunk
	nc = *c
	nc.Cells = nil
	return &nc
}
