package ahimsadb

import (
	"github.com/NSkelsey/ahimsad/scanner"
	"github.com/NSkelsey/protocol/ahimsa"
	"github.com/conformal/btcwire"
)

// Writes a bulletin into the sqlite db, runs an insert or update depending on whether
// block hash exists.
func (db *PublicRecord) StoreBulletin(bltn *ahimsa.Bulletin) error {

	var err error
	if bltn.Block == nil {
		cmd := `
		INSERT OR REPLACE INTO bulletins 
		(txid, author, board, message, timestamp) VALUES($1, $2, $3, $4, $5)
		`
		_, err = db.conn.Exec(cmd,
			bltn.Txid.String(),
			bltn.Author,
			bltn.Board,
			bltn.Message,
			bltn.Timestamp,
		)
	} else {
		blockstr := bltn.Block.String()
		cmd := `
		INSERT OR REPLACE INTO bulletins 
		(txid, block, author, board, message, timestamp) VALUES($1, $2, $3, $4, $5, $6)
		`
		_, err = db.conn.Exec(cmd,
			bltn.Txid.String(),
			blockstr,
			bltn.Author,
			bltn.Board,
			bltn.Message,
			bltn.Timestamp,
		)
	}
	if err != nil {
		return err
	}

	return nil
}

// Returns a getblocks msg that requests the best chain.
func (db *PublicRecord) MakeBlockMsg() (btcwire.Message, error) {

	chaintip, err := db.GetChainTip()
	if err != nil {
		return btcwire.NewMsgGetBlocks(nil), err
	}

	var curblk *BlockRecord = chaintip
	msg := btcwire.NewMsgGetBlocks(curblk.Hash)

	heights := []int{}
	step, start := 1, 0
	for i := int(chaintip.Height); i > 0; i -= step {
		// Push last 10 indices first
		if start >= 10 {
			step *= 2
		}
		heights = append(heights, i)
		start++
	}
	heights = append(heights, 0)

	for _, h := range heights {

		var err error
		curblk, err := db.getBlkAtHeight(h)
		if err != nil {
			return nil, err
		}
		msg.AddBlockLocatorHash(curblk.Hash)
	}

	return msg, nil
}

// Generates a batch insert from the list of scanner.Blocks provided. Intended to
// speed up the initial dump of headers into the db.
func (db *PublicRecord) BatchInsertBH(blks []*scanner.Block, height int) error {

	stmt, err := db.conn.Prepare(`
	INSERT INTO blocks (hash, prevhash, height, timestamp) VALUES($1, $2, $3, $4)
	`)
	defer stmt.Close()
	if err != nil {
		return err
	}

	tx, err := db.conn.Begin()
	defer tx.Commit()
	if err != nil {
		return err
	}

	for _, blk := range blks {
		bh := scanner.ConvBHtoBTCBH(*blk.Head)

		hash, _ := bh.BlockSha()
		prevh := bh.PrevBlock

		_, err = tx.Stmt(stmt).Exec(
			hash.String(),
			prevh.String(),
			height-blk.Depth,
			bh.Timestamp.Unix(),
		)

		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Randomly returns a BlockBecord at height
func (db *PublicRecord) getBlkAtHeight(height int) (*BlockRecord, error) {
	cmd := `
	SELECT hash, prevhash, height, timestamp FROM blocks WHERE height = $1
	ORDER BY RANDOM()
	LIMIT 1
	`

	row := db.conn.QueryRow(cmd, height)

	blkrec, err := scanBlkRec(row)
	if err != nil {
		return nil, err
	}
	return blkrec, nil
}
