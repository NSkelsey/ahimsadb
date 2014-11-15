package ahimsadb

import "database/sql"

// The overarching struct that contains everything needed for a connection to a
// sqlite db containing the public record
type PublicRecord struct {
	conn *sql.DB

	// Precompiled SQL for ahimsarest
	selectTxid        *sql.Stmt
	selectBlockHead   *sql.Stmt
	selectBlockBltns  *sql.Stmt
	selectAuthor      *sql.Stmt
	selectAuthorBltns *sql.Stmt
	selectBlacklist   *sql.Stmt
	selectBoardSum    *sql.Stmt
	selectBoardBltns  *sql.Stmt
	selectAllBoards   *sql.Stmt
	selectRecentConf  *sql.Stmt
	selectUnconfirmed *sql.Stmt
	selectBlksByDay   *sql.Stmt
	selectDBStatus    *sql.Stmt

	// Precompiled
}

// Loads a sqlite db, checks if its reachabale and prepares all the queries.
func LoadDb(dbpath string) (*PublicRecord, error) {
	conn, err := sql.Open("sqlite3", dbpath)
	if err != nil {
		return nil, err
	}

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	db := &PublicRecord{
		conn: conn,
	}

	err = prepareQueries(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Prepares all of the selects for maximal speediness note that all of the queries
// must be initialized here or nil pointers will get thrown at runtime.
func prepareQueries(db *PublicRecord) error {

	var err error
	db.selectTxid, err = db.conn.Prepare(selectTxidSql)
	if err != nil {
		return err
	}

	db.selectBlockHead, err = db.conn.Prepare(selectBlockHeadSql)
	if err != nil {
		return err
	}

	db.selectBlockBltns, err = db.conn.Prepare(selectBlockBltnsSql)
	if err != nil {
		return err
	}

	db.selectAuthor, err = db.conn.Prepare(selectAuthorSql)
	if err != nil {
		return err
	}

	db.selectAuthorBltns, err = db.conn.Prepare(selectAuthorBltnsSql)
	if err != nil {
		return err
	}

	db.selectBlacklist, err = db.conn.Prepare(selectBlacklistSql)
	if err != nil {
		return err
	}

	db.selectBoardSum, err = db.conn.Prepare(selectBoardSumSql)
	if err != nil {
		return err
	}

	db.selectBoardBltns, err = db.conn.Prepare(selectBoardBltnsSql)
	if err != nil {
		return err
	}

	db.selectBoardSum, err = db.conn.Prepare(selectBoardSumSql)
	if err != nil {
		return err
	}

	db.selectBoardBltns, err = db.conn.Prepare(selectBoardBltnsSql)
	if err != nil {
		return err
	}

	db.selectAllBoards, err = db.conn.Prepare(selectAllBoardsSql)
	if err != nil {
		return err
	}

	db.selectRecentConf, err = db.conn.Prepare(selectRecentConfSql)
	if err != nil {
		return err
	}

	db.selectUnconfirmed, err = db.conn.Prepare(selectUnconfirmedSql)
	if err != nil {
		return err
	}

	db.selectBlksByDay, err = db.conn.Prepare(selectBlksByDaySql)
	if err != nil {
		return err
	}

	db.selectDBStatus, err = db.conn.Prepare(selectDBStatusSql)
	if err != nil {
		return err
	}

	return nil
}
