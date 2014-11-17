package ahimsadb

import (
	"os"
	"path/filepath"
	"testing"
)

func SetupTestDB() (*PublicRecord, error) {

	var dbpath string

	testEnvPath := os.Getenv("TEST_DB_PATH")
	if testEnvPath != "" {
		dbpath = testEnvPath
	} else {
		dbpath = os.Getenv("GOPATH") + "/src/github.com/NSkelsey/ahimsadb/test.db"
		dbpath = filepath.Clean(dbpath)
	}
	var err error
	db, err := LoadDB(dbpath)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func TestSetupDB(t *testing.T) {
	_, err := SetupTestDB()
	if err != nil {
		t.Fatal(err)
	}

}
