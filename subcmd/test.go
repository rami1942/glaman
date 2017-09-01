package subcmd

import (
	"crypto/aes"
	"database/sql"
	"log"
	"github.com/rami1942/glaman/util"
)

func SubCmdTest(logger *log.Logger, dbName, fileName string, key []byte) (err error) {

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		logger.Printf("Open db failed.(%v)\n", err)
		return
	}
	defer db.Close()

	iv := make([]byte, aes.BlockSize)
	err = db.QueryRow("select iv from initial_vector where id=1").Scan(&iv)
	if err != nil {
		logger.Printf("Query failed(%v)\n", err)
		return
	}

	_, err = util.Decrypt(fileName, "xx.tar", key, iv)
	if err != nil {
		logger.Printf("Decrypt failed(%v)\n", err)
	}

	return
}
