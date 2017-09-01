package subcmd

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func SubCmdExtract(logger *log.Logger, dbname, idxname string, key []byte) error {

	datDB, err := sql.Open("sqlite3", dbname)
	if err != nil {
		logger.Printf("sql.Open failed.")
		return err
	}
	defer datDB.Close()

	var idxDB *sql.DB
	if idxname != "" {
		idxDB, err = sql.Open("sqlite3", idxname)
		if err != nil {
			logger.Printf("open index db failed.")
			return err
		}
		defer idxDB.Close()
	} else {
		idxDB = datDB
	}

	rows, err := idxDB.Query("select id, name, mtime, size, md5sum, iv from file_entry")
	if err != nil {
		logger.Printf("Query file_entry failed.(%v)\n", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, mtime, size int64
		var name, md5sum string
		iv := make([]byte, aes.BlockSize)
		err := rows.Scan(&id, &name, &mtime, &size, &md5sum, &iv)
		if err != nil {
			logger.Printf("Scan failed.(%v)\n", err)
			return err
		}
		err = extract(logger, datDB, id, name, mtime, size, md5sum, key, iv)
		if err != nil {
			logger.Printf("Extract failed(%v)\n", err)
			// cont
		}
	}

	return nil
}

func extract(logger *log.Logger, db *sql.DB, id int64, name string, mtime, size int64, md5sum string, key, iv []byte) error {

	md5w, err := extractFromDB(logger, db, id, name, key, iv)
	if err != nil {
		logger.Printf("ファイル展開に失敗しました(%v)\n", err)
		return err
	}

	if md5sum != md5w {
		logger.Printf("md5が一致しませんでした\n")
		return errors.New("md5sumが一致しませんでした")
	}

	// ファイル存在チェック
	_, err = os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			// OK
		} else {
			logger.Printf("ファイル情報取得に失敗しました(%v)\n", err)
			return err
		}
	} else {
		logger.Printf("ファイルが存在しています。")
		return errors.New("ファイルが存在しています。")
	}

	err = os.Rename(name+".tmp", name)
	if err != nil {
		logger.Printf("ファイル名変更に失敗しました(%v)\n", err)
		return err
	}

	// atime,mtime設定
	t := time.Unix(0, mtime)
	err = os.Chtimes(name, t, t)
	if err != nil {
		logger.Printf("タイムスタンプ設定に失敗しました(%v)\n", err)
		return err
	}

	return nil
}

func extractFromDB(logger *log.Logger, db *sql.DB, id int64, name string, key, iv []byte) (md5sum string, err error) {

	dir, _ := filepath.Split(name)
	if dir != "" {
		err = os.MkdirAll(dir, 0777)
	}

	of, err := os.Create(name + ".tmp")
	if err != nil {
		logger.Printf("open file failed(%v)\n", err)
		return
	}
	defer of.Close()

	md5Hash := md5.New()

	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}
	stream := cipher.NewCTR(block, iv)

	rows, err := db.Query("select chunk from file_data where file_entry_id=? order by entry_order asc", id)
	if err != nil {
		logger.Print("query file_data failed.(%v)\n", err)
		return
	}
	defer rows.Close()

	data := make([]byte, 8192)
	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			logger.Printf("extract data failed.(%v)\n", err)
			return
		}
		stream.XORKeyStream(data, data[:len(data)])
		md5Hash.Write(data[:len(data)])
		of.Write(data)
	}

	md5sum = fmt.Sprintf("%x", md5Hash.Sum(nil))

	return
}
