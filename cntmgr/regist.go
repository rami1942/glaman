package cntmgr

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os"
	"path/filepath"
	"github.com/rami1942/glaman/glacier-manager"
	"github.com/rami1942/glaman/util"
)

/*
アーカイブへの登録

DB情報の更新とGlacierへの登録
*/
func RegisterToArchive(logger *log.Logger, db *sql.DB, path, fileName, vaultName, region string, key []byte) (err error) {

	iv, err := util.MakeIV()
	if err != nil {
		return
	}

	fullPath := filepath.Join(path, fileName)
	encFilePath := fullPath + ".enc"

	// 暗号化
	logger.Printf("暗号化: %v\n", fileName)
	md5sum, err := util.Encrypt(fullPath, encFilePath, key, iv)
	if err != nil {
		return
	}
	defer os.Remove(encFilePath)

	// 元データ情報記録
	id, err := recordPlainFileMeta(db, path, fileName, md5sum, iv)
	if err != nil {
		return
	}

	gmgr, err := glacier_manager.New("-", vaultName, region)
	if err != nil {
		return
	}
	// アップロード
	logger.Printf("アップロード: %v\n", fileName)
	archiveId, err := gmgr.UploadFile(logger, encFilePath)
	if err != nil {
		return
	}

	_, err = db.Exec("update file_entry set archive_id=? where id=?", archiveId, id)
	if err != nil {
		return errors.WithStack(err)
	}

	logger.Printf("アップロード完了: %v\n", fileName)
	return
}

func recordPlainFileMeta(db *sql.DB, path, fileName string, md5sum, iv []byte) (id int64, err error) {

	fullPath := filepath.Join(path, fileName)

	fi, err := os.Stat(fullPath)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	result, err := db.Exec("insert into file_entry (md5sum, name, mtime, size) values (?, ?, ?, ?)",
		fmt.Sprintf("%x", md5sum), fileName, fi.ModTime().UnixNano(), fi.Size())
	if err != nil {
		return 0, errors.WithStack(err)
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	id = lastInsertID

	_, err = db.Exec("insert into initial_vector (id, iv) values (?, ?)", lastInsertID, iv)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return lastInsertID, nil
}
