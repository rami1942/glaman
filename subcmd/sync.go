package subcmd

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"time"
	"github.com/rami1942/glaman/cntmgr"
	"github.com/rami1942/glaman/glacier-manager"
	"github.com/rami1942/glaman/model"
	"github.com/rami1942/glaman/util"
)

var (
	ErrMD5Mismatch = errors.New("md5sum is not matched")
)

func Sync(config *util.Config, doRun bool) error {
	//	config.Logger.Printf("base directory=%s", config.DocRoot)
	if !doRun {
		config.Logger.Printf("ドライランモードのため、実際のアップロード/ダウンロードは行われません。行うには-rオプションをつけてください。")
	}
	config.Logger.Printf("アップロードのチェック")
	err := checkUpl(config, doRun)
	if err != nil {
		return err
	}

	config.Logger.Printf("ダウンロードのチェック")
	err = checkDown(config, doRun)

	return err
}

/*
ディレクトリをスキャンしてGlacierに登録されていなかったら登録する
*/
func checkUpl(config *util.Config, doRun bool) error {
	err := filepath.Walk(config.DocRoot,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.WithStack(err)
			}
			relPath, err := filepath.Rel(config.DocRoot, path)
			if err != nil {
				return errors.WithStack(err)
			}
			if relPath == "." {
				return nil
			}

			if info.IsDir() {
				//				fmt.Printf("d:%v\n", relPath)
				// Skip
			} else {
				err = keepInGlacier(config, relPath, doRun)
				if err != nil {
					return err
				}
			}

			return nil
		})
	return err
}

/*
Glacier登録チェック
*/
func keepInGlacier(config *util.Config, relPath string, doRun bool) error {

	// まずパスでDBに当たってみる
	ent, err := model.FindEntryByName(config.Database, relPath)
	if err != nil {
		return err
	}
	if ent != nil {
		if ent.Lock == 0 {
			config.Logger.Printf("%v: ファイルは存在しますがロックされていません", relPath)
		}
		return nil
	}

	fullPath := filepath.Join(config.DocRoot, relPath)
	// MD5でDBに当たってみる
	md5sum, err := util.GetMD5(fullPath)
	if err != nil {
		return err
	}
	ent, err = model.FindEntryByMD5(config.Database, md5sum)
	if err != nil {
		return err
	}
	if ent != nil {
		config.Logger.Printf("%v Exists same MD5 in DB.", relPath)
		config.Logger.Printf("Rewrite path: %s -> %s", ent.Name, relPath)
		if doRun {
			err = model.UpdateName(config.Database, ent.Id, relPath)
		}
		return err
	}
	config.Logger.Printf("%v : Not exists so regist it.", relPath)

	// 新規なので登録する
	if doRun {
		err = cntmgr.RegisterToArchive(config.Logger, config.Database, config.DocRoot, relPath, config.VaultName, config.Region, config.Key)
	} else {
		config.Logger.Printf("DRY RUN: upload %v to Glacier.", relPath)
	}

	return err
}

/*
Lockフラグが付いているエントリについて、実在しているかを確認する
*/
func checkDown(config *util.Config, doRun bool) error {

	entry, err := model.LockedEntry(config.Database)
	if err != nil {
		return err
	}

	for _, e := range entry {
		fullPath := filepath.Join(config.DocRoot, e.Name)

		_, err := os.Stat(fullPath)
		if err != nil {
			if os.IsNotExist(err) {
				err = processExtract(config, e, doRun)
			} else {
				return errors.WithStack(err)
			}
		}

	}

	return nil
}

func processExtract(config *util.Config, entry model.FileEntry, doRun bool) error {
	// jobが発行されてるかチェック
	ex, err := model.FindExRequestById(config.Database, entry.Id)
	if err != nil {
		return err
	}
	if ex == nil {
		// 発行されてないので発行する
		if doRun {
			err = requestExtractJob(config, entry)
		} else {
			config.Logger.Printf("DRY RUN: Request extract %s", entry.Name)
		}
	} else {
		// 発行されてる
		if doRun {
			err = retrieve(config, *ex, entry)
		} else {
			config.Logger.Printf("DRY RUN: Try Extract %s", entry.Name)
		}
	}

	return err
}

func requestExtractJob(config *util.Config, entry model.FileEntry) error {
	fmt.Printf("%v is not exists. Request retrieve..\n", entry.Name)

	gmgr, err := config.GlacierManager()
	if err != nil {
		return err
	}

	jobId, err := gmgr.RequestRetrieve(entry.ArchiveId)
	if err != nil {
		return err
	}
	return model.InsertRequest(config.Database, entry.Id, jobId)
}

func retrieve(config *util.Config, ex model.ExRequest, entry model.FileEntry) error {
	gmgr, err := config.GlacierManager()
	if err != nil {
		return err
	}

	plainFile := filepath.Join(config.DocRoot, entry.Name)
	cryptFile := plainFile + ".enc"
	defer os.Remove(cryptFile)

	// DL
	err = gmgr.DownloadFile(config.Logger, ex.JobId, cryptFile)
	if err != nil {
		if err == glacier_manager.ErrJobNotComplete {
			config.Logger.Printf("%v: 取得ジョブがまだ完了していません。もうしばらくしてから実行してください(開始時刻=%s)\n", entry.Name, ex.StartDt.Format("2006/01/02 15:04:05"))
			return nil
		} else {
			return err
		}
	}
	config.Logger.Printf("DL終了。復号中..")

	// 復号
	iv, err := model.GetIV(config.Database, entry.Id)
	if err != nil {
		return err
	}
	dlsum, err := util.Decrypt(cryptFile, plainFile, config.Key, iv)
	if err != nil {
		return err
	}
	config.Logger.Printf("MD5チェック")

	// MD5チェック
	if entry.MD5Sum != fmt.Sprintf("%x", dlsum) {
		config.Logger.Printf("MD5 mismatch. DB=%v <-> File=%v", entry.MD5Sum, dlsum)
		return ErrMD5Mismatch
	}

	//タイムスタンプ復元
	t := time.Unix(0, entry.Mtime)
	err = os.Chtimes(plainFile, t, t)
	if err != nil {
		return errors.WithStack(err)
	}

	// ex_request削除
	err = model.DeleteRequest(config.Database, entry.Id)
	config.Logger.Printf("復元完了")
	return nil
}
