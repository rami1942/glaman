package main

import (
	"database/sql"
	"fmt"
	"github.com/alecthomas/kingpin"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"github.com/rami1942/glaman/subcmd"
	"github.com/rami1942/glaman/util"
)

var (
	home = os.Getenv("HOME")
	app        = kingpin.New("glaman", "Glacier file Manager")
	goptDBName = app.Flag("db", "データベース名").Short('d').Default(home + "/Documents/glaman.sqlite3").ExistingFile()

	scmdNew  = app.Command("newdb", "インデックスDBの新規作成")
	sNewName = scmdNew.Arg("dbname", "インデックスDB名").Default(home + "/Documents/glaman.sqlite3").String()
	sNewRegion = scmdNew.Flag("region", "リージョン").Required().String()
	sNewVault = scmdNew.Flag("vault", "Vault名").Required().String()
	sNewBaseDir = scmdNew.Flag("basedir", "同期対象ディレクトリ").Required().ExistingDir()
	sNewPassword = scmdNew.Flag("password", "パスワード").Required().String()


	scmdLs     = app.Command("ls", "アーカイブファイル一覧")
	sLsPattern = scmdLs.Arg("dir", "ディレクトリ").String()
	sLsComment = scmdLs.Flag("comment", "コメントを表示").Short('c').Bool()
	sLsLock    = scmdLs.Flag("lock", "ロックされているファイルを表示").Short('L').Bool()

	scmdGdlById  = app.Command("gdlbycid", "Glacier DL(ジョブID指定)")
	sGdlJobId    = scmdGdlById.Arg("jobid", "ジョブID").Required().String()
	sGdlFileName = scmdGdlById.Arg("filename", "ファイル名").Required().String()
	sGdlVault    = scmdGdlById.Flag("vault", "vault名").Short('v').Default("mars").String()
	sGdlRegion   = scmdGdlById.Flag("region", "region名").Short('r').Default("us-west-2").String()

	scmdGup      = app.Command("gup", "Glacier UPload")
	sGupFileName = scmdGup.Arg("filename", "ファイル名").Required().ExistingFiles()

	scmdSync   = app.Command("sync", "Glacierとの同期")
	sSyncDoRun = scmdSync.Flag("run", "実際の処理を実行").Short('r').Bool()

	scmdTest      = app.Command("test", "テスト用")
	sTestFileName = scmdTest.Arg("filename", "ファイル名").Required().ExistingFile()

	scmdJobStatus = app.Command("jobstatus", "Glacierジョブの状態")

	scmdLock = app.Command("lock", "ファイルのロック")
	sLockIds = scmdLock.Arg("id", "エントリID").Int64List()

	scmdUnlock = app.Command("unlock", "ファイルのアンロック")
	sUnlockIds = scmdUnlock.Arg("id", "エントリID").Int64List()

	scmdClean = app.Command("clean", "アンロックファイルの削除")
)

func main() {
	logger := log.New(os.Stderr, "", log.Lshortfile|log.LstdFlags)

	pv := kingpin.MustParse(app.Parse(os.Args[1:]))
	if pv == scmdNew.FullCommand() {
		err := subcmd.NewDB(logger, *sNewName, *sNewRegion, *sNewVault, *sNewBaseDir, *sNewPassword)
		if err != nil {
			logger.Printf("%+v\n", err)
		}
		return
	}

	db, err := sql.Open("sqlite3", *goptDBName)
	if err != nil {
		fmt.Printf("%s: DBが開けませんでした(%+v)", *goptDBName, err)
	}
	defer db.Close()

	cfg, err := util.NewConfig(logger, db)
	if err != nil {
		fmt.Printf("設定の取得に失敗しました(%+v)", err)
	}

	switch pv {
	case scmdLs.FullCommand():
		err = subcmd.Ls(cfg, *sLsComment, *sLsLock)
	case scmdGdlById.FullCommand():
		// marsからの使用も考えてvault, regionを一応パラメータ化しておく
		err = subcmd.Gdl(cfg, *sGdlJobId, *sGdlFileName, *sGdlVault, *sGdlRegion)
	case scmdGup.FullCommand():
		err = subcmd.Gup(cfg, *sGupFileName)
	case scmdTest.FullCommand():
		err = subcmd.SubCmdTest(logger, *goptDBName, *sTestFileName, cfg.Key)
	case scmdSync.FullCommand():
		err = subcmd.Sync(cfg, *sSyncDoRun)
	case scmdJobStatus.FullCommand():
		err = subcmd.JobStatus(cfg)
	case scmdClean.FullCommand():
		err = subcmd.Clean(cfg)
	case scmdLock.FullCommand():
		err = subcmd.Lock(cfg, *sLockIds, 1)
	case scmdUnlock.FullCommand():
		err = subcmd.Lock(cfg, *sUnlockIds, 0)
	}

	if err != nil {
		logger.Printf("%+v\n", err)
	}
}
