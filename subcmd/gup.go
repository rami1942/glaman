package subcmd

import (
	"fmt"
	"github.com/rami1942/glaman/cntmgr"
	"github.com/rami1942/glaman/util"
)

func Gup(cfg *util.Config, files []string) (err error) {
	for _, fileName := range files {
		err = cntmgr.RegisterToArchive(cfg.Logger, cfg.Database, ".", fileName, cfg.VaultName, cfg.Region, cfg.Key)
		if err != nil {
			fmt.Printf("upload failed. skip..(%+v)\n", err)
		}
	}
	return
}
