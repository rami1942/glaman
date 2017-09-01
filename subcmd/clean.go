package subcmd

import (
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"github.com/rami1942/glaman/model"
	"github.com/rami1942/glaman/util"
	"sort"
)

// パスの長い順にソート
type dirs []string

func (d dirs) Len() int {
	return len(d)
}

func (d dirs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d dirs) Less(i, j int) bool {
	return len(d[i]) > len(d[j])
}

func Clean(config *util.Config) error {

	var dirList dirs

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
				dirList = append(dirList, path)
			} else {
				err = cleanFile(config, path, relPath)
			}

			return nil
		})

	sort.Sort(dirList)
	for _, d := range dirList {
		err = removeIfEmpty(d)
		if err != nil {
			return err
		}
	}

	return err
}

func cleanFile(config *util.Config, fullPath, relPath string) error {
	ent, err := model.FindEntryByName(config.Database, relPath)
	if err != nil {
		return err
	}
	if ent.Lock == 0 {
		md5file, err := util.GetMD5(fullPath)
		if err != nil {
			return err
		}

		if md5file == ent.MD5Sum {
			fmt.Printf("%v: MD5一致。ロックされていないので削除します。\n", relPath)
			err = os.Remove(fullPath)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

/*
下位にファイルがなければディレクトリを削除する
*/
func removeIfEmpty(path string) error {
	fi, err := ioutil.ReadDir(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(fi) == 0 {
		err = os.Remove(path)
		if err != nil {
			return err
		}
	}
	return nil
}
