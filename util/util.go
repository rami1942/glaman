package util

import (
	"crypto/md5"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
)

func GetMD5(fileName string) (string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", errors.WithStack(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
