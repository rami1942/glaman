package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"github.com/pkg/errors"
	"io"
	"os"
)

const bufsize = 16 * 1024

func MakeIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, errors.WithStack(err)
	}
	return iv, nil
}

func Decrypt(cryptedFile, plainFile string, key []byte, iv []byte) (plainMd5sum []byte, err error) {

	inFile, err := os.Open(cryptedFile)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer inFile.Close()

	outFile, err := os.Create(plainFile)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer outFile.Close()

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	hash := md5.New()

	stream := cipher.NewCTR(block, iv)
	reader := &cipher.StreamReader{S: stream, R: inFile}

	buf := make([]byte, bufsize)
	for {
		n, err := reader.Read(buf)
		if n == 0 {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}

		_, err = hash.Write(buf[:n])
		if err != nil {
			return nil, errors.WithStack(err)
		}

		_, err = outFile.Write(buf[:n])
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	plainMd5sum = hash.Sum(nil)
	return
}

func Encrypt(plainFile, cryptedFile string, key []byte, iv []byte) (plainMd5sum []byte, err error) {
	inFile, err := os.Open(plainFile)
	if err != nil {
		return
	}
	defer inFile.Close()

	hash := md5.New()

	outFile, err := os.Create(cryptedFile)
	if err != nil {
		return
	}
	defer outFile.Close()

	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}
	stream := cipher.NewCTR(block, iv)
	writer := &cipher.StreamWriter{S: stream, W: outFile}

	buf := make([]byte, bufsize)
	for {
		n, err := inFile.Read(buf)
		if n == 0 {
			break
		}
		if err != nil {
			return nil, err
		}

		_, err = hash.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		_, err = writer.Write(buf[:n])
		if err != nil {
			return nil, err
		}
	}
	plainMd5sum = hash.Sum(nil)
	return
}
