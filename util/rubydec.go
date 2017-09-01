package util

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"fmt"
	"os"
)

const (
	key_len   = 32
	iv_len    = 16
	count     = 2048
	total_len = key_len + iv_len
)

type MiscError struct {
	Msg   string
	Cause error
}

func (e MiscError) Error() string {
	return e.Msg
}

func NewMiscError(msg string, cause error) MiscError {
	return MiscError{msg, cause}
}

/*
func main() {
	err := Decrypt("2%]6vQGLL0,g", "1367815551168.enc", "1367815551168.dec")
	if err != nil {
		panic(err)
	}
}
*/

/*
passwordで暗号化されたcryptedFileを復号し、平文をoutFileに出力する。
フォーマットはencrypt.rbで暗号化したもの。
*/
func RubyDecrypt(password, cryptedFile, outFile string) error {
	f, err := os.Open(cryptedFile)
	if err != nil {
		return NewMiscError("Cryptfile open failed", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)

	of, err := os.Create(outFile)
	if err != nil {
		return NewMiscError("Plainfile write open failed", err)
	}
	defer of.Close()

	w := bufio.NewWriter(of)
	defer w.Flush()

	// Saltは8-16バイト目
	var header [16]byte
	n, err := r.Read(header[:])
	if err != nil {
		return NewMiscError("Can't read header", err)
	}
	if n != 16 {
		return NewMiscError("Can't read head 16 bytes", nil)
	}

	// ruby互換の鍵とIVを生成
	k, iv := GetKIV([]byte(password), header[8:16])

	block, err := aes.NewCipher(k)
	if err != nil {
		return err
	}
	bmode := cipher.NewCBCDecrypter(block, iv)

	fs, _ := os.Stat(cryptedFile)
	numBlocks := int((fs.Size() - 16) / aes.BlockSize)

	buf := make([]byte, 16)
	for i := 0; i < numBlocks; i++ {
		n, err = r.Read(buf)
		if err != nil {
			return NewMiscError("Can't read", err)
		}
		bmode.CryptBlocks(buf, buf)

		if i == numBlocks-1 {
			pad := int(buf[15])
			fmt.Printf("Padding = %d\n", pad)
			w.Write(buf[:aes.BlockSize-pad])
		} else {
			w.Write(buf)
		}
	}

	/*
		var buf [16] byte
		numWrite := int64(0)
		for numWrite < flen - 16 {
			n, err = r.Read(buf[:])
			if err != nil {
				return NewMiscError("Can't read ", err)
			}
			if n != 16 {
				return NewMiscError("Invalid read size", nil)
			}
			bmode.CryptBlocks(buf[:], buf[:])

			if numWrite == flen - 32 {
				// 最終ブロックの場合、最後の1バイトを見てその数だけサイズを減らす(PKCS5Padding)
				lb := int(buf[15])
				if lb == 16 {
					break
				}

				_, err = w.Write(buf[0:lb - 1])
				if err != nil {
					return NewMiscError("Write failed", err)
				}

				break
			} else {
				n, err = w.Write(buf[:])
				if err != nil {
					return NewMiscError("Write failed", err)
				}
				if n != 16 {
					return NewMiscError("Invalid write size", nil)
				}

				numWrite += 16
			}
		}
	*/

	return nil
}

func GetKIV(pass, salt []byte) ([]byte, []byte) {
	var return_buf bytes.Buffer
	var digest_buf bytes.Buffer

	for return_buf.Len() < total_len {
		digest_buf.Write(pass)
		digest_buf.Write(salt)
		s := digest_buf.Bytes()[:]
		for i := 0; i < count; i++ {
			t := md5.Sum(s)
			s = t[:]
		}
		return_buf.Write(s)
		digest_buf = *bytes.NewBuffer(s)
	}
	return return_buf.Bytes()[0:key_len], return_buf.Bytes()[key_len : key_len+iv_len]
}

//CORRECT: 0399e9b27061000a879f9728be869caf73747461813f0fa599fbd215c9f8e215
//         0399e9b27061000a879f9728be869caf73747461813f0fa599fbd215c9f8e21500000000000000000000000000000000

/*
func PKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}
*/
