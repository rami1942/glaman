package glacier_manager

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/aws/aws-sdk-go/service/glacier"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"sync"
)

const (
	UPL_CHUNK_SIZE_MB = 8
	ONE_MB            = 1024 * 1024
	UPL_CHUNK_SIZE    = UPL_CHUNK_SIZE_MB * ONE_MB
	NUM_UPL_GORUTINE  = 16
)

type chunkParam struct {
	id         int
	start, end int64
	logger     *log.Logger
}

type chunkResult struct {
	id   int
	err  error
	hash []byte
}

func (m *Manager) UploadFile(logger *log.Logger, fileName string) (archiveId string, err error) {

	fi2, err := os.Stat(fileName)
	if err != nil {
		err = errors.Cause(err)
		return
	}
	size := fi2.Size()

	svc := glacier.New(m.AwsSession)

	var initUplReq glacier.InitiateMultipartUploadInput
	initUplReq.SetVaultName(m.Vault).SetAccountId(m.Account).SetPartSize(fmt.Sprintf("%d", UPL_CHUNK_SIZE))

	initUplRes, err := svc.InitiateMultipartUpload(&initUplReq)
	if err != nil {
		err = errors.Cause(err)
		return
	}
	uploadId := *initUplRes.UploadId
	logger.Printf("Initiate upload done. uploadID=%v\n", uploadId)

	// パラメータ取得&キュー登録
	var params []chunkParam
	i := 0
	for p := int64(0); p < size; p += UPL_CHUNK_SIZE {
		pe := p + UPL_CHUNK_SIZE - 1
		if pe > size {
			pe = size - 1
		}
		params = append(params, chunkParam{i, p, pe, logger})
		i++
	}
	q := make(chan chunkParam, i)
	for i = 0; i < len(params); i++ {
		q <- params[i]
	}
	close(q)
	logger.Printf("Num of chunks = %d", len(params))

	reschan := make(chan chunkResult, len(params))

	// goroutine実行
	var wg sync.WaitGroup
	for i := 0; i < NUM_UPL_GORUTINE; i++ {
		wg.Add(1)
		go func(q chan chunkParam, reschan chan chunkResult, fileName, uploadId string) {
			defer wg.Done()
			for {
				cp, ok := <-q
				if !ok {
					return
				}
				m.processChunk(fileName, uploadId, cp, reschan)

			}
		}(q, reschan, fileName, uploadId)
	}
	wg.Wait()

	// 各goroutine結果チェック&ツリーハッシュ集約
	success := true
	hashes := make([][]byte, len(params))
	for i := 0; i < len(params); i++ {
		res := <-reschan
		if res.err != nil {
			err = res.err
			success = false
			break
		}
		hashes[res.id] = res.hash
	}

	if !success {
		logger.Printf("Upload failed. Abort upload.\n")

		var abortMUInput glacier.AbortMultipartUploadInput
		abortMUInput.SetUploadId(uploadId).SetVaultName(m.Vault).SetAccountId(m.Account)
		svc.AbortMultipartUpload(&abortMUInput)

		return
	}

	treeHash := glacier.ComputeTreeHash(hashes)
	logger.Printf("Total hash: %x\n", treeHash)

	// アップロード終了指示
	var completeMUInput glacier.CompleteMultipartUploadInput
	completeMUInput.SetAccountId(m.Account).SetVaultName(m.Vault).SetUploadId(uploadId).
		SetChecksum(fmt.Sprintf("%x", treeHash)).SetArchiveSize(fmt.Sprintf("%d", size))

	archiveCreationOut, err := svc.CompleteMultipartUpload(&completeMUInput)
	if err != nil {
		err = errors.Cause(err)
		return
	}

	archiveId = *archiveCreationOut.ArchiveId
	logger.Printf("Upload success. archiveId=%v\n", archiveId)
	return
}

// チャンクのアップロード処理
func (m *Manager) processChunk(fileName, uploadId string, cp chunkParam, reschan chan chunkResult) {
	cp.logger.Printf("upload chunk %d\n", cp.id)

	f, _ := os.Open(fileName)
	defer f.Close()

	// チャンクの読み込み
	_, err := f.Seek(cp.start, 0)
	if err != nil {
		err = errors.Wrapf(err, "%v", cp.id)
		reschan <- chunkResult{cp.id, err, nil}
		return
	}
	buf := make([]byte, UPL_CHUNK_SIZE)
	n, err := io.ReadAtLeast(f, buf, int(cp.end-cp.start))
	if err != nil {
		err = errors.Wrapf(err, "%v", cp.id)
		reschan <- chunkResult{cp.id, err, nil}
		return
	}

	// treehash計算
	hashes := [][]byte{}
	for p := 0; p < n; p += ONE_MB {
		pe := p + ONE_MB
		if pe > n {
			pe = n
		}
		tmpHash := sha256.Sum256(buf[p:pe])
		hashes = append(hashes, tmpHash[:])
	}
	treeHash := glacier.ComputeTreeHash(hashes)

	// Glacier UPL
	svc := glacier.New(m.AwsSession)

	var uplMPInput glacier.UploadMultipartPartInput
	uplMPInput.SetAccountId(m.Account).SetBody(bytes.NewReader(buf[:n])).
		SetChecksum(fmt.Sprintf("%x", treeHash)).SetRange(fmt.Sprintf("bytes %v-%v/*", cp.start, cp.end)).
		SetVaultName(m.Vault).SetUploadId(uploadId)

	err = uplMPInput.Validate()
	if err != nil {
		err = errors.Cause(err)
		reschan <- chunkResult{cp.id, err, nil}
		return
	}

	for {
		_, err = svc.UploadMultipartPart(&uplMPInput)
		if err == nil {
			cp.logger.Printf("%v : Chunk upload done.", cp.id)
			break
		}

		cp.logger.Printf("%v : Chunk upload failed. Retry..(%v)\n", cp.id, err)

		uplMPInput.Body.Seek(0, 0)

	}

	reschan <- chunkResult{cp.id, nil, treeHash}
}
