package glacier_manager

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/glacier"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	DL_CHUNK_SIZE_MB = 8
	DL_CHUNK_SIZE    = DL_CHUNK_SIZE_MB * 1024 * 1024
	NUM_DL_GOROUTINE = 8
)

var (
	ErrJobNotComplete = errors.New("Job is not completed.")
)

type dlSpec struct {
	id       int
	from, to int64
}

func (m *Manager) DownloadFile(logger *log.Logger, jobId, filePath string) (err error) {
	logger.Printf("check job %s", filePath)
	// ジョブ一覧取得
	job, err := m.DescribeJob(jobId)
	if err != nil {
		err = errors.Cause(err)
		return
	}

	if !*job.Completed {
		return ErrJobNotComplete
	}
	logger.Printf("Retrieve job %s", filePath)

	path, _ := filepath.Split(filePath)
	err = os.MkdirAll(path, 0755)
	if err != nil {
		return errors.WithStack(err)
	}

	size := *job.ArchiveSizeInBytes
	chunkSize := int64(DL_CHUNK_SIZE)

	var chunks []dlSpec
	i := 1
	for p := int64(0); p < size; p += chunkSize {
		px := p + chunkSize - 1
		if px > size {
			px = size - 1
		}
		chunks = append(chunks, dlSpec{i, p, px})
		i++
	}

	q := make(chan dlSpec, len(chunks))
	for _, v := range chunks {
		q <- v
	}
	close(q)
	logger.Printf("Num chunks = %d", len(chunks))
	defer cleanupTemp(filePath, len(chunks))

	var wg sync.WaitGroup
	for i := 0; i < NUM_DL_GOROUTINE; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				cp, ok := <-q
				if !ok {
					return
				}
				m.downloadChunk(cp, jobId, filePath)
			}

		}()
	}
	wg.Wait()
	logger.Printf("Download done.\n")

	// マージ

	of, err := os.Create(filePath)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	defer of.Close()

	for i := 1; i <= len(chunks); i++ {
		path, fileName := filepath.Split(filePath)
		fn := filepath.Join(path, fmt.Sprintf("mars-%v_%03d.tmp", fileName, i))

		err = doAppend(of, fn)
		if err != nil {
			err = errors.WithMessage(err, fmt.Sprintf("pos =%v", i))
			return
		}
	}
	return
}

func cleanupTemp(filePath string, n int) {
	for i := 1; i <= n; i++ {
		path, fileName := filepath.Split(filePath)
		fn := filepath.Join(path, fmt.Sprintf("mars-%v_%03d.tmp", fileName, i))
		os.Remove(fn)
	}
}

func (m *Manager) downloadChunk(spec dlSpec, jobId, filePath string) {
	fmt.Printf("%v: retrieve %v-%v\n", spec.id, spec.from, spec.to)

	svc := glacier.New(m.AwsSession)

	path, fileName := filepath.Split(filePath)
	fn := filepath.Join(path, fmt.Sprintf("mars-%v_%03d.tmp", fileName, spec.id))

	var jo glacier.GetJobOutputInput
	jo.SetAccountId(m.Account).SetJobId(jobId).SetRange(fmt.Sprintf("bytes=%d-%d", spec.from, spec.to)).SetVaultName(m.Vault)

	for {
		out, err := svc.GetJobOutput(&jo)
		if err == nil {
			err = doCopy(fn, out.Body)
			if err == nil {
				break
			} else {
				fmt.Printf("%d: copy failed.(%v) retry..\n", spec.id, err)
				os.Remove(fn)
				continue
			}
		}
		fmt.Printf("%d : extract failed.(%v) retry..\n", spec.id, err)
		os.Remove(fn)
	}
	fmt.Printf("%d: done.\n", spec.id)
}

func doCopy(fileName string, reader io.Reader) error {
	f, err := os.Create(fileName)
	if err != nil {
		err = errors.WithStack(err)
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, reader)
	return errors.WithStack(err)
}

func doAppend(writer *os.File, fileName string) (err error) {
	f, err := os.Open(fileName)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	defer f.Close()

	_, err = io.Copy(writer, f)
	return errors.WithStack(err)
}
