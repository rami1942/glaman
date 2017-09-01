package glacier_manager

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	"github.com/pkg/errors"
)

type Manager struct {
	Account, Vault, Region string

	AwsSession *session.Session
}

func New(account, vault, region string) (manager *Manager, err error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return
	}
	manager = &Manager{account, vault, region, sess}
	return
}

func (m *Manager) DescribeJob(jobId string) (jobDesc *glacier.JobDescription, err error) {
	svc := glacier.New(m.AwsSession)

	// ジョブ一覧取得
	var ji glacier.DescribeJobInput
	ji.SetAccountId("-").
		SetJobId(jobId).
		SetVaultName(m.Vault)

	jobDesc, err = svc.DescribeJob(&ji)
	return
}

func (m *Manager) RequestRetrieve(archiveId string) (string, error) {

	svc := glacier.New(m.AwsSession)

	jobParam := glacier.JobParameters{
		ArchiveId: aws.String(archiveId),
		Tier:      aws.String("Standard"),
		Type:      aws.String("archive-retrieval"),
	}

	jobInput := glacier.InitiateJobInput{
		AccountId:     aws.String("-"),
		JobParameters: &jobParam,
		VaultName:     aws.String(m.Vault),
	}

	out, err := svc.InitiateJob(&jobInput)
	if err != nil {
		return "", errors.WithStack(err)
	}

	return *out.JobId, nil
}

func (m *Manager) JobList() (jobDesc []*glacier.JobDescription, err error) {
	svc := glacier.New(m.AwsSession)

	ji := glacier.ListJobsInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(m.Vault),
	}

	out, err := svc.ListJobs(&ji)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return out.JobList, nil
}
