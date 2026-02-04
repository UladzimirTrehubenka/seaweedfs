package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	remote_storage.RemoteStorageClientMakers["wasabi"] = new(WasabiRemoteStorageMaker)
}

type WasabiRemoteStorageMaker struct{}

func (s WasabiRemoteStorageMaker) HasBucket() bool {
	return true
}

func (s WasabiRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		supportTagging: true,
		conf:           conf,
	}
	accessKey := util.Nvl(conf.GetWasabiAccessKey())
	secretKey := util.Nvl(conf.GetWasabiSecretKey())

	config := &aws.Config{
		Endpoint:                      aws.String(conf.GetWasabiEndpoint()),
		Region:                        aws.String(conf.GetWasabiRegion()),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(true),
	}
	if accessKey != "" && secretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create wasabi session: %w", err)
	}
	sess.Handlers.Build.PushFront(skipSha256PayloadSigning)
	client.conn = s3.New(sess)

	return client, nil
}

var skipSha256PayloadSigning = func(r *request.Request) {
	// see https://github.com/ceph/ceph/pull/15965/files
	if r.ClientInfo.ServiceID != "S3" {
		return
	}
	if r.Operation.Name == "PutObject" || r.Operation.Name == "UploadPart" {
		if len(r.HTTPRequest.Header.Get("X-Amz-Content-Sha256")) == 0 {
			r.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		}
	}
}
