package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandS3BucketQuota{})
}

type commandS3BucketQuota struct {
}

func (c *commandS3BucketQuota) Name() string {
	return "s3.bucket.quota"
}

func (c *commandS3BucketQuota) Help() string {
	return `set/remove/enable/disable quota for a bucket

	Example:
		s3.bucket.quota -name=<bucket_name> -op=set -sizeMB=1024
`
}

func (c *commandS3BucketQuota) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketQuota) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	operationName := bucketCommand.String("op", "set", "operation name [set|get|remove|enable|disable]")
	sizeMB := bucketCommand.Int64("sizeMB", 0, "bucket quota size in MiB")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return errors.New("empty bucket name")
	}

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()

		resp, err := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.GetDirBuckets()

		lookupResp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerBucketsPath,
			Name:      *bucketName,
		})
		if err != nil {
			return fmt.Errorf("did not find bucket %s: %w", *bucketName, err)
		}
		bucketEntry := lookupResp.GetEntry()

		switch *operationName {
		case "set":
			bucketEntry.Quota = *sizeMB * 1024 * 1024
		case "get":
			fmt.Fprintf(writer, "bucket quota: %dMiB \n", bucketEntry.GetQuota()/1024/1024)

			return nil
		case "remove":
			bucketEntry.Quota = 0
		case "enable":
			if bucketEntry.GetQuota() < 0 {
				bucketEntry.Quota = -bucketEntry.GetQuota()
			}
		case "disable":
			if bucketEntry.GetQuota() > 0 {
				bucketEntry.Quota = -bucketEntry.GetQuota()
			}
		}

		if err := filer_pb.UpdateEntry(context.Background(), client, &filer_pb.UpdateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     bucketEntry,
		}); err != nil {
			return err
		}

		println("updated quota for bucket", *bucketName)

		return nil
	})

	return err
}
