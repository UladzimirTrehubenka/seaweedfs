package filer

import (
	"fmt"
	"io"

	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func ParseS3ConfigurationFromBytes[T proto.Message](content []byte, config T) error {
	options := &jsonpb.UnmarshalOptions{
		DiscardUnknown: true,
		AllowPartial:   true,
	}
	if err := options.Unmarshal(content, config); err != nil {
		return err
	}

	return nil
}

func ProtoToText(writer io.Writer, config proto.Message) error {
	m := jsonpb.MarshalOptions{
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(config)
	if marshalErr != nil {
		return fmt.Errorf("marshal proto message: %w", marshalErr)
	}

	_, writeErr := writer.Write(text)
	if writeErr != nil {
		return fmt.Errorf("fail to write proto message: %w", writeErr)
	}

	return writeErr
}

// CheckDuplicateAccessKey returns an error message when s3cfg has duplicate access keys
func CheckDuplicateAccessKey(s3cfg *iam_pb.S3ApiConfiguration) error {
	accessKeySet := make(map[string]string)
	for _, ident := range s3cfg.GetIdentities() {
		for _, cred := range ident.GetCredentials() {
			if userName, found := accessKeySet[cred.GetAccessKey()]; !found {
				accessKeySet[cred.GetAccessKey()] = ident.GetName()
			} else if userName != ident.GetName() {
				return fmt.Errorf("duplicate accessKey[%s], already configured in user[%s]", cred.GetAccessKey(), userName)
			}
		}
	}

	return nil
}
