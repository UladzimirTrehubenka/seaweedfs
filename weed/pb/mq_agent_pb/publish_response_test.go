package mq_agent_pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestPublishRecordResponseSerialization(t *testing.T) {
	// Test that PublishRecordResponse can serialize/deserialize with new offset fields
	original := &PublishRecordResponse{
		AckSequence: 123,
		Error:       "",
		BaseOffset:  1000, // New field
		LastOffset:  1005, // New field
	}

	// Test proto marshaling/unmarshaling
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal PublishRecordResponse: %v", err)
	}

	restored := &PublishRecordResponse{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal PublishRecordResponse: %v", err)
	}

	// Verify all fields are preserved
	if restored.GetAckSequence() != original.GetAckSequence() {
		t.Errorf("AckSequence = %d, want %d", restored.GetAckSequence(), original.GetAckSequence())
	}
	if restored.GetBaseOffset() != original.GetBaseOffset() {
		t.Errorf("BaseOffset = %d, want %d", restored.GetBaseOffset(), original.GetBaseOffset())
	}
	if restored.GetLastOffset() != original.GetLastOffset() {
		t.Errorf("LastOffset = %d, want %d", restored.GetLastOffset(), original.GetLastOffset())
	}
}

func TestSubscribeRecordResponseSerialization(t *testing.T) {
	// Test that SubscribeRecordResponse can serialize/deserialize with new offset field
	original := &SubscribeRecordResponse{
		Key:           []byte("test-key"),
		TsNs:          1234567890,
		Error:         "",
		IsEndOfStream: false,
		IsEndOfTopic:  false,
		Offset:        42, // New field
	}

	// Test proto marshaling/unmarshaling
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal SubscribeRecordResponse: %v", err)
	}

	restored := &SubscribeRecordResponse{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal SubscribeRecordResponse: %v", err)
	}

	// Verify all fields are preserved
	if restored.GetTsNs() != original.GetTsNs() {
		t.Errorf("TsNs = %d, want %d", restored.GetTsNs(), original.GetTsNs())
	}
	if restored.GetOffset() != original.GetOffset() {
		t.Errorf("Offset = %d, want %d", restored.GetOffset(), original.GetOffset())
	}
	if string(restored.GetKey()) != string(original.GetKey()) {
		t.Errorf("Key = %s, want %s", string(restored.GetKey()), string(original.GetKey()))
	}
}

func TestPublishRecordResponseBackwardCompatibility(t *testing.T) {
	// Test that PublishRecordResponse without offset fields still works
	original := &PublishRecordResponse{
		AckSequence: 123,
		Error:       "",
		// BaseOffset and LastOffset not set (defaults to 0)
	}

	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal PublishRecordResponse: %v", err)
	}

	restored := &PublishRecordResponse{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal PublishRecordResponse: %v", err)
	}

	// Offset fields should default to 0
	if restored.GetBaseOffset() != 0 {
		t.Errorf("BaseOffset = %d, want 0", restored.GetBaseOffset())
	}
	if restored.GetLastOffset() != 0 {
		t.Errorf("LastOffset = %d, want 0", restored.GetLastOffset())
	}
}
