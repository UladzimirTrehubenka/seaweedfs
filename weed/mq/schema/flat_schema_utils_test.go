package schema

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestSplitFlatSchemaToKeyValue(t *testing.T) {
	// Create a test flat schema
	flatSchema := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "user_id",
				FieldIndex: 0,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}},
			},
			{
				Name:       "session_id",
				FieldIndex: 1,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
			{
				Name:       "event_type",
				FieldIndex: 2,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
			{
				Name:       "timestamp",
				FieldIndex: 3,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}},
			},
		},
	}

	keyColumns := []string{"user_id", "session_id"}

	keySchema, valueSchema, err := SplitFlatSchemaToKeyValue(flatSchema, keyColumns)
	if err != nil {
		t.Fatalf("SplitFlatSchemaToKeyValue failed: %v", err)
	}

	// Verify key schema
	if keySchema == nil {
		t.Fatal("Expected key schema, got nil")
	}
	if len(keySchema.GetFields()) != 2 {
		t.Errorf("Expected 2 key fields, got %d", len(keySchema.GetFields()))
	}
	if keySchema.GetFields()[0].GetName() != "user_id" || keySchema.GetFields()[1].GetName() != "session_id" {
		t.Errorf("Key field names incorrect: %v", []string{keySchema.GetFields()[0].GetName(), keySchema.GetFields()[1].GetName()})
	}

	// Verify value schema
	if valueSchema == nil {
		t.Fatal("Expected value schema, got nil")
	}
	if len(valueSchema.GetFields()) != 2 {
		t.Errorf("Expected 2 value fields, got %d", len(valueSchema.GetFields()))
	}
	if valueSchema.GetFields()[0].GetName() != "event_type" || valueSchema.GetFields()[1].GetName() != "timestamp" {
		t.Errorf("Value field names incorrect: %v", []string{valueSchema.GetFields()[0].GetName(), valueSchema.GetFields()[1].GetName()})
	}

	// Verify field indices are reindexed
	for i, field := range keySchema.GetFields() {
		if field.GetFieldIndex() != int32(i) {
			t.Errorf("Key field %s has incorrect index %d, expected %d", field.GetName(), field.GetFieldIndex(), i)
		}
	}
	for i, field := range valueSchema.GetFields() {
		if field.GetFieldIndex() != int32(i) {
			t.Errorf("Value field %s has incorrect index %d, expected %d", field.GetName(), field.GetFieldIndex(), i)
		}
	}
}

func TestSplitFlatSchemaToKeyValueMissingColumns(t *testing.T) {
	flatSchema := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{Name: "field1", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
		},
	}

	keyColumns := []string{"field1", "missing_field"}

	_, _, err := SplitFlatSchemaToKeyValue(flatSchema, keyColumns)
	if err == nil {
		t.Error("Expected error for missing key column, got nil")
	}
	if !contains(err.Error(), "missing_field") {
		t.Errorf("Error should mention missing_field: %v", err)
	}
}

func TestCombineFlatSchemaFromKeyValue(t *testing.T) {
	keySchema := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "user_id",
				FieldIndex: 0,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}},
			},
			{
				Name:       "session_id",
				FieldIndex: 1,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
		},
	}

	valueSchema := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "event_type",
				FieldIndex: 0,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
			{
				Name:       "timestamp",
				FieldIndex: 1,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}},
			},
		},
	}

	flatSchema, keyColumns := CombineFlatSchemaFromKeyValue(keySchema, valueSchema)

	// Verify combined schema
	if flatSchema == nil {
		t.Fatal("Expected flat schema, got nil")
	}
	if len(flatSchema.GetFields()) != 4 {
		t.Errorf("Expected 4 fields, got %d", len(flatSchema.GetFields()))
	}

	// Verify key columns
	expectedKeyColumns := []string{"user_id", "session_id"}
	if !reflect.DeepEqual(keyColumns, expectedKeyColumns) {
		t.Errorf("Expected key columns %v, got %v", expectedKeyColumns, keyColumns)
	}

	// Verify field order (key fields first)
	expectedNames := []string{"user_id", "session_id", "event_type", "timestamp"}
	actualNames := make([]string, len(flatSchema.GetFields()))
	for i, field := range flatSchema.GetFields() {
		actualNames[i] = field.GetName()
	}
	if !reflect.DeepEqual(actualNames, expectedNames) {
		t.Errorf("Expected field names %v, got %v", expectedNames, actualNames)
	}

	// Verify field indices are sequential
	for i, field := range flatSchema.GetFields() {
		if field.GetFieldIndex() != int32(i) {
			t.Errorf("Field %s has incorrect index %d, expected %d", field.GetName(), field.GetFieldIndex(), i)
		}
	}
}

func TestExtractKeyColumnsFromCombinedSchema(t *testing.T) {
	// Create a combined schema with key_ prefixes (as created by CreateCombinedRecordType)
	combinedSchema := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "key_user_id",
				FieldIndex: 0,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}},
			},
			{
				Name:       "key_session_id",
				FieldIndex: 1,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
			{
				Name:       "event_type",
				FieldIndex: 2,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
			{
				Name:       "timestamp",
				FieldIndex: 3,
				Type:       &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}},
			},
		},
	}

	flatSchema, keyColumns := ExtractKeyColumnsFromCombinedSchema(combinedSchema)

	// Verify flat schema
	if flatSchema == nil {
		t.Fatal("Expected flat schema, got nil")
	}
	if len(flatSchema.GetFields()) != 4 {
		t.Errorf("Expected 4 fields, got %d", len(flatSchema.GetFields()))
	}

	// Verify key columns (should be sorted)
	expectedKeyColumns := []string{"session_id", "user_id"}
	if !reflect.DeepEqual(keyColumns, expectedKeyColumns) {
		t.Errorf("Expected key columns %v, got %v", expectedKeyColumns, keyColumns)
	}

	// Verify field names (key_ prefixes removed)
	expectedNames := []string{"user_id", "session_id", "event_type", "timestamp"}
	actualNames := make([]string, len(flatSchema.GetFields()))
	for i, field := range flatSchema.GetFields() {
		actualNames[i] = field.GetName()
	}
	if !reflect.DeepEqual(actualNames, expectedNames) {
		t.Errorf("Expected field names %v, got %v", expectedNames, actualNames)
	}
}

func TestValidateKeyColumns(t *testing.T) {
	schema := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{Name: "field1", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
			{Name: "field2", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}},
		},
	}

	// Valid key columns
	err := ValidateKeyColumns(schema, []string{"field1"})
	if err != nil {
		t.Errorf("Expected no error for valid key columns, got: %v", err)
	}

	// Invalid key columns
	err = ValidateKeyColumns(schema, []string{"field1", "missing_field"})
	if err == nil {
		t.Error("Expected error for invalid key columns, got nil")
	}

	// Nil schema should not error
	err = ValidateKeyColumns(nil, []string{"any_field"})
	if err != nil {
		t.Errorf("Expected no error for nil schema, got: %v", err)
	}

	// Empty key columns should not error
	err = ValidateKeyColumns(schema, []string{})
	if err != nil {
		t.Errorf("Expected no error for empty key columns, got: %v", err)
	}
}

// Helper function to check if string contains substring
func contains(str, substr string) bool {
	return len(str) >= len(substr) &&
		(len(substr) == 0 || str[len(str)-len(substr):] == substr ||
			str[:len(substr)] == substr ||
			len(str) > len(substr) && (str[len(str)-len(substr)-1:len(str)-len(substr)] == " " || str[len(str)-len(substr)-1] == ' ') && str[len(str)-len(substr):] == substr ||
			findInString(str, substr))
}

func findInString(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
