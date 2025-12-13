package utils

import (
	"testing"
)

func TestToPtr(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{
			name:     "int",
			input:    42,
			expected: 42,
		},
		{
			name:     "string",
			input:    "test",
			expected: "test",
		},
		{
			name:     "bool true",
			input:    true,
			expected: true,
		},
		{
			name:     "bool false",
			input:    false,
			expected: false,
		},
		{
			name:     "float64",
			input:    3.14,
			expected: 3.14,
		},
		{
			name:     "struct",
			input:    struct{ Value int }{Value: 123},
			expected: struct{ Value int }{Value: 123},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.input.(type) {
			case int:
				result := ToPtr(v)
				if result == nil {
					t.Fatal("expected non-nil result")
				}
				if *result != v {
					t.Errorf("expected %v, got %v", v, *result)
				}
				// Verify it's actually a pointer by modifying it
				*result = 999
				if v == 999 {
					t.Error("original value should not be modified")
				}
			case string:
				result := ToPtr(v)
				if result == nil {
					t.Fatal("expected non-nil result")
				}
				if *result != v {
					t.Errorf("expected %v, got %v", v, *result)
				}
			case bool:
				result := ToPtr(v)
				if result == nil {
					t.Fatal("expected non-nil result")
				}
				if *result != v {
					t.Errorf("expected %v, got %v", v, *result)
				}
			case float64:
				result := ToPtr(v)
				if result == nil {
					t.Fatal("expected non-nil result")
				}
				if *result != v {
					t.Errorf("expected %v, got %v", v, *result)
				}
			case struct{ Value int }:
				result := ToPtr(v)
				if result == nil {
					t.Fatal("expected non-nil result")
				}
				if result.Value != v.Value {
					t.Errorf("expected %v, got %v", v, *result)
				}
			}
		})
	}
}

// TestToPtrZeroValues tests that ToPtr works correctly with zero values
func TestToPtrZeroValues(t *testing.T) {
	t.Run("zero int", func(t *testing.T) {
		result := ToPtr(0)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if *result != 0 {
			t.Errorf("expected 0, got %v", *result)
		}
	})

	t.Run("empty string", func(t *testing.T) {
		result := ToPtr("")
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if *result != "" {
			t.Errorf("expected empty string, got %v", *result)
		}
	})

	t.Run("false bool", func(t *testing.T) {
		result := ToPtr(false)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if *result != false {
			t.Errorf("expected false, got %v", *result)
		}
	})
}

// BenchmarkToPtr benchmarks the ToPtr function
func BenchmarkToPtr(b *testing.B) {
	b.Run("int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ToPtr(42)
		}
	})

	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ToPtr("test")
		}
	})

	b.Run("struct", func(b *testing.B) {
		type testStruct struct {
			Value int
			Name  string
		}
		for i := 0; i < b.N; i++ {
			_ = ToPtr(testStruct{Value: 42, Name: "test"})
		}
	})
}
