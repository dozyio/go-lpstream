package lpstream_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dozyio/go-lpstream"
)

// MockReadWriter is a mock implementation of io.ReadWriter that supports SetReadDeadline
type MockReadWriter struct {
	ReadError     error
	WriteError    error
	ReadDeadline  error
	WriteDeadline error
	BytesBuffer   *bytes.Buffer
}

func (m *MockReadWriter) Read(p []byte) (n int, err error) {
	return m.BytesBuffer.Read(p)
}

func (m *MockReadWriter) Write(p []byte) (n int, err error) {
	return m.BytesBuffer.Write(p)
}

func (m *MockReadWriter) SetReadDeadline(t time.Time) error {
	return m.ReadDeadline
}

func (m *MockReadWriter) SetWriteDeadline(t time.Time) error {
	return m.WriteDeadline
}

func TestLengthPrefixedStream_SetReadDeadlineError(t *testing.T) {
	mock := &MockReadWriter{
		ReadDeadline: errors.New("mock read deadline error"),
		BytesBuffer:  new(bytes.Buffer),
	}

	stream := lpstream.New(mock)

	// Write some data to the buffer
	data := []byte{0, 1, 2, 3, 4}
	if err := stream.Write(data); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := stream.Read(lpstream.AbortOptions{Context: ctx})
	expectedError := fmt.Sprintf("%s: %v", lpstream.ErrReadingLengthByte, fmt.Errorf("%s: %v", lpstream.ErrSettingReadDeadline, mock.ReadDeadline))

	if err == nil {
		t.Fatalf("Expected error %v, got nil", expectedError)
	}

	if err.Error() != expectedError {
		t.Fatalf("Expected error %v, got %v", expectedError, err)
	}
}

func TestLengthPrefixedStream_SetWriteDeadlineError(t *testing.T) {
	mock := &MockReadWriter{
		WriteDeadline: errors.New("mock write deadline error"),
		BytesBuffer:   new(bytes.Buffer),
	}

	stream := lpstream.New(mock)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	data := []byte{0, 1, 2, 3, 4}
	err := stream.Write(data, lpstream.AbortOptions{Context: ctx})
	expectedError := fmt.Sprintf("%s: %v", lpstream.ErrWritingLength, fmt.Errorf("%s: %v", lpstream.ErrSettingWriteDeadline, mock.WriteDeadline))

	if err == nil {
		t.Fatalf("Expected error %v, got nil", expectedError)
	}

	if err.Error() != expectedError {
		t.Fatalf("Expected error %v, got %v", expectedError, err)
	}
}

func TestLengthPrefixedStream_MultipleReadWrite(t *testing.T) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	data1 := []byte{0, 1, 2, 3, 4}
	data2 := []byte{5, 6, 7, 8, 9}
	data3 := []byte{10, 11, 12, 13, 14}

	// Write data1
	if err := stream.Write(data1); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Write data2
	if err := stream.Write(data2); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Read and check data1
	readData, err := stream.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	if !bytes.Equal(readData, data1) {
		t.Fatalf("Expected %v, got %v", data1, readData)
	}

	// Write data3
	if err := stream.Write(data3); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Read and check data2
	readData, err = stream.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	if !bytes.Equal(readData, data2) {
		t.Fatalf("Expected %v, got %v", data2, readData)
	}

	// Read and check data3
	readData, err = stream.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	if !bytes.Equal(readData, data3) {
		t.Fatalf("Expected %v, got %v", data3, readData)
	}
}

func TestLengthPrefixedStream(t *testing.T) {
	tests := []struct {
		name            string
		maxLengthVarint int
		maxDataVarint   int
		data            []byte
	}{
		{
			name:            "Normal Write and Read",
			maxLengthVarint: 0,
			maxDataVarint:   0,
			data:            []byte{0, 1, 2, 3, 4},
		},
		{
			name:            "Write and Read with Max Length Varint",
			maxLengthVarint: binary.MaxVarintLen64,
			maxDataVarint:   0,
			data:            []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name:            "Write and Read with Max Data Varint",
			maxLengthVarint: 0,
			maxDataVarint:   10,
			data:            []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:            "Write and Read with Max Length Varint and Max Data Varint",
			maxLengthVarint: binary.MaxVarintLen64,
			maxDataVarint:   100,
			data:            make([]byte, 100), // Data length is exactly 100 bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := new(bytes.Buffer)
			stream := lpstream.New(
				buffer,
				lpstream.WithMaxLengthVarint(tt.maxLengthVarint),
				lpstream.WithMaxDataVarint(tt.maxDataVarint),
			)

			// Write data
			if err := stream.Write(tt.data); err != nil {
				t.Fatalf("Write error: %v", err)
			}

			// Read data
			readData, err := stream.Read()
			if err != nil {
				t.Fatalf("Read error: %v", err)
			}

			if !bytes.Equal(readData, tt.data) {
				t.Fatalf("Expected %v, got %v", tt.data, readData)
			}
		})
	}
}

func TestLengthPrefixedStream_Errors(t *testing.T) {
	tests := []struct {
		name            string
		maxLengthVarint int
		maxDataVarint   int
		data            []byte
		expectedError   error
		setupBuffer     func(*bytes.Buffer)
	}{
		{
			name:            "ErrMessageLengthTooLong on Write",
			maxLengthVarint: 1,
			maxDataVarint:   0,
			data:            make([]byte, 128), // Varint length of 128 is 2 bytes
			expectedError:   lpstream.ErrMessageLengthTooLong,
			setupBuffer:     nil,
		},
		{
			name:            "ErrMessageDataTooLong on Write",
			maxLengthVarint: 0,
			maxDataVarint:   2,
			data:            []byte{0, 1, 2},
			expectedError:   lpstream.ErrMaxDataLengthExceeded,
			setupBuffer:     nil,
		},
		{
			name:            "ErrMessageLengthTooLong on Read",
			maxLengthVarint: 1,
			maxDataVarint:   0,
			data:            nil,
			expectedError:   lpstream.ErrMessageLengthTooLong,
			setupBuffer: func(buffer *bytes.Buffer) {
				varint := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(varint, uint64(128)) // Varint length of 128 is 2 bytes
				buffer.Write(varint[:n])
			},
		},
		{
			name:            "ErrMessageDataTooLong on Read",
			maxLengthVarint: 0,
			maxDataVarint:   2,
			data:            nil,
			expectedError:   lpstream.ErrMessageDataTooLong,
			setupBuffer: func(buffer *bytes.Buffer) {
				varint := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(varint, uint64(3)) // Length of 3 bytes
				buffer.Write(varint[:n])
				buffer.Write([]byte{0, 1, 2}) // Actual data
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := new(bytes.Buffer)
			stream := lpstream.New(
				buffer,
				lpstream.WithMaxLengthVarint(tt.maxLengthVarint),
				lpstream.WithMaxDataVarint(tt.maxDataVarint),
			)

			if tt.setupBuffer != nil {
				tt.setupBuffer(buffer)
			}

			var err error
			if tt.data != nil {
				err = stream.Write(tt.data)
			} else {
				_, err = stream.Read()
			}

			if err == nil {
				t.Fatalf("Expected error %v, got nil", tt.expectedError)
			}

			if !errors.Is(err, tt.expectedError) {
				t.Fatalf("Expected error %v, got %v", tt.expectedError, err)
			}
		})
	}
}

func TestLengthPrefixedStream_Context(t *testing.T) {
	tests := []struct {
		name           string
		data           []byte
		contextTimeout time.Duration
		expectedError  string
	}{
		{
			name:           "Write with Context Timeout",
			data:           []byte{0, 1, 2, 3, 4},
			contextTimeout: 1 * time.Nanosecond, // Immediate timeout
			expectedError:  fmt.Sprintf("%s: %v", lpstream.ErrWritingLength, context.DeadlineExceeded),
		},
		{
			name:           "Read with Context Timeout",
			data:           nil,
			contextTimeout: 1 * time.Nanosecond, // Immediate timeout
			expectedError:  fmt.Sprintf("%s: %v", lpstream.ErrReadingLengthByte, context.DeadlineExceeded),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := new(bytes.Buffer)
			stream := lpstream.New(buffer)

			ctx, cancel := context.WithTimeout(context.Background(), tt.contextTimeout)
			defer cancel()

			var err error
			if tt.data != nil {
				err = stream.Write(tt.data, lpstream.AbortOptions{Context: ctx})
			} else {
				_, err = stream.Read(lpstream.AbortOptions{Context: ctx})
			}

			if err == nil {
				t.Fatalf("Expected error %v, got nil", tt.expectedError)
			}

			if err.Error() != tt.expectedError {
				t.Fatalf("Expected error %v, got %v", tt.expectedError, err)
			}
		})
	}
}

func TestLengthPrefixedStream_WriteV(t *testing.T) {
	tests := []struct {
		name            string
		maxLengthVarint int
		maxDataVarint   int
		data            [][]byte
		expectedError   error
	}{
		{
			name:            "Normal WriteV",
			maxLengthVarint: 0,
			maxDataVarint:   0,
			data:            [][]byte{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}},
			expectedError:   nil,
		},
		{
			name:            "WriteV with Max Length Varint",
			maxLengthVarint: binary.MaxVarintLen64,
			maxDataVarint:   0,
			data:            [][]byte{{0, 1, 2}, {3, 4, 5, 6, 7, 8, 9, 10}},
			expectedError:   nil,
		},
		{
			name:            "WriteV with Max Data Varint",
			maxLengthVarint: 0,
			maxDataVarint:   10,
			data:            [][]byte{{0, 1, 2}, {3, 4, 5}, {6, 7, 8, 9}},
			expectedError:   nil,
		},
		{
			name:            "ErrMessageDataTooLong on WriteV",
			maxLengthVarint: 0,
			maxDataVarint:   2,
			data:            [][]byte{{0, 1, 2}},
			expectedError:   lpstream.ErrMaxDataLengthExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := new(bytes.Buffer)
			stream := lpstream.New(
				buffer,
				lpstream.WithMaxLengthVarint(tt.maxLengthVarint),
				lpstream.WithMaxDataVarint(tt.maxDataVarint),
			)

			err := stream.WriteV(tt.data)
			if tt.expectedError != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tt.expectedError)
				}

				if !errors.Is(err, tt.expectedError) {
					t.Fatalf("Expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// Read and verify each data slice
				for _, expectedData := range tt.data {
					readData, err := stream.Read()
					if err != nil {
						t.Fatalf("Read error: %v", err)
					}

					if !bytes.Equal(readData, expectedData) {
						t.Fatalf("Expected %v, got %v", expectedData, readData)
					}
				}
			}
		})
	}
}

func TestLengthPrefixedStream_Unwrap(t *testing.T) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	unwrapped := stream.Unwrap()
	if unwrapped != buffer {
		t.Fatalf("Expected %v, got %v", buffer, unwrapped)
	}
}

// BENCHMARKS

func BenchmarkLengthPrefixedStream_Write(b *testing.B) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	// Test data
	data := []byte{0, 1, 2, 3, 4}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.Reset()

		if err := stream.Write(data); err != nil {
			b.Fatalf("Write error: %v", err)
		}
	}
}

func BenchmarkLengthPrefixedStream_Read(b *testing.B) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	// Test data
	data := []byte{0, 1, 2, 3, 4}
	if err := stream.Write(data); err != nil {
		b.Fatalf("Write error: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.Reset()

		if err := stream.Write(data); err != nil {
			b.Fatalf("Write error: %v", err)
		}

		if _, err := stream.Read(); err != nil {
			b.Fatalf("Read error: %v", err)
		}
	}
}

func BenchmarkLengthPrefixedStream_WriteWithContext(b *testing.B) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	// Test data
	data := []byte{0, 1, 2, 3, 4}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.Reset()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		if err := stream.Write(data, lpstream.AbortOptions{Context: ctx}); err != nil {
			b.Fatalf("Write error with context: %v", err)
		}

		cancel()
	}
}

func BenchmarkLengthPrefixedStream_ReadWithContext(b *testing.B) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	// Test data
	data := []byte{0, 1, 2, 3, 4}
	if err := stream.Write(data); err != nil {
		b.Fatalf("Write error: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.Reset()

		if err := stream.Write(data); err != nil {
			b.Fatalf("Write error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		if _, err := stream.Read(lpstream.AbortOptions{Context: ctx}); err != nil {
			b.Fatalf("Read error with context: %v", err)
		}

		cancel()
	}
}

func BenchmarkLengthPrefixedStream_WriteV(b *testing.B) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	// Test data
	data := [][]byte{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.Reset()

		if err := stream.WriteV(data); err != nil {
			b.Fatalf("WriteV error: %v", err)
		}
	}
}

func BenchmarkLengthPrefixedStream_MultipleReadWrite(b *testing.B) {
	buffer := new(bytes.Buffer)
	stream := lpstream.New(buffer)

	data1 := []byte{0, 1, 2, 3, 4}
	data2 := []byte{5, 6, 7, 8, 9}
	data3 := []byte{10, 11, 12, 13, 14}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.Reset()

		// Write data1
		if err := stream.Write(data1); err != nil {
			b.Fatalf("Write error: %v", err)
		}

		// Write data2
		if err := stream.Write(data2); err != nil {
			b.Fatalf("Write error: %v", err)
		}

		// Read and check data1
		readData, err := stream.Read()
		if err != nil {
			b.Fatalf("Read error: %v", err)
		}

		if !bytes.Equal(readData, data1) {
			b.Fatalf("Expected %v, got %v", data1, readData)
		}

		// Write data3
		if err := stream.Write(data3); err != nil {
			b.Fatalf("Write error: %v", err)
		}

		// Read and check data2
		readData, err = stream.Read()
		if err != nil {
			b.Fatalf("Read error: %v", err)
		}

		if !bytes.Equal(readData, data2) {
			b.Fatalf("Expected %v, got %v", data2, readData)
		}

		// Read and check data3
		readData, err = stream.Read()
		if err != nil {
			b.Fatalf("Read error: %v", err)
		}

		if !bytes.Equal(readData, data3) {
			b.Fatalf("Expected %v, got %v", data3, readData)
		}
	}
}

// FUZZING

func FuzzLengthPrefixedStream_WriteRead(f *testing.F) {
	// Seed the fuzzer with initial data
	f.Add([]byte{0, 1, 2, 3, 4})

	f.Fuzz(func(t *testing.T, data []byte) {
		buffer := new(bytes.Buffer)
		stream := lpstream.New(buffer)

		// Write the fuzzed data
		if err := stream.Write(data); err != nil {
			t.Fatalf("Write error: %v", err)
		}

		// Read the data back
		readData, err := stream.Read()
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}

		// Check if the read data matches the written data
		if !bytes.Equal(readData, data) {
			t.Fatalf("Expected %v, got %v", data, readData)
		}
	})
}

func FuzzLengthPrefixedStream_WriteVRead(f *testing.F) {
	// Seed the fuzzer with initial data
	f.Add([]byte{0, 1, 2}, []byte{3, 4, 5}, []byte{6, 7, 8})

	f.Fuzz(func(t *testing.T, data1, data2, data3 []byte) {
		buffer := new(bytes.Buffer)
		stream := lpstream.New(buffer)

		data := [][]byte{data1, data2, data3}

		// Write the fuzzed data slices
		if err := stream.WriteV(data); err != nil {
			t.Fatalf("WriteV error: %v", err)
		}

		// Read the data back slice by slice
		for _, expectedData := range data {
			readData, err := stream.Read()
			if err != nil {
				t.Fatalf("Read error: %v", err)
			}

			// Check if the read data matches the written data slice
			if !bytes.Equal(readData, expectedData) {
				t.Fatalf("Expected %v, got %v", expectedData, readData)
			}
		}
	})
}

func FuzzLengthPrefixedStream_ContextWriteRead(f *testing.F) {
	// Seed the fuzzer with initial data
	f.Add([]byte{0, 1, 2, 3, 4})

	f.Fuzz(func(t *testing.T, data []byte) {
		buffer := new(bytes.Buffer)
		stream := lpstream.New(buffer)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Write the fuzzed data with context
		if err := stream.Write(data, lpstream.AbortOptions{Context: ctx}); err != nil {
			t.Fatalf("Write error: %v", err)
		}

		// Read the data back with context
		readData, err := stream.Read(lpstream.AbortOptions{Context: ctx})
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}

		// Check if the read data matches the written data
		if !bytes.Equal(readData, data) {
			t.Fatalf("Expected %v, got %v", data, readData)
		}
	})
}

func FuzzLengthPrefixedStream_ContextWriteVRead(f *testing.F) {
	// Seed the fuzzer with initial data
	f.Add([]byte{0, 1, 2}, []byte{3, 4, 5}, []byte{6, 7, 8})

	f.Fuzz(func(t *testing.T, data1, data2, data3 []byte) {
		buffer := new(bytes.Buffer)
		stream := lpstream.New(buffer)

		data := [][]byte{data1, data2, data3}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Write the fuzzed data slices with context
		if err := stream.WriteV(data, lpstream.AbortOptions{Context: ctx}); err != nil {
			t.Fatalf("WriteV error: %v", err)
		}

		// Read the data back slice by slice with context
		for _, expectedData := range data {
			readData, err := stream.Read(lpstream.AbortOptions{Context: ctx})
			if err != nil {
				t.Fatalf("Read error: %v", err)
			}

			// Check if the read data matches the written data slice
			if !bytes.Equal(readData, expectedData) {
				t.Fatalf("Expected %v, got %v", expectedData, readData)
			}
		}
	})
}
