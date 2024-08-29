package lpstream

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	ErrMessageLengthTooLong  = errors.New("message length too long")
	ErrMessageDataTooLong    = errors.New("message data too long")
	ErrMaxDataLengthExceeded = errors.New("message data exceeds maximum allowed length")
	ErrReadingLengthByte     = errors.New("error reading length byte")
	ErrReadingData           = errors.New("error reading data")
	ErrWritingLength         = errors.New("error writing length")
	ErrWritingData           = errors.New("error writing data")
	ErrSettingReadDeadline   = errors.New("error setting read deadline")
	ErrSettingWriteDeadline  = errors.New("error setting write deadline")
)

type LengthPrefixedStream struct {
	duplex          io.ReadWriter
	maxLengthVarint int
	maxDataVarint   int
	readLengthBuf   []byte
	writeLengthBuf  []byte
	dataBuf         []byte
}

type AbortOptions struct {
	Context context.Context
}

type Option func(*LengthPrefixedStream)

type writeDeadliner interface {
	SetWriteDeadline(time.Time) error
}

type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

func New(duplex io.ReadWriter, opts ...Option) *LengthPrefixedStream {
	stream := &LengthPrefixedStream{
		duplex:         duplex,
		readLengthBuf:  make([]byte, binary.MaxVarintLen64),
		writeLengthBuf: make([]byte, binary.MaxVarintLen64),
	}

	for _, opt := range opts {
		opt(stream)
	}

	if stream.maxDataVarint > 0 && stream.maxLengthVarint == 0 {
		stream.maxLengthVarint = binary.MaxVarintLen64
	}

	return stream
}

func WithMaxLengthVarint(maxLengthVarint int) Option {
	return func(stream *LengthPrefixedStream) {
		stream.maxLengthVarint = maxLengthVarint
	}
}

func WithMaxDataVarint(maxDataVarint int) Option {
	return func(stream *LengthPrefixedStream) {
		stream.maxDataVarint = maxDataVarint
	}
}

func (s *LengthPrefixedStream) Read(options ...AbortOptions) ([]byte, error) {
	var opt AbortOptions
	if len(options) > 0 {
		opt = options[0]
	}

	lengthBuffer := s.readLengthBuf[:1]

	for {
		if err := s.readBuffer(opt.Context, lengthBuffer[len(lengthBuffer)-1:]); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrReadingLengthByte, err)
		}

		dataLength, n := binary.Uvarint(lengthBuffer)
		if n > 0 {
			if s.maxLengthVarint > 0 && len(lengthBuffer) > s.maxLengthVarint {
				return nil, ErrMessageLengthTooLong
			}

			if s.maxDataVarint > 0 && int(dataLength) > s.maxDataVarint {
				return nil, ErrMessageDataTooLong
			}

			if int(dataLength) > cap(s.dataBuf) {
				s.dataBuf = make([]byte, dataLength)
			} else {
				s.dataBuf = s.dataBuf[:dataLength]
			}

			if err := s.readBuffer(opt.Context, s.dataBuf); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrReadingData, err)
			}

			return append([]byte{}, s.dataBuf...), nil
		}

		if len(lengthBuffer) < cap(lengthBuffer) {
			lengthBuffer = lengthBuffer[:len(lengthBuffer)+1]
		} else {
			lengthBuffer = append(lengthBuffer, 0)
		}
	}
}

func (s *LengthPrefixedStream) Write(data []byte, options ...AbortOptions) error {
	return s.write(data, options...)
}

func (s *LengthPrefixedStream) WriteV(data [][]byte, options ...AbortOptions) error {
	for _, d := range data {
		if err := s.write(d, options...); err != nil {
			return err
		}
	}

	return nil
}

func (s *LengthPrefixedStream) write(data []byte, options ...AbortOptions) error {
	if s.maxDataVarint > 0 && len(data) > s.maxDataVarint {
		return ErrMaxDataLengthExceeded
	}

	lengthBuffer := s.writeLengthBuf[:binary.PutUvarint(s.writeLengthBuf, uint64(len(data)))]

	if s.maxLengthVarint > 0 && len(lengthBuffer) > s.maxLengthVarint {
		return ErrMessageLengthTooLong
	}

	var opt AbortOptions
	if len(options) > 0 {
		opt = options[0]
	}

	if err := s.writeBuffer(opt.Context, lengthBuffer); err != nil {
		return fmt.Errorf("%w: %v", ErrWritingLength, err)
	}

	if err := s.writeBuffer(opt.Context, data); err != nil {
		return fmt.Errorf("%w: %v", ErrWritingData, err)
	}

	return nil
}

func (s *LengthPrefixedStream) readBuffer(ctx context.Context, buffer []byte) error {
	if ctx == nil {
		_, err := s.duplex.Read(buffer)
		return err
	}

	deadline, hasDeadline := ctx.Deadline()

	if hasDeadline {
		if rd, ok := s.duplex.(readDeadliner); ok {
			if err := rd.SetReadDeadline(deadline); err != nil {
				return fmt.Errorf("%w: %v", ErrSettingReadDeadline, err)
			}

			defer func() {
				_ = rd.SetReadDeadline(time.Time{}) //nolint:errcheck // not much can be done at this point
			}()
		}
	}

	_, err := s.duplex.Read(buffer)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return err
}

func (s *LengthPrefixedStream) writeBuffer(ctx context.Context, data []byte) error {
	if ctx == nil {
		_, err := s.duplex.Write(data)
		return err
	}

	deadline, hasDeadline := ctx.Deadline()

	if hasDeadline {
		if wd, ok := s.duplex.(writeDeadliner); ok {
			if err := wd.SetWriteDeadline(deadline); err != nil {
				return fmt.Errorf("%w: %v", ErrSettingWriteDeadline, err)
			}

			defer func() {
				_ = wd.SetWriteDeadline(time.Time{}) //nolint:errcheck // not much can be done at this point
			}()
		}
	}

	_, err := s.duplex.Write(data)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return err
}

func (s *LengthPrefixedStream) Unwrap() io.ReadWriter {
	return s.duplex
}
