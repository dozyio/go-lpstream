# Go LPStream

A length prefixed varint stream ReadWriter for Go.

## Example length prefixed network.Stream handler

```go
func handler(s network.Stream) {
    lps := lpstream.New(s)

    buf, err := lps.Read()
    if err != nil {
        fmt.Printf("Error reading from stream: %v\n", err)
        s.Reset()
        return
    }

    // echo the received buf
    err = lps.Write(buf)
    if err != nil {
        fmt.Printf("Error writing to stream: %v\n", err)
        s.Reset()
        return
    }

    s.Close()
}
```

## Example with context

```go
func handler(s networkStream) {
    lps := lpstream.New(s)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    buf, err := lps.Read(lpstream.AbortOptions{Context: ctx})
    if err != nil {
        fmt.Printf("Error reading from stream: %v\n", err)
        s.Reset()
        return
    }

    // echo the received buf
    err = lps.Write(buf, lpstream.AbortOptions{Context: ctx})
    if err != nil {
        fmt.Printf("Error writing to stream: %v\n", err)
        s.Reset()
        return
    }

    s.Close()
}
```

See tests for examples of writing multiple buffers with WriteV and setting the max length.

## Tests

### Unit tests
```sh
go test -v ./...
```

### Benchmark Tests
```sh
go test -bench=^Benchmark -benchmem ./...
```

### Fuzz Tests
```sh
./fuzz.sh
```
