package hippy

import (
	"bytes"

	"github.com/missionMeteora/uuid"
)

// action stores the action-type and body for a transaction item
type action struct {
	a byte
	b []byte
}

type storage map[string][]byte

// Error is a simple error type which is able to be stored as a const, rather than a global var
type Error string

// Error fulfills the interface requirements for an error
func (e Error) Error() string {
	return string(e)
}

// ErrorList is a list of errors
type ErrorList []error

// Error is the error interface implementation
func (e ErrorList) Error() string {
	if len(e) == 0 {
		return ""
	}

	b := []byte("the following errors occured:\n")
	for _, err := range e {
		b = append(b, err.Error()...)
		b = append(b, '\n')
	}

	return string(b)
}

// Append appends an error to the error list
func (e ErrorList) Append(err error) ErrorList {
	if err == nil {
		return e
	}

	if oe, ok := err.(ErrorList); ok {
		return append(e, oe...)
	}
	return append(e, err)
}

// Push adds the error to the list if it is not nil
func (e *ErrorList) Push(err error) {
	if e == nil || err == nil {
		return
	}
	switch err := err.(type) {
	case ErrorList:
		*e = append(*e, err...)
	case *ErrorList:
		*e = append(*e, *err...)
	default:
		*e = append(*e, err)
	}
}

// Err returns error value of ErrorList
func (e ErrorList) Err() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

// newLogLine will return a new log line given a provided key, action, and body
func newLogLine(a byte, key string, b []byte) (out *bytes.Buffer) {
	// Get buffer from the buffer pool
	out = bp.Get()
	// Write action
	out.WriteByte(byte(a))
	// Write key length
	out.WriteByte(uint8(len(key)))
	// Write key
	out.WriteString(key)

	writeMWBytes()

	// If the action is not PUT, return
	if a != _put {
		return
	}

	// Write body
	out.Write(b)
	return
}

func newHashLine() (out []byte) {
	// Out is the length of a UUID (16), our prefix '# ' (2)
	out = make([]byte, 18)
	out[0] = _hash
	out[1] = hashLen
	copy(out[2:], []byte(uuid.New().String()))
	return
}

// parseLogLine will return an action, key, and body from a provided log line (in the form of a byte slice)
func parseLogLine(a byte, b []byte) (key string, body []byte, err error) {
	// Validate action
	switch a {
	case _put, _del, _hash:
	default:
		// Invalid action, return ErrInvalidAction
		err = ErrInvalidAction
		return
	}

	var i uint8
	keyLen := uint8(b[i])
	i++

	key = string(b[i : i+keyLen])
	i += keyLen

	// If our action is not PUT, we do not need to parse any further
	if a != _put {
		return
	}

	body = b[i:]
	return
}

func reverseByteSlice(bs []byte) {
	var n int
	mc := len(bs) - 1
	for i := range bs {
		if n = mc - i; n == i || n < i {
			break
		}

		bs[i], bs[n] = bs[n], bs[i]
	}
}
