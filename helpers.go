package hippy

import (
	"encoding/base64"

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
func newLogLine(key string, a byte, b []byte) (out []byte) {
	// Pre-allocate the slice to the size of the sum:
	//	- Length of key
	//	- Length of body
	//	- Action and newline (two total)
	out = make([]byte, 0, len(key)+len(b)+2)

	// Append action
	out = append(out, byte(a))

	// Append key
	out = append(out, encodeBase64([]byte(key))...)

	// If action is DELETE, we don't need to append the separator and value, goto the end
	if a == _del {
		return
	}

	// Append separator
	out = append(out, _separator)

	// Append body
	out = append(out, encodeBase64(b)...)
	return
}

func newHashLine() (out []byte) {
	// Out is the length of a UUID (16), our prefix '# ' (2)
	out = make([]byte, 18)
	out[0] = _pound
	out[1] = _space
	copy(out[2:], []byte(uuid.New().String()))
	return
}

// parseLogLine will return an action, key, and body from a provided log line (in the form of a byte slice)
func parseLogLine(b []byte) (a byte, key string, body []byte, err error) {
	var (
		keyB []byte
		i    = 1
	)

	if b[0] == _pound && b[1] == _space {
		a = _hash
		i++
		goto END
	}

	// Action is the first index
	a = b[0]

	// Validate action
	switch a {
	case _put, _del:
	default:
		// Invalid action, return ErrInvalidAction
		err = ErrInvalidAction
		return
	}

	// Iterate through the inbound byteslice
	for ; i < len(b); i++ {
		// We have reached the separator, break out of loop. It's time to get the body!
		if b[i] == _separator {
			i++ // Increment to move past the _separator
			break
		}

		// Append the byte to the byteslice representation of key
		keyB = append(keyB, b[i])
	}

	// If our action is delete, we do not need to parse any further
	if a == _del {
		return
	}

END:
	// Set key
	if keyB, err = decodeBase64(keyB); err != nil {
		return
	}
	key = string(keyB)

	// Pre-allocate body as the length of the inbound byteslice minus the current index
	if body, err = decodeBase64(b[i:]); err != nil {
		return
	}

	return
}

func archive(in, out *file, mws []Middleware) (hash []byte, err error) {
	/*
		var (
			cu bool // Caught up boolean
			b  []byte

			scnr *bufio.Scanner
			nl   = []byte{_newline}
		)

		in.SeekToStart()
		scnr = bufio.NewScanner(in)

		// For each line..
		for scnr.Scan() {
			b = scnr.Bytes()
			// Parse action, key, and value
			a, _, _, _ := parseLogLine(b)

			switch a {
			case _put, _del:
			case _hash:
				cu = true
				continue
			default:
				continue
			}

			if !cu {
				continue
			}

			// TODO: Switch this section out with an io.Copy
			if _, err = out.Write(b); err != nil {
				return
			}

			if _, err = out.Write(nl); err != nil {
				return
			}
			// TODO END
		}

		if !cu {
			err = ErrHashNotFound
			return
		}

		hash = newHashLine()
		if _, err = out.Write(hash); err != nil {
			return
		}

		err = out.Flush()
	*/
	return
}

func encodeBase64(in []byte) (out []byte) {
	out = make([]byte, base64.StdEncoding.EncodedLen(len(in)))
	base64.StdEncoding.Encode(out, in)
	return
}

func decodeBase64(in []byte) (out []byte, err error) {
	var n int

	out = make([]byte, base64.StdEncoding.DecodedLen(len(in)))
	if n, err = base64.StdEncoding.Decode(out, in); err != nil {
		return
	}

	out = out[:n]
	return
}
