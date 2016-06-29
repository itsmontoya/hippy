package hippy

import (
	"bufio"
	"os"

	"github.com/missionMeteora/uuid"
)

// action stores the action-type and body for a transaction item
type action struct {
	a byte
	b []byte
}

// storage is the internal store for Hippy
type storage map[string][]byte

// Error is a simple error type which is able to be stored as a const, rather than a global var
type Error string

// Error fulfills the interface requirements for an error
func (e Error) Error() string {
	return string(e)
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
	out = append(out, key...)

	// If action is DELETE, we don't need to append the separator and value, goto the end
	if a == _del {
		goto END
	}

	// Append separator
	out = append(out, _separator)

	// Append body
	out = append(out, b...)

END:
	// Lastly, append a newline before returning
	out = append(out, _newline)
	return
}

func newHashLine() (out []byte) {
	u := uuid.New()

	// Out is the length of a UUID (16), our prefix '# ' (2), and our suffix (Newline, 1)
	out = make([]byte, 19)
	out[0] = _pound
	out[1] = _space
	copy(out[2:], u[:])
	out[18] = _newline
	return
}

// parseLogLine will return an action, key, and body from a provided log line (in the form of a byte slice)
func parseLogLine(b []byte) (a byte, key string, body []byte, err error) {
	if b[0] == _pound && b[1] == _space {
		err = ErrHashLine
		return
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

	var (
		keyB []byte
		i    = 1
	)

	// Iterate through the inbound byteslice
	for ; i < len(b); i++ {
		// We have reached the separator, break out of loop. It's time to get the body!
		if b[i] == _separator {
			i++ // Increment tomove past the _separator
			break
		}

		// Append the byte to the byteslice representation of key
		keyB = append(keyB, b[i])
	}

	// If our action is delete, we do not need to parse any further
	if a == _del {
		return
	}

	// Set key
	key = string(keyB)
	// Pre-allocate body as the length of the inbound byteslice minus the current index
	body = make([]byte, len(b)-i)
	// Copy inbound slice (from the current index to the end) to body
	copy(body, b[i:])
	return
}

func archive(in, out *os.File) (hash []byte, err error) {
	var (
		cu bool // Caught up boolean
		b  []byte

		nl = []byte{_newline}
	)

	in.Seek(0, 0)
	scnr := bufio.NewScanner(in)

	// For each line..
	for err == nil && scnr.Scan() {
		b = scnr.Bytes()
		// Parse action, key, and value
		_, _, _, err = parseLogLine(b)

		switch err {
		case nil:
		case ErrHashLine:
			cu = true
			err = nil
			continue
		default:
			continue
		}

		if !cu {
			continue
		}

		_, err = out.Write(b)
		out.Write(nl)
	}

	if !cu {
		err = ErrHashNotFound
		return
	}

	hash = newHashLine()
	out.Write(hash)
	return
}
