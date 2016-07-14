package hippy

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
