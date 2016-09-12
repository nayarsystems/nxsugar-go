package nxsugar

import (
	"crypto/rand"
	"encoding/hex"
)

func inStrSlice(elem string, elems []string) bool {
	for _, el := range elems {
		if el == elem {
			return true
		}
	}
	return false
}

func newTrackId() string {
	b := make([]byte, 8)
	n, err := rand.Read(b)
	if err != nil || n != len(b) {
		return ""
	}
	return hex.EncodeToString(b)
}
