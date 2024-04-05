package strings

import (
	"crypto/rand"
	"encoding/base32"
	"io"
)

var base32Encoder = base32.StdEncoding.WithPadding(base32.NoPadding)

func Rand() string {
	randomBytes := make([]byte, 32)
	for {
		if _, err := io.ReadFull(rand.Reader, randomBytes); err == nil {
			break
		}
	}
	return base32Encoder.EncodeToString(randomBytes)
}
