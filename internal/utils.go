package internal

import (
	"crypto/sha1"
)

func Str_uint32_sha1(val string) uint32 {
	sha := sha1.Sum([]byte(val))
	var hsh uint32
	for i := 0; i < 4; i++ {
		hsh <<= 8
		hsh += uint32(sha[i])
	}
	return hsh
}

type ValueEntry struct {
	Key   string
	Value string
}
