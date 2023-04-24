// Copyright (C) 2019-2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"reflect"
	"strings"
	"unsafe"
)

const EmptyPath Path = ""

type Path string

// Compare returns:
// * 0 if [p] == [other].
// * -1 if [p] < [other].
// * 1 if [p] > [other].
func (p Path) Compare(other Path) int {
	return strings.Compare(string(p), string(other))
}

// Invariant: The returned value must not be modified.
func (p Path) bytes() []byte {
	// avoid copying during the conversion
	// "safe" because we never edit the value, only used as DB key
	buf := *(*[]byte)(unsafe.Pointer(&p))
	(*reflect.SliceHeader)(unsafe.Pointer(&buf)).Cap = len(p)
	return buf
}

// HasPrefix returns true iff [p] begins with [prefix].
func (p Path) HasPrefix(prefix Path) bool {
	return strings.HasPrefix(string(p), string(prefix))
}

// HasStrictPrefix returns true iff [prefix] is a prefix of [s] but not equal to it.
func (p Path) HasStrictPrefix(prefix Path) bool {
	return p.HasPrefix(prefix) && p != prefix
}

// Append [val] to [p].
func (p Path) Append(val byte) Path {
	return p + Path(val)
}

// AsKey returns the byte representation of the Path.
// Since paths may be of a length that equate to a whole number of bytes, the result is ambiguous.
// Ex: Path{1}.AsKey() and Path{1,0}.AsKey() result in []byte{0x10}
func (p Path) AsKey() []byte {
	// need half the number of bytes as nibbles
	// add one so there is a byte for the odd nibble if it exists
	// the extra nibble gets rounded down if even length
	byteLength := (len(p) + 1) / 2

	nibbleLength := len(p)
	value := make([]byte, byteLength)

	// loop over the path's bytes
	// if the length is odd, subtract 1, so we don't overflow on the p[pathIndex+1]
	keyIndex := 0
	lastIndex := len(p) - len(p)&1
	for pathIndex := 0; pathIndex < lastIndex; pathIndex += 2 {
		value[keyIndex] = p[pathIndex]<<4 + p[pathIndex+1]
		keyIndex++
	}

	// if there is an odd number of nibbles, grab the last nibble
	if nibbleLength&1 == 1 {
		value[keyIndex] = p[keyIndex<<1] << 4
	}

	return value
}

func NewPath(p []byte) Path {
	// create new buffer with double the length of the input since each byte gets split into two nibbles
	buffer := make([]byte, 2*len(p))

	// first nibble gets shifted right 4 (divided by 16) to isolate the first nibble
	// second nibble gets bitwise anded with 0x0F (1111) to isolate the second nibble
	bufferIndex := 0
	for _, currentByte := range p {
		buffer[bufferIndex] = currentByte >> 4
		buffer[bufferIndex+1] = currentByte & 0x0F
		bufferIndex += 2
	}

	// avoid copying during the conversion
	return *(*Path)(unsafe.Pointer(&buffer))
}
