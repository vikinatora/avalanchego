// Copyright (C) 2019-2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Path_Val(t *testing.T) {
	path := NewPath([]byte{240, 237})
	require.Equal(t, byte(15), path[0])
	require.Equal(t, byte(0), path[1])
	require.Equal(t, byte(14), path[2])
	require.Equal(t, byte(13), path[3])
}

func Test_Path_Append(t *testing.T) {
	path := EmptyPath
	require.Len(t, path, 0)

	path = path.Append(1)
	require.Len(t, path, 1)
	require.Equal(t, byte(1), path[0])

	path = path.Append(2)
	require.Len(t, path, 2)
	require.Equal(t, byte(2), path[1])
}

func Test_Path_Has_Prefix(t *testing.T) {
	first := Path([]byte{0, 1, 2, 3})
	prefix := Path([]byte{0, 1, 2})
	require.True(t, first.HasPrefix(prefix))
	require.True(t, first.HasStrictPrefix(prefix))

	first = Path([]byte{0, 1, 2})
	prefix = Path([]byte{0, 1, 2})
	require.True(t, first.HasPrefix(prefix))
	require.False(t, first.HasStrictPrefix(prefix))

	first = Path([]byte{1, 2})
	prefix = Path([]byte{1, 0})
	require.False(t, first.HasPrefix(prefix))
	require.False(t, first.HasStrictPrefix(prefix))

	first = Path([]byte{1, 2})
	prefix = Path([]byte{1})
	require.True(t, first.HasPrefix(prefix))
	require.True(t, first.HasStrictPrefix(prefix))

	first = EmptyPath
	prefix = EmptyPath
	require.True(t, first.HasPrefix(prefix))
	require.False(t, first.HasStrictPrefix(prefix))
}
