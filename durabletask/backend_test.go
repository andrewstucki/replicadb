package durabletask

import (
	"testing"

	"github.com/andrewstucki/replicadb"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	db, err := replicadb.Memory()
	require.NoError(t, err)

	durableBackend := NewReplicaDBBackend(db)

	require.NoError(t, durableBackend.CreateTaskHub(t.Context()))
	defer func() {
		require.NoError(t, durableBackend.DeleteTaskHub(t.Context()))
	}()
}
