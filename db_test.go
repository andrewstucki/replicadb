package replicadb_test

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/andrewstucki/replicadb"
	minioclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestPrimaryAndReplica(t *testing.T) {
	container, err := minio.Run(t.Context(), "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	require.NoError(t, err)
	t.Cleanup(func() { container.Terminate(context.Background()) })

	port, err := container.MappedPort(t.Context(), "9000/tcp")
	require.NoError(t, err)

	endpoint := fmt.Sprintf("http://localhost:%d", port.Int())
	accessKey := container.Username
	secretKey := container.Password

	client, err := minioclient.New(strings.TrimPrefix(endpoint, "http://"), &minioclient.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)
	require.NoError(t, client.MakeBucket(t.Context(), "test", minioclient.MakeBucketOptions{}))

	config := replicadb.S3Configuration{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		Region:          "us-east-1",
		Bucket:          "test",
		Path:            "backup.db",
		Endpoint:        endpoint,
		ForcePathStyle:  true,
	}

	directory := t.TempDir()
	primaryPath := path.Join(directory, "primary.db")
	replicaPath := path.Join(directory, "replica.db")
	restorePath := path.Join(directory, "restore.db")

	primary, err := replicadb.Open(primaryPath, replicadb.WithS3Replication(config))
	require.NoError(t, err)
	writer, err := primary.Write()
	require.NoError(t, err)

	replica, err := replicadb.OpenReplica(replicaPath, replicadb.WithS3Replication(config), replicadb.WithSyncInterval(100*time.Millisecond))
	require.NoError(t, err)

	createTable := `CREATE TABLE IF NOT EXISTS test (id INTEGER AUTO_INCREMENT PRIMARY KEY)`
	insertTable := `INSERT INTO test (id) VALUES (NULL)`
	selectTable := `SELECT COUNT(*) FROM test;`

	_, err = writer.Exec(createTable)
	require.NoError(t, err)
	_, err = writer.Exec(insertTable)
	require.NoError(t, err)

	hasData := func(db *replicadb.DB, name string) {
		require.Eventually(t, func() bool {
			row := db.QueryRow(selectTable)
			err := row.Err()
			if err != nil {
				t.Logf("error querying %s: %v", name, err)
				return false
			}

			var count int
			err = row.Scan(&count)
			if err != nil {
				t.Logf("error scanning row: %v", err)
				return false
			}

			return count == 1
		}, 10*time.Second, 1*time.Second, fmt.Sprintf("data never synced to %s", name))
	}

	require.Eventually(t, func() bool {
		hasResults := false
		for range client.ListObjects(t.Context(), "test", minioclient.ListObjectsOptions{}) {
			hasResults = true
		}
		return hasResults
	}, 10*time.Second, 1*time.Second, "data never replicated to bucket")

	hasData(primary, "primary")
	primary.Close()

	restore, err := replicadb.Open(restorePath, replicadb.WithS3Replication(config))
	require.NoError(t, err)
	hasData(restore, "restore")
	hasData(replica, "replica")
}
