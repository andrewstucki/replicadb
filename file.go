package replicadb

import "github.com/benbjohnson/litestream/file"

// WithFileReplication configures the database to use the local filesystem
// as its Litestream replication backend.
//
// The provided path specifies the directory where replica data will be
// stored. This option is useful for local development, testing, or
// single-node deployments where remote object storage is not required.
func WithFileReplication(path string) Option {
	return func(db *DB) {
		client := file.NewReplicaClient(path)
		db.client = client
	}
}
