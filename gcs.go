package replicadb

import "github.com/benbjohnson/litestream/gs"

// GCSConfiguration defines the configuration required to use
// Google Cloud Storage (GCS) as a Litestream replication backend.
type GCSConfiguration struct {
	// Bucket is the name of the Google Cloud Storage bucket
	// used to store database replicas.
	Bucket string
	// Path is an optional prefix within the bucket under which
	// database replicas will be stored.
	Path string
}

// WithGCSReplication configures the database to use Google Cloud Storage
// as its Litestream replication backend.
//
// When applied, this option initializes a GCS replica client and
// attaches it to the DB. The client is used when opening the database
// as either a primary or replica via Open or OpenReplica.
func WithGCSReplication(config GCSConfiguration) Option {
	return func(db *DB) {
		client := gs.NewReplicaClient()
		if config.Bucket != "" {
			client.Bucket = config.Bucket
		}
		if config.Path != "" {
			client.Path = config.Path
		}
		db.client = client
	}
}
