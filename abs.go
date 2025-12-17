package replicadb

import "github.com/benbjohnson/litestream/abs"

// ABSConfiguration defines the configuration required to use
// Azure Blob Storage (ABS) as a Litestream replication backend.
type ABSConfiguration struct {
	// AccountName is the Azure storage account name.
	AccountName string
	// AccountKey is the access key for the Azure storage account.
	AccountKey string
	// Endpoint is an optional custom Azure Blob Storage endpoint.
	// This is commonly used for emulators or private cloud deployments.
	Endpoint string
	// Bucket is the name of the Azure Blob Storage container.
	Bucket string
	// Path is the prefix within the container under which
	// database replicas will be stored.
	Path string
}

// WithABSReplication configures the database to use Azure Blob Storage
// as its Litestream replication backend.
//
// When applied, this option initializes an ABS replica client and
// attaches it to the DB. The client is used when opening the database
// as either a primary or replica via Open or OpenReplica.
func WithABSReplication(config ABSConfiguration) Option {
	return func(db *DB) {
		client := abs.NewReplicaClient()
		if config.Path != "" {
			client.Path = config.Path
		}
		if config.Bucket != "" {
			client.Bucket = config.Bucket
		}
		if config.AccountName != "" {
			client.AccountName = config.AccountName
		}
		if config.AccountKey != "" {
			client.AccountKey = config.AccountKey
		}
		if config.Endpoint != "" {
			client.Endpoint = config.Endpoint
		}

		db.client = client
	}
}
