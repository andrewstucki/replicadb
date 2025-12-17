package replicadb

import "github.com/benbjohnson/litestream/s3"

// S3Configuration defines the configuration required to use
// Amazon S3–compatible object storage as a Litestream replication backend.
//
// This configuration supports AWS S3 as well as S3-compatible providers
// such as MinIO, DigitalOcean Spaces, and others.
type S3Configuration struct {
	// AccessKeyID is the AWS access key ID used for authentication.
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key used for authentication.
	SecretAccessKey string
	// Region is the AWS region where the S3 bucket resides.
	Region string
	// Bucket is the name of the S3 bucket used to store database replicas.
	Bucket string
	// Path is the prefix within the bucket under which
	// database replicas will be stored.
	Path string
	// Endpoint is an optional custom S3 endpoint.
	// This is commonly used for S3-compatible storage providers.
	Endpoint string
	// ForcePathStyle forces path-style addressing instead of virtual-hosted style.
	ForcePathStyle bool
	// SkipVerify disables TLS certificate verification.
	// This should only be used in development or testing environments.
	SkipVerify bool
	// SignPayload enables request payload signing.
	SignPayload bool
	// RequireContentMD5 enforces Content-MD5 validation on uploads.
	RequireContentMD5 bool
	// PartSize is the size of each part for multipart uploads.
	// If zero, the Litestream default (5MB) is used.
	PartSize int64 // Part size for multipart uploads (default: 5MB)
	// Concurrency is the number of concurrent parts uploaded during
	// multipart uploads. If zero, the Litestream default (5) is used.
	Concurrency int // Number of concurrent parts to upload (default: 5)
}

// WithS3Replication configures the database to use Amazon S3–compatible
// object storage as its Litestream replication backend.
//
// When applied, this option initializes an S3 replica client and attaches
// it to the DB. The client is used when opening the database as either a
// primary or replica via Open or OpenReplica.
func WithS3Replication(config S3Configuration) Option {
	return func(db *DB) {
		client := s3.NewReplicaClient()
		if config.Path != "" {
			client.Path = config.Path
		}
		if config.AccessKeyID != "" {
			client.AccessKeyID = config.AccessKeyID
		}
		if config.SecretAccessKey != "" {
			client.SecretAccessKey = config.SecretAccessKey
		}
		if config.Region != "" {
			client.Region = config.Region
		}
		if config.Bucket != "" {
			client.Bucket = config.Bucket
		}
		if config.Endpoint != "" {
			client.Endpoint = config.Endpoint
		}
		if config.PartSize != 0 {
			client.PartSize = config.PartSize
		}
		if config.Concurrency != 0 {
			client.Concurrency = config.Concurrency
		}

		client.ForcePathStyle = config.ForcePathStyle
		client.SkipVerify = config.SkipVerify
		client.SignPayload = config.SignPayload
		client.RequireContentMD5 = config.RequireContentMD5

		db.client = client
	}
}
