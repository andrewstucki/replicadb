package replicadb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	uplitestream "github.com/benbjohnson/litestream"
	"github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/ncruces/go-sqlite3/litestream"
)

var (
	// defaultParallelism is the number of parallel connections used for
	// read-only queries and restore operations.
	defaultParallelism = runtime.NumCPU()

	// defaultSyncInterval is the polling interval for replica synchronization.
	defaultSyncInterval = 30 * time.Second

	// defaultCacheSize is the SQLite page cache size, in bytes.
	defaultCacheSize = 1 * (1 << 30) // 1 GB

	// defaultBusyTimeout is the maximum time SQLite waits for a locked database.
	defaultBusyTimeout = 5 * time.Second
)

// DB represents a replicated SQLite database.
//
// It embeds a read-only *sql.DB for concurrent queries and, when initialized
// as a primary, a single-threaded write database for mutations backed by an
// embedded litestream instance. When configured as a replica, it only gets the
// read-only database and periodically syncs from the remote source.
type DB struct {
	// DB is the read-only database handle used for concurrent reads.
	*sql.DB

	// write is the single-connection database handle used for writes.
	write *sql.DB

	maxConn int

	client   uplitestream.ReplicaClient
	upstream *uplitestream.DB
	path     string

	syncInterval       time.Duration
	restoreParallelism int
	cacheSize          int
	busyTimeout        time.Duration
}

// Option configures a DB during construction.
type Option func(db *DB)

// newDB initializes a DB with defaults and applies the provided options.
func newDB(path string, options ...Option) *DB {
	db := &DB{
		path:               path,
		restoreParallelism: defaultParallelism,
		syncInterval:       defaultSyncInterval,
		cacheSize:          defaultCacheSize,
		busyTimeout:        defaultBusyTimeout,
		maxConn:            defaultParallelism,
	}

	for _, option := range options {
		option(db)
	}

	return db
}

// Write returns the write-enabled database connection.
//
// The returned database is single-threaded and must be used for all mutations.
// An error is returned if the database was opened in replica mode.
func (d *DB) Write() (*sql.DB, error) {
	if d.write == nil {
		return nil, errors.New("no write database")
	}
	return d.write, nil
}

// pragmas returns the SQLite pragmas applied to database connections.
//
// The configuration varies depending on whether the database is primary and
// whether the connection is read-only.
func (d *DB) pragmas(primary, read bool) []string {
	pragmas := []string{
		fmt.Sprintf("busy_timeout=%d", d.busyTimeout.Milliseconds()),
		fmt.Sprintf("cache_size=%d", d.cacheSize),
		"foreign_keys=true",
		"synchronous=NORMAL",
		"temp_store=memory",
	}
	if primary && !read {
		pragmas = append(pragmas, "journal_mode=WAL")
	}
	if read {
		pragmas = append(pragmas, "query_only=ON")
	}
	return pragmas
}

// params returns the SQLite connection parameters derived from pragmas
// and the database role (primary or replica).
func (d *DB) params(primary, read bool) string {
	params := []string{}
	for _, pragma := range d.pragmas(primary, read) {
		params = append(params, "_pragma="+pragma)
	}
	if primary {
		params = append(params, "_txlock=immediate")
	} else {
		params = append(params, "vfs=litestream")
	}

	return strings.Join(params, "&")
}

// WithSyncInterval sets the polling interval used by replicas to
// synchronize from the primary.
func WithSyncInterval(interval time.Duration) Option {
	return func(db *DB) {
		db.syncInterval = interval
	}
}

// WithRestoreParallelism sets the number of parallel workers used when
// restoring a primary database from a replica.
func WithRestoreParallelism(parallelism int) Option {
	return func(db *DB) {
		db.restoreParallelism = parallelism
	}
}

// WithCacheSize sets the SQLite page cache size, in bytes.
func WithCacheSize(size int) Option {
	return func(db *DB) {
		db.cacheSize = size
	}
}

// WithBusyTimeout sets the SQLite busy timeout duration.
func WithBusyTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.busyTimeout = timeout
	}
}

// WithMaxConnections sets the maximum number of connections
// on the read-only DB.
func WithMaxConnections(conn int) Option {
	return func(db *DB) {
		db.maxConn = conn
	}
}

// Open opens a primary database at the given path.
//
// It initializes Litestream replication if a replica client is configured,
// attempts a restore if necessary, and returns a DB with separate read-only
// and write connections.
func Open(path string, options ...Option) (*DB, error) {
	db := newDB(path, options...)

	var upstream *uplitestream.DB
	var err error
	if db.client != nil {
		upstream, err = litestream.NewPrimary(context.Background(), path, db.client, nil)
		if err != nil {
			return nil, fmt.Errorf("error initializing primary replica: %v", err)
		}

		upstream.Logger = slog.Default()

		shouldSwallow := func(err error) bool {
			if strings.Contains(err.Error(), "already exists") {
				return true
			}
			if strings.Contains(err.Error(), "transaction not available") {
				return true
			}

			return false
		}

		// attempt a restore
		err = upstream.Replica.Restore(context.Background(), uplitestream.RestoreOptions{
			OutputPath:  path,
			Parallelism: db.restoreParallelism,
		})
		if err != nil && !shouldSwallow(err) {
			return nil, fmt.Errorf("error restoring primary replica: %v", err)
		}
	}

	writeDB, err := driver.Open(fmt.Sprintf("file:%s?%s", path, db.params(true, false)))
	if err != nil {
		if upstream != nil {
			return nil, errors.Join(err, upstream.Close(context.Background()))
		}
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)

	readDB, err := driver.Open(fmt.Sprintf("file:%s?%s", path, db.params(true, true)))
	if err != nil {
		if upstream != nil {
			return nil, errors.Join(err, upstream.Close(context.Background()), writeDB.Close())
		}
		return nil, err
	}
	readDB.SetMaxOpenConns(db.maxConn)

	db.upstream = upstream
	db.write = writeDB
	db.DB = readDB

	return db, nil
}

// OpenReplica opens a read-only replica database at the given path.
//
// The replica periodically synchronizes from the primary using the configured
// replica client. Write access is not supported.
func OpenReplica(path string, options ...Option) (*DB, error) {
	db := newDB(path, options...)

	if db.client == nil {
		return nil, errors.New("must configure a client")
	}

	litestream.NewReplica(path, db.client, litestream.ReplicaOptions{
		PollInterval: db.syncInterval,
	})

	sqlDB, err := driver.Open(fmt.Sprintf("file:%s?%s", path, db.params(false, true)))
	if err != nil {
		litestream.RemoveReplica(path)
		return nil, err
	}
	sqlDB.SetMaxOpenConns(db.maxConn)

	db.DB = sqlDB

	return db, nil
}

// Close closes all database connections and shuts down any active
// Litestream replication.
func (db *DB) Close() error {
	err := db.DB.Close()
	if db.upstream != nil {
		err = errors.Join(err, db.upstream.Close(context.Background()))
	}
	if db.write != nil {
		err = errors.Join(err, db.write.Close())
	}

	litestream.RemoveReplica(db.path)
	return err
}
