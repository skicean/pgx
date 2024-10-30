package stmtcache

import (
	"math"

	"github.com/skicean/pgx/v5/pgconn"
)

// UnlimitedCache implements Cache with no capacity limit.
type UnlimitedCache struct {
	m            map[string]*pgconn.StatementDescription
	invalidStmts []*pgconn.StatementDescription
}

// NewUnlimitedCache creates a new UnlimitedCache.
func NewUnlimitedCache() *UnlimitedCache {
	return &UnlimitedCache{
		m: make(map[string]*pgconn.StatementDescription),
	}
}

// Get returns the statement description for sql. Returns nil if not found.
func (c *UnlimitedCache) Get(sql string) *pgconn.StatementDescription {
	return c.m[sql]
}

// Put stores sd in the cache. Put panics if sd.SQL is "". Put does nothing if sd.SQL already exists in the cache.
func (c *UnlimitedCache) Put(sd *pgconn.StatementDescription) {
	if sd.SQL == "" {
		panic("cannot store statement description with empty SQL")
	}

	if _, present := c.m[sd.SQL]; present {
		return
	}

	c.m[sd.SQL] = sd
}

// Invalidate invalidates statement description identified by sql. Does nothing if not found.
func (c *UnlimitedCache) Invalidate(sql string) {
	if sd, ok := c.m[sql]; ok {
		delete(c.m, sql)
		c.invalidStmts = append(c.invalidStmts, sd)
	}
}

// InvalidateAll invalidates all statement descriptions.
func (c *UnlimitedCache) InvalidateAll() {
	for _, sd := range c.m {
		c.invalidStmts = append(c.invalidStmts, sd)
	}

	c.m = make(map[string]*pgconn.StatementDescription)
}

func (c *UnlimitedCache) HandleInvalidated() []*pgconn.StatementDescription {
	invalidStmts := c.invalidStmts
	c.invalidStmts = nil
	return invalidStmts
}

// Len returns the number of cached prepared statement descriptions.
func (c *UnlimitedCache) Len() int {
	return len(c.m)
}

// Cap returns the maximum number of cached prepared statement descriptions.
func (c *UnlimitedCache) Cap() int {
	return math.MaxInt
}