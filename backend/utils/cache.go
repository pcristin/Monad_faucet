package utils

import (
	"sync"
	"time"
)

// Cache represents a generic time-based cache
type Cache struct {
	data       map[string]cacheItem
	mu         sync.RWMutex
	defaultTTL time.Duration
}

type cacheItem struct {
	value     interface{}
	expiresAt time.Time
}

// NewCache creates a new cache with the specified default TTL
func NewCache(defaultTTL time.Duration) *Cache {
	return &Cache{
		data:       make(map[string]cacheItem),
		defaultTTL: defaultTTL,
	}
}

// Set adds or updates a value in the cache with the default TTL
func (c *Cache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetWithTTL adds or updates a value in the cache with a specific TTL
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = cacheItem{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

// Get retrieves a value from the cache
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.data[key]
	if !exists {
		return nil, false
	}

	// Check if the item has expired
	if time.Now().After(item.expiresAt) {
		return nil, false
	}

	return item.value, true
}

// Delete removes a value from the cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
}

// Clear removes all values from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[string]cacheItem)
}

// Cleanup removes expired items from the cache
func (c *Cache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, item := range c.data {
		if now.After(item.expiresAt) {
			delete(c.data, key)
		}
	}
}
