package utils  

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"time"

	"your_project/config" // Replace with your actual package path for Redis client
)

// CacheUtils provides utility functions for caching API responses and blockchain data
type CacheUtils struct {
	RedisClient *config.RedisClient
	DefaultTTL  time.Duration
}

// CacheKeyPrefix defines prefixes for different types of cached data
const (
	APIPrefix         = "api:"
	BlockchainPrefix  = "blockchain:"
	TransactionPrefix = "tx:"
	AccountPrefix     = "account:"
)

// NewCacheUtils initializes a new CacheUtils instance with a Redis client
func NewCacheUtils(redisClient *config.RedisClient, defaultTTL time.Duration) *CacheUtils {
	return &CacheUtils{
		RedisClient: redisClient,
		DefaultTTL:  defaultTTL,
	}
}

// generateCacheKey creates a unique cache key based on prefix and input parameters
func generateCacheKey(prefix string, params ...string) string {
	// Join parameters with a separator to avoid collisions
	joined := strings.Join(params, ":")
	// Create a hash of the joined string for shorter keys
	h := fnv.New64a()
	h.Write([]byte(joined))
	hash := h.Sum64()
	return fmt.Sprintf("%s%d:%s", prefix, hash, joined)
}

// CacheAPIResponse caches an API response with a specified TTL
func (cu *CacheUtils) CacheAPIResponse(endpoint string, queryParams map[string]string, response interface{}, ttl time.Duration) error {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	// Convert query parameters to a sorted string to ensure consistent keys
	var paramStrs []string
	for k, v := range queryParams {
		paramStrs = append(paramStrs, fmt.Sprintf("%s=%s", k, v))
	}
	keyInput := append([]string{endpoint}, paramStrs...)
	cacheKey := generateCacheKey(APIPrefix, keyInput...)

	// Serialize response to JSON
	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to serialize API response for caching: %v", err)
	}

	// Use provided TTL or default
	if ttl == 0 {
		ttl = cu.DefaultTTL
	}

	// Store in Redis
	err = cu.RedisClient.SetWithExpiry(cacheKey, string(data), ttl)
	if err != nil {
		return fmt.Errorf("failed to cache API response for key %s: %v", cacheKey, err)
	}

	log.Printf("Cached API response for key: %s with TTL: %v", cacheKey, ttl)
	return nil
}

// GetCachedAPIResponse retrieves a cached API response
func (cu *CacheUtils) GetCachedAPIResponse(endpoint string, queryParams map[string]string, response interface{}) (bool, error) {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return false, fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	// Convert query parameters to a sorted string to ensure consistent keys
	var paramStrs []string
	for k, v := range queryParams {
		paramStrs = append(paramStrs, fmt.Sprintf("%s=%s", k, v))
	}
	keyInput := append([]string{endpoint}, paramStrs...)
	cacheKey := generateCacheKey(APIPrefix, keyInput...)

	// Retrieve from Redis
	data, err := cu.RedisClient.Get(cacheKey)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve cached API response for key %s: %v", cacheKey, err)
	}

	// Deserialize JSON data into the provided response struct
	err = json.Unmarshal([]byte(data), response)
	if err != nil {
		return false, fmt.Errorf("failed to deserialize cached API response for key %s: %v", cacheKey, err)
	}

	log.Printf("Retrieved cached API response for key: %s", cacheKey)
	return true, nil
}

// CacheBlockchainData caches blockchain data (e.g., account info, block data) with a specified TTL
func (cu *CacheUtils) CacheBlockchainData(dataType, identifier string, data interface{}, ttl time.Duration) error {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	// Generate cache key based on data type and identifier
	prefix := BlockchainPrefix
	if dataType == "transaction" {
		prefix += TransactionPrefix
	} else if dataType == "account" {
		prefix += AccountPrefix
	}
	cacheKey := generateCacheKey(prefix, identifier)

	// Serialize data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize blockchain data for caching: %v", err)
	}

	// Use provided TTL or default
	if ttl == 0 {
		ttl = cu.DefaultTTL
	}

	// Store in Redis
	err = cu.RedisClient.SetWithExpiry(cacheKey, string(jsonData), ttl)
	if err != nil {
		return fmt.Errorf("failed to cache blockchain data for key %s: %v", cacheKey, err)
	}

	log.Printf("Cached blockchain data for key: %s with TTL: %v", cacheKey, ttl)
	return nil
}

// GetCachedBlockchainData retrieves cached blockchain data
func (cu *CacheUtils) GetCachedBlockchainData(dataType, identifier string, result interface{}) (bool, error) {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return false, fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	// Generate cache key based on data type and identifier
	prefix := BlockchainPrefix
	if dataType == "transaction" {
		prefix += TransactionPrefix
	} else if dataType == "account" {
		prefix += AccountPrefix
	}
	cacheKey := generateCacheKey(prefix, identifier)

	// Retrieve from Redis
	data, err := cu.RedisClient.Get(cacheKey)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve cached blockchain data for key %s: %v", cacheKey, err)
	}

	// Deserialize JSON data into the provided result struct
	err = json.Unmarshal([]byte(data), result)
	if err != nil {
		return false, fmt.Errorf("failed to deserialize cached blockchain data for key %s: %v", cacheKey, err)
	}

	log.Printf("Retrieved cached blockchain data for key: %s", cacheKey)
	return true, nil
}

// InvalidateCacheByPrefix invalidates all cache entries with a given prefix
func (cu *CacheUtils) InvalidateCacheByPrefix(prefix string) error {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	// Use Redis SCAN to find keys matching the prefix
	// Note: This is a simplified approach; in production, handle pagination for large datasets
	ctx := cu.RedisClient.Ctx
	iter := cu.RedisClient.Client.Scan(ctx, 0, prefix+"*", 100).Iterator()
	count := 0

	for iter.Next(ctx) {
		key := iter.Val()
		err := cu.RedisClient.Delete(key)
		if err != nil {
			log.Printf("Failed to delete cache key %s during invalidation: %v", key, err)
			continue
		}
		count++
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("error scanning cache keys with prefix %s: %v", prefix, err)
	}

	log.Printf("Invalidated %d cache entries with prefix: %s", count, prefix)
	return nil
}

// InvalidateCacheByKey invalidates a specific cache entry by key
func (cu *CacheUtils) InvalidateCacheByKey(key string) error {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	err := cu.RedisClient.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to invalidate cache for key %s: %v", key, err)
	}

	log.Printf("Invalidated cache for key: %s", key)
	return nil
}

// InvalidateBlockchainCache invalidates blockchain cache for a specific data type or identifier
func (cu *CacheUtils) InvalidateBlockchainCache(dataType, identifier string) error {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	// If identifier is empty, invalidate all entries of the data type
	prefix := BlockchainPrefix
	if dataType == "transaction" {
		prefix += TransactionPrefix
	} else if dataType == "account" {
		prefix += AccountPrefix
	}

	if identifier == "" {
		return cu.InvalidateCacheByPrefix(prefix)
	}

	// Invalidate specific identifier
	cacheKey := generateCacheKey(prefix, identifier)
	return cu.InvalidateCacheByKey(cacheKey)
}

// SetCacheTTL updates the TTL for an existing cache entry
func (cu *CacheUtils) SetCacheTTL(key string, ttl time.Duration) error {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	// Check if key exists
	_, err := cu.RedisClient.Get(key)
	if err != nil {
		return fmt.Errorf("cache key %s not found or error retrieving: %v", key, err)
	}

	// Set new TTL (Redis does not have a direct SetTTL, so we use Expire)
	err = cu.RedisClient.Client.Expire(cu.RedisClient.Ctx, key, ttl).Err()
	if err != nil {
		return fmt.Errorf("failed to set TTL for cache key %s: %v", key, err)
	}

	log.Printf("Updated TTL for cache key %s to %v", key, ttl)
	return nil
}

// GetCacheTTL retrieves the remaining TTL for a cache entry
func (cu *CacheUtils) GetCacheTTL(key string) (time.Duration, error) {
	if cu.RedisClient == nil || !cu.RedisClient.IsHealthy() {
		return 0, fmt.Errorf("Redis client is not initialized or unhealthy")
	}

	ttl, err := cu.RedisClient.Client.TTL(cu.RedisClient.Ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get TTL for cache key %s: %v", key, err)
	}

	if ttl < 0 {
		return 0, fmt.Errorf("cache key %s does not exist or has no TTL", key)
	}

	log.Printf("Retrieved TTL for cache key %s: %v", key, ttl)
	return ttl, nil
}
