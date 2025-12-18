package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
 
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

// RedisConfig holds the configuration for Redis connection
type RedisConfig struct {
	Host              string
	Port              int
	Password          string
	DB                int
	PoolSize          int
	MinIdleConns      int
	MaxConnAge        time.Duration
	IdleTimeout       time.Duration
	SentinelEnabled   bool
	SentinelMaster    string
	SentinelAddresses []string
}

// RedisClient wraps the Redis client with additional metadata
type RedisClient struct {
	Client *redis.Client
	Ctx    context.Context
	Cancel context.CancelFunc
	Config *RedisConfig
}

// LoadRedisConfig loads Redis configuration from environment variables
func LoadRedisConfig() (*RedisConfig, error) {
	// Load environment variables from .env file if present
	if err := godotenv.Load(); err != nil {
		log.Printf("No .env file found, relying on system environment variables: %v", err)
	}

	// Parse port
	portStr := os.Getenv("REDIS_PORT")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_PORT value: %v", err)
	}

	// Parse DB number
	dbStr := os.Getenv("REDIS_DB")
	db, err := strconv.Atoi(dbStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_DB value: %v", err)
	}

	// Parse pool size
	poolSizeStr := os.Getenv("REDIS_POOL_SIZE")
	poolSize, err := strconv.Atoi(poolSizeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_POOL_SIZE value: %v", err)
	}

	// Parse min idle connections
	minIdleConnsStr := os.Getenv("REDIS_MIN_IDLE_CONNS")
	minIdleConns, err := strconv.Atoi(minIdleConnsStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_MIN_IDLE_CONNS value: %v", err)
	}

	// Parse max connection age (in seconds)
	maxConnAgeStr := os.Getenv("REDIS_MAX_CONN_AGE")
	maxConnAgeSeconds, err := strconv.Atoi(maxConnAgeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_MAX_CONN_AGE value: %v", err)
	}

	// Parse idle timeout (in seconds)
	idleTimeoutStr := os.Getenv("REDIS_IDLE_TIMEOUT")
	idleTimeoutSeconds, err := strconv.Atoi(idleTimeoutStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_IDLE_TIMEOUT value: %v", err)
	}

	// Parse Sentinel enabled flag
	sentinelEnabledStr := os.Getenv("REDIS_SENTINEL_ENABLED")
	sentinelEnabled, err := strconv.ParseBool(sentinelEnabledStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_SENTINEL_ENABLED value: %v", err)
	}

	// Parse Sentinel addresses (comma-separated)
	sentinelAddresses := []string{}
	if sentinelEnabled {
		sentinelAddrStr := os.Getenv("REDIS_SENTINEL_ADDRESSES")
		if sentinelAddrStr == "" {
			return nil, fmt.Errorf("REDIS_SENTINEL_ADDRESSES must be set when Sentinel is enabled")
		}
		sentinelAddresses = splitCommaSeparated(sentinelAddrStr)
	}

	config := &RedisConfig{
		Host:            os.Getenv("REDIS_HOST"),
		Port:            port,
		Password:        os.Getenv("REDIS_PASSWORD"),
		DB:              db,
		PoolSize:        poolSize,
		MinIdleConns:    minIdleConns,
		MaxConnAge:      time.Duration(maxConnAgeSeconds) * time.Second,
		IdleTimeout:     time.Duration(idleTimeoutSeconds) * time.Second,
		SentinelEnabled: sentinelEnabled,
		SentinelMaster:  os.Getenv("REDIS_SENTINEL_MASTER"),
		SentinelAddresses: sentinelAddresses,
	}

	// Validate required fields
	if config.Host == "" {
		return nil, fmt.Errorf("REDIS_HOST is required")
	}
	if config.SentinelEnabled && config.SentinelMaster == "" {
		return nil, fmt.Errorf("REDIS_SENTINEL_MASTER is required when Sentinel is enabled")
	}

	return config, nil
}

// splitCommaSeparated splits a comma-separated string into a slice
func splitCommaSeparated(input string) []string {
	if input == "" {
		return []string{}
	}
	result := []string{}
	current := ""
	for _, char := range input {
		if char == ',' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else if char != ' ' {
			current += string(char)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

// NewRedisClient initializes a new Redis client with or without Sentinel support
func NewRedisClient(config *RedisConfig) (*RedisClient, error) {
	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	var client *redis.Client
	if config.SentinelEnabled {
		// Configure Redis with Sentinel for failover
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       config.SentinelMaster,
			SentinelAddrs:    config.SentinelAddresses,
			Password:         config.Password,
			DB:               config.DB,
			PoolSize:         config.PoolSize,
			MinIdleConns:     config.MinIdleConns,
			MaxConnAge:       config.MaxConnAge,
			IdleTimeout:      config.IdleTimeout,
			DialTimeout:      5 * time.Second,
			ReadTimeout:      3 * time.Second,
			WriteTimeout:     3 * time.Second,
			SentinelPassword: config.Password, // Optional, if Sentinel requires authentication
		})
	} else {
		// Configure standalone Redis client
		client = redis.NewClient(&redis.Options{
			Addr:         fmt.Sprintf("%s:%d", config.Host, config.Port),
			Password:     config.Password,
			DB:           config.DB,
			PoolSize:     config.PoolSize,
			MinIdleConns: config.MinIdleConns,
			MaxConnAge:   config.MaxConnAge,
			IdleTimeout:  config.IdleTimeout,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
	}

	// Test the connection
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	log.Println("Successfully connected to Redis")
	return &RedisClient{
		Client: client,
		Ctx:    ctx,
		Cancel: cancel,
		Config: config,
	}, nil
}

// Close shuts down the Redis client and cancels the context
func (rc *RedisClient) Close() {
	if rc.Client != nil {
		if err := rc.Client.Close(); err != nil {
			log.Printf("Error closing Redis client: %v", err)
		}
	}
	rc.Cancel()
	log.Println("Redis client closed")
}

// IsHealthy checks if the Redis connection is active
func (rc *RedisClient) IsHealthy() bool {
	if rc.Client == nil {
		return false
	}
	err := rc.Client.Ping(rc.Ctx).Err()
	if err != nil {
		log.Printf("Redis health check failed: %v", err)
		return false
	}
	return true
}

// SetWithExpiry sets a key-value pair in Redis with an expiration time
func (rc *RedisClient) SetWithExpiry(key string, value interface{}, expiration time.Duration) error {
	err := rc.Client.Set(rc.Ctx, key, value, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set key %s in Redis: %v", key, err)
	}
	return nil
}

// Get retrieves a value from Redis by key
func (rc *RedisClient) Get(key string) (string, error) {
	val, err := rc.Client.Get(rc.Ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("key %s not found in Redis", key)
	} else if err != nil {
		return "", fmt.Errorf("failed to get key %s from Redis: %v", key, err)
	}
	return val, nil
}

// Delete removes a key from Redis
func (rc *RedisClient) Delete(key string) error {
	err := rc.Client.Del(rc.Ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to delete key %s from Redis: %v", key, err)
	}
	return nil
}
