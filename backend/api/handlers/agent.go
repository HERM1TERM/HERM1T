// agent.go    BUILME
// AI agent interaction endpoints for creation, control, and monitoring.
// This module provides RESTful API endpoints for handling AI agent-related operations.
// It includes agent lifecycle management and integration with user authentication.

package main 

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Agent represents the structure of an AI agent entity.
type Agent struct {
	ID          string    `json:"id"`
	Name        string    `json:"name" binding:"required"`
	Description string    `json:"description"`
	OwnerID     string    `json:"owner_id"`
	Status      string    `json:"status"` // e.g., "active", "inactive", "training"
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`
	Config      AgentConfig `json:"config"`
}

// AgentConfig represents the configuration for an AI agent.
type AgentConfig struct {
	ModelType    string `json:"model_type" binding:"required"` // e.g., "neural_net", "reinforcement"
	LearningRate float64 `json:"learning_rate" binding:"required"`
	Environment  string `json:"environment"` // e.g., "web3_solana", "local_sim"
}

// AgentControlRequest represents the structure for controlling an agent (start/stop).
type AgentControlRequest struct {
	Action string `json:"action" binding:"required"` // e.g., "start", "stop", "restart"
}

// AgentStore is a simple in-memory store for agents (replace with database in production).
var AgentStore = make(map[string]Agent)

// CreateAgentHandler handles the creation of a new AI agent.
func CreateAgentHandler(c *gin.Context) {
	userID := c.GetString("user_id") // Assumes user_id is set by AuthMiddleware
	if userID == "" {
		logger.Error("No user ID found in context for agent creation")
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Unauthorized access",
		})
		return
	}

	var agent Agent
	if err := c.ShouldBindJSON(&agent); err != nil {
		logger.Error("Invalid input for agent creation", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid input data",
			"details": err.Error(),
		})
		return
	}

	// Generate a unique ID (simplified; use UUID in production)
	idBytes := make([]byte, 16)
	_, err := rand.Read(idBytes)
	if err != nil {
		logger.Error("Failed to generate agent ID", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to generate agent ID",
		})
		return
	}
	agent.ID = base64.StdEncoding.EncodeToString(idBytes)
	agent.OwnerID = userID
	agent.Status = "inactive"
	agent.Created = time.Now()
	agent.Updated = time.Now()

	// Store the agent
	AgentStore[agent.ID] = agent
	logger.Info("AI agent created successfully", zap.String("agent_id", agent.ID), zap.String("owner_id", userID))

	// Return success response
	c.JSON(http.StatusCreated, gin.H{
		"message": "AI agent created successfully",
		"data":    agent,
	})
}

// GetAgentHandler retrieves details of a specific AI agent.
func GetAgentHandler(c *gin.Context) {
	userID := c.GetString("user_id") // Assumes user_id is set by AuthMiddleware
	if userID == "" {
		logger.Error("No user ID found in context for agent retrieval")
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Unauthorized access",
		})
		return
	}

	agentID := c.Param("id")
	if agentID == "" {
		logger.Error("Agent ID not provided in request")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Agent ID is required",
		})
		return
	}

	agent, exists := AgentStore[agentID]
	if !exists {
		logger.Warn("Agent not found", zap.String("agent_id", agentID))
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Agent not found",
		})
		return
	}

	// Ensure the user owns the agent
	if agent.OwnerID != userID {
		logger.Warn("Unauthorized access to agent", zap.String("agent_id", agentID), zap.String("user_id", userID))
		c.JSON(http.StatusForbidden, gin.H{
			"error": "You do not have permission to access this agent",
		})
		return
	}

	logger.Info("Agent details retrieved", zap.String("agent_id", agentID), zap.String("user_id", userID))
	c.JSON(http.StatusOK, gin.H{
		"data": agent,
	})
}

// ListAgentsHandler retrieves a list of all AI agents owned by the user.
func ListAgentsHandler(c *gin.Context) {
	userID := c.GetString("user_id") // Assumes user_id is set by AuthMiddleware
	if userID == "" {
		logger.Error("No user ID found in context for agent listing")
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Unauthorized access",
		})
		return
	}

	var userAgents []Agent
	for _, agent := range AgentStore {
		if agent.OwnerID == userID {
			userAgents = append(userAgents, agent)
		}
	}

	logger.Info("Agent list retrieved for user", zap.String("user_id", userID), zap.Int("count", len(userAgents)))
	c.JSON(http.StatusOK, gin.H{
		"data":  userAgents,
		"count": len(userAgents),
	})
}

// ControlAgentHandler handles starting, stopping, or restarting an AI agent.
func ControlAgentHandler(c *gin.Context) {
	userID := c.GetString("user_id") // Assumes user_id is set by AuthMiddleware
	if userID == "" {
		logger.Error("No user ID found in context for agent control")
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Unauthorized access",
		})
		return
	}

	agentID := c.Param("id")
	if agentID == "" {
		logger.Error("Agent ID not provided in request for control")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Agent ID is required",
		})
		return
	}

	var controlReq AgentControlRequest
	if err := c.ShouldBindJSON(&controlReq); err != nil {
		logger.Error("Invalid input for agent control", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid input data",
			"details": err.Error(),
		})
		return
	}

	agent, exists := AgentStore[agentID]
	if !exists {
		logger.Warn("Agent not found for control", zap.String("agent_id", agentID))
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Agent not found",
		})
		return
	}

	// Ensure the user owns the agent
	if agent.OwnerID != userID {
		logger.Warn("Unauthorized access to control agent", zap.String("agent_id", agentID), zap.String("user_id", userID))
		c.JSON(http.StatusForbidden, gin.H{
			"error": "You do not have permission to control this agent",
		})
		return
	}

	// Handle control actions (simplified logic; extend with actual agent runtime control)
	switch controlReq.Action {
	case "start":
		if agent.Status == "active" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Agent is already active",
			})
			return
		}
		agent.Status = "active"
	case "stop":
		if agent.Status == "inactive" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Agent is already inactive",
			})
			return
		}
		agent.Status = "inactive"
	case "restart":
		agent.Status = "active" // Simplified; could involve stopping and starting with delay
	default:
		logger.Warn("Invalid control action", zap.String("action", controlReq.Action))
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid control action. Use 'start', 'stop', or 'restart'",
		})
		return
	}

	agent.Updated = time.Now()
	AgentStore[agentID] = agent
	logger.Info("Agent control action executed", zap.String("agent_id", agentID), zap.String("action", controlReq.Action))

	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("Agent %s action completed", controlReq.Action),
		"data":    agent,
	})
}

// MonitorAgentHandler retrieves monitoring data or status updates for an AI agent.
func MonitorAgentHandler(c *gin.Context) {
	userID := c.GetString("user_id") // Assumes user_id is set by AuthMiddleware
	if userID == "" {
		logger.Error("No user ID found in context for agent monitoring")
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Unauthorized access",
		})
		return
	}

	agentID := c.Param("id")
	if agentID == "" {
		logger.Error("Agent ID not provided in request for monitoring")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Agent ID is required",
		})
		return
	}

	agent, exists := AgentStore[agentID]
	if !exists {
		logger.Warn("Agent not found for monitoring", zap.String("agent_id", agentID))
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Agent not found",
		})
		return
	}

	// Ensure the user owns the agent
	if agent.OwnerID != userID {
		logger.Warn("Unauthorized access to monitor agent", zap.String("agent_id", agentID), zap.String("user_id", userID))
		c.JSON(http.StatusForbidden, gin.H{
			"error": "You do not have permission to monitor this agent",
		})
		return
	}

	// Placeholder for monitoring data (extend with real metrics or logs in production)
	monitoringData := gin.H{
		"status":       agent.Status,
		"last_updated": agent.Updated,
		"uptime":       time.Since(agent.Created).String(), // Simplified uptime
		"metrics": gin.H{
			"tasks_completed": 0,    // Placeholder
			"error_rate":      0.0,  // Placeholder
			"performance":     "N/A", // Placeholder
		},
	}

	logger.Info("Agent monitoring data retrieved", zap.String("agent_id", agentID), zap.String("user_id", userID))
	c.JSON(http.StatusOK, gin.H{
		"data": monitoringData,
	})
}

// DeleteAgentHandler deletes an AI agent.
func DeleteAgentHandler(c *gin.Context) {
	userID := c.GetString("user_id") // Assumes user_id is set by AuthMiddleware
	if userID == "" {
		logger.Error("No user ID found in context for agent deletion")
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Unauthorized access",
		})
		return
	}

	agentID := c.Param("id")
	if agentID == "" {
		logger.Error("Agent ID not provided in request for deletion")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Agent ID is required",
		})
		return
	}

	agent, exists := AgentStore[agentID]
	if !exists {
		logger.Warn("Agent not found for deletion", zap.String("agent_id", agentID))
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Agent not found",
		})
		return
	}

	// Ensure the user owns the agent
	if agent.OwnerID != userID {
		logger.Warn("Unauthorized access to delete agent", zap.String("agent_id", agentID), zap.String("user_id", userID))
		c.JSON(http.StatusForbidden, gin.H{
			"error": "You do not have permission to delete this agent",
		})
		return
	}

	delete(AgentStore, agentID)
	logger.Info("Agent deleted successfully", zap.String("agent_id", agentID), zap.String("user_id", userID))

	c.JSON(http.StatusOK, gin.H{
		"message": "Agent deleted successfully",
	})
}

// SetupAgentRoutes configures the AI agent management endpoints.
func SetupAgentRoutes(router *gin.Engine) {
	agentGroup := router.Group("/api/agents")
	{
		agentGroup.POST("/create", AuthMiddleware(), CreateAgentHandler)
		agentGroup.GET("/list", AuthMiddleware(), ListAgentsHandler)
		agentGroup.GET("/:id", AuthMiddleware(), GetAgentHandler)
		agentGroup.POST("/:id/control", AuthMiddleware(), ControlAgentHandler)
		agentGroup.GET("/:id/monitor", AuthMiddleware(), MonitorAgentHandler)
		agentGroup.DELETE("/:id", AuthMiddleware(), DeleteAgentHandler)
	}
	logger.Info("AI agent management routes setup completed")
}
