package main

import (
	"bufio"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// CqlshrcConfig represents parsed cqlshrc configuration
type CqlshrcConfig struct {
	Connection     ConnectionConfig     `json:"connection"`
	Authentication AuthenticationConfig `json:"authentication"`
	SSL            SSLConfig            `json:"ssl"`
}

// ConnectionConfig holds [connection] section values
type ConnectionConfig struct {
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
	Timeout  int    `json:"timeout"`
}

// AuthenticationConfig holds [authentication] section values
type AuthenticationConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// SSLConfig holds [ssl] section values
type SSLConfig struct {
	Certfile     string `json:"certfile"`
	Keyfile      string `json:"keyfile"`
	CAFile       string `json:"ca_certs"`
	Validate     bool   `json:"validate"`
	Version      string `json:"version"`
	UserKeyStore string `json:"userkeystore"`
	UserKeyPass  string `json:"userkeypass"`
}

// VariableManifestEntry represents a variable definition
type VariableManifestEntry struct {
	Name  string `json:"name"`
	Scope string `json:"scope"`
}

// VariableValueEntry represents a variable value
type VariableValueEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Scope string `json:"scope"`
}

// ParseCqlshrc parses a cqlshrc INI-style configuration file
func ParseCqlshrc(filePath string) (*CqlshrcConfig, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &CqlshrcConfig{
		Connection: ConnectionConfig{
			Port: 9042, // Default port
		},
		SSL: SSLConfig{
			Validate: true, // Default to validate
		},
	}

	var currentSection string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// Check for section header
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.ToLower(strings.Trim(line, "[]"))
			continue
		}

		// Parse key=value pairs
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(strings.ToLower(parts[0]))
		value := strings.TrimSpace(parts[1])

		switch currentSection {
		case "connection":
			switch key {
			case "hostname":
				config.Connection.Hostname = value
			case "port":
				if port, err := strconv.Atoi(value); err == nil {
					config.Connection.Port = port
				}
			case "timeout":
				if timeout, err := strconv.Atoi(value); err == nil {
					config.Connection.Timeout = timeout
				}
			}

		case "authentication":
			switch key {
			case "username":
				config.Authentication.Username = value
			case "password":
				config.Authentication.Password = value
			}

		case "ssl":
			switch key {
			case "certfile":
				config.SSL.Certfile = value
			case "keyfile":
				config.SSL.Keyfile = value
			case "ca_certs":
				config.SSL.CAFile = value
			case "validate":
				config.SSL.Validate = strings.ToLower(value) == "true"
			case "version":
				config.SSL.Version = value
			case "userkeystore":
				config.SSL.UserKeyStore = value
			case "userkeypass":
				config.SSL.UserKeyPass = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return config, nil
}

// LoadVariables loads variable manifest and values, filtered by workspace ID
func LoadVariables(manifestPath, valuesPath, workspaceID string) (map[string]string, error) {
	variables := make(map[string]string)

	// Load manifest if provided
	var manifest []VariableManifestEntry
	if manifestPath != "" {
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, err
		}
	}

	// Load values if provided
	var values []VariableValueEntry
	if valuesPath != "" {
		data, err := os.ReadFile(valuesPath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &values); err != nil {
			return nil, err
		}
	}

	// Build allowed variables set from manifest (filtered by workspace)
	allowedVars := make(map[string]bool)
	for _, entry := range manifest {
		// Allow if scope matches workspace ID or is "workspace-all"
		if entry.Scope == workspaceID || entry.Scope == "workspace-all" {
			allowedVars[entry.Name] = true
		}
	}

	// Build variables map from values (only if in allowed set)
	for _, entry := range values {
		// Check scope match
		if entry.Scope == workspaceID || entry.Scope == "workspace-all" {
			// Check if in manifest (or if no manifest, allow all)
			if len(manifest) == 0 || allowedVars[entry.Name] {
				variables[entry.Name] = entry.Value
			}
		}
	}

	return variables, nil
}

// ApplyVariables replaces ${var_name} placeholders in a string with variable values
func ApplyVariables(input string, variables map[string]string) string {
	result := input
	// Match ${variable_name} pattern
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	result = re.ReplaceAllStringFunc(result, func(match string) string {
		// Extract variable name from ${name}
		varName := strings.TrimPrefix(strings.TrimSuffix(match, "}"), "${")
		if value, ok := variables[varName]; ok {
			return value
		}
		return match // Keep original if not found
	})
	return result
}

// ApplyVariablesToConfig applies variable substitution to all config values
func ApplyVariablesToConfig(config *CqlshrcConfig, variables map[string]string) {
	// Connection
	config.Connection.Hostname = ApplyVariables(config.Connection.Hostname, variables)

	// Authentication
	config.Authentication.Username = ApplyVariables(config.Authentication.Username, variables)
	config.Authentication.Password = ApplyVariables(config.Authentication.Password, variables)

	// SSL
	config.SSL.Certfile = ApplyVariables(config.SSL.Certfile, variables)
	config.SSL.Keyfile = ApplyVariables(config.SSL.Keyfile, variables)
	config.SSL.CAFile = ApplyVariables(config.SSL.CAFile, variables)
	config.SSL.UserKeyStore = ApplyVariables(config.SSL.UserKeyStore, variables)
	config.SSL.UserKeyPass = ApplyVariables(config.SSL.UserKeyPass, variables)
}

// ParseCqlshrcWithVariables parses cqlshrc and applies variable substitution
func ParseCqlshrcWithVariables(cqlshrcPath, manifestPath, valuesPath, workspaceID string) (*CqlshrcConfig, error) {
	// First, we need to read the raw file and apply variables before parsing
	// This is because variables might be in the cqlshrc file itself

	// Load variables
	variables, err := LoadVariables(manifestPath, valuesPath, workspaceID)
	if err != nil {
		return nil, err
	}

	// Read cqlshrc file
	data, err := os.ReadFile(cqlshrcPath)
	if err != nil {
		return nil, err
	}

	// Apply variables to raw content
	content := ApplyVariables(string(data), variables)

	// Create a temp file with substituted content (or parse in-memory)
	// For simplicity, let's parse from the substituted string directly
	config := &CqlshrcConfig{
		Connection: ConnectionConfig{
			Port: 9042,
		},
		SSL: SSLConfig{
			Validate: true,
		},
	}

	var currentSection string
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// Check for section header
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.ToLower(strings.Trim(line, "[]"))
			continue
		}

		// Parse key=value pairs
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(strings.ToLower(parts[0]))
		value := strings.TrimSpace(parts[1])

		switch currentSection {
		case "connection":
			switch key {
			case "hostname":
				config.Connection.Hostname = value
			case "port":
				if port, err := strconv.Atoi(value); err == nil {
					config.Connection.Port = port
				}
			case "timeout":
				if timeout, err := strconv.Atoi(value); err == nil {
					config.Connection.Timeout = timeout
				}
			}

		case "authentication":
			switch key {
			case "username":
				config.Authentication.Username = value
			case "password":
				config.Authentication.Password = value
			}

		case "ssl":
			switch key {
			case "certfile":
				config.SSL.Certfile = value
			case "keyfile":
				config.SSL.Keyfile = value
			case "ca_certs":
				config.SSL.CAFile = value
			case "validate":
				config.SSL.Validate = strings.ToLower(value) == "true"
			case "version":
				config.SSL.Version = value
			case "userkeystore":
				config.SSL.UserKeyStore = value
			case "userkeypass":
				config.SSL.UserKeyPass = value
			}
		}
	}

	return config, nil
}
