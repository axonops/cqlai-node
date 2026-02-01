package main

import (
	"archive/zip"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// AstraBundleInfo represents parsed secure connect bundle information
type AstraBundleInfo struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Keyspace       string `json:"keyspace,omitempty"`
	LocalDC        string `json:"localDC"`
	CACertPath     string `json:"caCertPath"`
	CertPath       string `json:"certPath"`
	KeyPath        string `json:"keyPath"`
	TrustStorePath string `json:"trustStorePath,omitempty"`
	KeyStorePath   string `json:"keyStorePath,omitempty"`
	BundlePath     string `json:"bundlePath"`
	ExtractedDir   string `json:"extractedDir"`

	// Resolved from metadata service (populated by FetchAstraMetadata)
	SniHost       string   `json:"sniHost,omitempty"`
	SniPort       int      `json:"sniPort,omitempty"`
	ContactPoints []string `json:"contactPoints,omitempty"`
}

// AstraMetadataResponse represents the response from the Astra metadata service
type AstraMetadataResponse struct {
	Version     int              `json:"version"`
	Region      string           `json:"region"`
	ContactInfo AstraContactInfo `json:"contact_info"`
}

// AstraContactInfo contains the connection information from the metadata service
type AstraContactInfo struct {
	Type            string   `json:"type"`
	LocalDC         string   `json:"local_dc"`
	SniProxyAddress string   `json:"sni_proxy_address"`
	ContactPoints   []string `json:"contact_points"`
}

// AstraConfig represents the config.json inside the bundle
type AstraConfig struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Keyspace   string `json:"keyspace"`
	LocalDC    string `json:"localDataCenter"`
	CQLVersion string `json:"cql_version"`
}

// ParseAstraBundle extracts and parses a DataStax Astra secure connect bundle
func ParseAstraBundle(bundlePath string, extractDir string) (*AstraBundleInfo, error) {
	// Verify bundle exists
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("bundle file not found: %s", bundlePath)
	}

	// Create extraction directory if not specified
	if extractDir == "" {
		extractDir = filepath.Join(os.TempDir(), "astra-bundle-"+filepath.Base(bundlePath))
	}

	// Extract the bundle
	if err := extractZip(bundlePath, extractDir); err != nil {
		return nil, fmt.Errorf("failed to extract bundle: %v", err)
	}

	// Parse config.json
	configPath := filepath.Join(extractDir, "config.json")
	config, err := parseAstraConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config.json: %v", err)
	}

	// Build result
	info := &AstraBundleInfo{
		Host:         config.Host,
		Port:         config.Port,
		Keyspace:     config.Keyspace,
		LocalDC:      config.LocalDC,
		BundlePath:   bundlePath,
		ExtractedDir: extractDir,
	}

	// Find certificate files
	caCertPath := filepath.Join(extractDir, "ca.crt")
	if _, err := os.Stat(caCertPath); err == nil {
		info.CACertPath = caCertPath
	}

	certPath := filepath.Join(extractDir, "cert")
	if _, err := os.Stat(certPath); err == nil {
		info.CertPath = certPath
	}

	keyPath := filepath.Join(extractDir, "key")
	if _, err := os.Stat(keyPath); err == nil {
		info.KeyPath = keyPath
	}

	// Check for Java keystore files (optional)
	trustStorePath := filepath.Join(extractDir, "trustStore.jks")
	if _, err := os.Stat(trustStorePath); err == nil {
		info.TrustStorePath = trustStorePath
	}

	keyStorePath := filepath.Join(extractDir, "keyStore.jks")
	if _, err := os.Stat(keyStorePath); err == nil {
		info.KeyStorePath = keyStorePath
	}

	// Validate required files exist
	if info.CACertPath == "" {
		return nil, fmt.Errorf("ca.crt not found in bundle")
	}
	if info.CertPath == "" {
		return nil, fmt.Errorf("cert not found in bundle")
	}
	if info.KeyPath == "" {
		return nil, fmt.Errorf("key not found in bundle")
	}

	return info, nil
}

// GetAstraSessionOptions converts bundle info to session options
func GetAstraSessionOptions(bundleInfo *AstraBundleInfo, username, password string) map[string]interface{} {
	return map[string]interface{}{
		"host":        bundleInfo.Host,
		"port":        bundleInfo.Port,
		"keyspace":    bundleInfo.Keyspace,
		"username":    username,
		"password":    password,
		"sslCaFile":   bundleInfo.CACertPath,
		"sslCertfile": bundleInfo.CertPath,
		"sslKeyfile":  bundleInfo.KeyPath,
		"sslValidate": true,
	}
}

func extractZip(zipPath, destDir string) error {
	// Create destination directory
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	// Open the zip file
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		// Prevent path traversal
		destPath := filepath.Join(destDir, f.Name)
		if !strings.HasPrefix(destPath, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("invalid file path in zip: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(destPath, f.Mode())
			continue
		}

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return err
		}

		// Extract file
		outFile, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

func parseAstraConfig(configPath string) (*AstraConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config AstraConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	// Set defaults
	if config.Port == 0 {
		config.Port = 29042 // Astra default port
	}

	return &config, nil
}

// CleanupAstraBundle removes extracted bundle files
func CleanupAstraBundle(extractedDir string) error {
	if extractedDir == "" {
		return nil
	}
	return os.RemoveAll(extractedDir)
}

// ValidateAstraBundle checks if a file is a valid Astra secure connect bundle
func ValidateAstraBundle(bundlePath string) (bool, []string) {
	var errors []string

	// Check file exists
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		return false, []string{"Bundle file not found"}
	}

	// Try to open as zip
	r, err := zip.OpenReader(bundlePath)
	if err != nil {
		return false, []string{"Not a valid zip file: " + err.Error()}
	}
	defer r.Close()

	// Check for required files
	requiredFiles := map[string]bool{
		"config.json": false,
		"ca.crt":      false,
		"cert":        false,
		"key":         false,
	}

	for _, f := range r.File {
		if _, ok := requiredFiles[f.Name]; ok {
			requiredFiles[f.Name] = true
		}
	}

	for file, found := range requiredFiles {
		if !found {
			errors = append(errors, fmt.Sprintf("Missing required file: %s", file))
		}
	}

	return len(errors) == 0, errors
}

// FetchAstraMetadata connects to the Astra metadata service and retrieves
// the actual connection endpoints (SNI proxy address and contact points).
// This must be called after ParseAstraBundle to get the real connection info.
func FetchAstraMetadata(bundleInfo *AstraBundleInfo, timeout time.Duration) error {
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	// Load CA certificate
	caCert, err := os.ReadFile(bundleInfo.CACertPath)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("failed to parse CA certificate")
	}

	// Load client certificate and key for mTLS
	clientCert, err := tls.LoadX509KeyPair(bundleInfo.CertPath, bundleInfo.KeyPath)
	if err != nil {
		return fmt.Errorf("failed to load client certificate: %v", err)
	}

	// Create TLS config for mTLS connection to metadata service
	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
	}

	// Create HTTP client with TLS config
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	// Build metadata URL
	metadataURL := fmt.Sprintf("https://%s:%d/metadata", bundleInfo.Host, bundleInfo.Port)

	// Make request to metadata service
	resp, err := client.Get(metadataURL)
	if err != nil {
		return fmt.Errorf("failed to connect to metadata service at %s: %v", metadataURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("metadata service returned status %d", resp.StatusCode)
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read metadata response: %v", err)
	}

	// Parse metadata response
	var metadata AstraMetadataResponse
	if err := json.Unmarshal(body, &metadata); err != nil {
		return fmt.Errorf("failed to parse metadata response: %v", err)
	}

	// Extract SNI proxy address (format: "host:port")
	proxyParts := strings.Split(metadata.ContactInfo.SniProxyAddress, ":")
	if len(proxyParts) != 2 {
		return fmt.Errorf("invalid sni_proxy_address format: %s", metadata.ContactInfo.SniProxyAddress)
	}

	bundleInfo.SniHost = proxyParts[0]
	sniPort, err := strconv.Atoi(proxyParts[1])
	if err != nil {
		return fmt.Errorf("invalid SNI proxy port: %s", proxyParts[1])
	}
	bundleInfo.SniPort = sniPort

	// Store contact points (host IDs used as SNI server names)
	bundleInfo.ContactPoints = metadata.ContactInfo.ContactPoints

	// Update LocalDC from metadata if provided
	if metadata.ContactInfo.LocalDC != "" {
		bundleInfo.LocalDC = metadata.ContactInfo.LocalDC
	}

	return nil
}
