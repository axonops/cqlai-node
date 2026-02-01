package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// TLSSecurityInfo represents TLS security analysis results
type TLSSecurityInfo struct {
	Encrypted       bool              `json:"encrypted"`
	Protocol        string            `json:"protocol"`
	CipherSuite     string            `json:"cipher_suite"`
	ServerCert      *CertificateInfo  `json:"server_cert,omitempty"`
	CertChain       []CertificateInfo `json:"cert_chain,omitempty"`
	Warnings        []string          `json:"warnings,omitempty"`
	Recommendations []string          `json:"recommendations,omitempty"`
}

// CertificateInfo represents certificate details
type CertificateInfo struct {
	Subject            string    `json:"subject"`
	Issuer             string    `json:"issuer"`
	SerialNumber       string    `json:"serial_number"`
	NotBefore          time.Time `json:"not_before"`
	NotAfter           time.Time `json:"not_after"`
	DNSNames           []string  `json:"dns_names,omitempty"`
	IPAddresses        []string  `json:"ip_addresses,omitempty"`
	IsCA               bool      `json:"is_ca"`
	SignatureAlgorithm string    `json:"signature_algorithm"`
	PublicKeyAlgorithm string    `json:"public_key_algorithm"`
	KeyUsage           []string  `json:"key_usage,omitempty"`
	DaysUntilExpiry    int       `json:"days_until_expiry"`
	IsExpired          bool      `json:"is_expired"`
	IsSelfSigned       bool      `json:"is_self_signed"`
}

// CheckTLSSecurity analyzes the TLS security of a Cassandra connection
func CheckTLSSecurity(host string, port int, caFile, certFile, keyFile string, skipVerify bool) (*TLSSecurityInfo, error) {
	info := &TLSSecurityInfo{
		Encrypted:       false,
		Warnings:        []string{},
		Recommendations: []string{},
	}

	// Build TLS config
	tlsConfig := &tls.Config{
		InsecureSkipVerify: skipVerify,
	}

	// Load CA certificate if provided
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %v", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate if provided
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Connect with TLS
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		// Connection failed - might not support TLS
		return nil, fmt.Errorf("TLS connection failed: %v", err)
	}
	defer conn.Close()

	// TLS connection successful
	info.Encrypted = true

	// Get connection state
	state := conn.ConnectionState()

	// Protocol version
	info.Protocol = tlsVersionString(state.Version)

	// Cipher suite
	info.CipherSuite = tls.CipherSuiteName(state.CipherSuite)

	// Analyze peer certificates
	if len(state.PeerCertificates) > 0 {
		// Server certificate (first in chain)
		serverCert := parseCertificate(state.PeerCertificates[0])
		info.ServerCert = &serverCert

		// Full certificate chain
		for _, cert := range state.PeerCertificates {
			info.CertChain = append(info.CertChain, parseCertificate(cert))
		}
	}

	// Security analysis
	analyzeSecurityIssues(info, skipVerify)

	return info, nil
}

// CheckTLSSecurityFromFiles analyzes certificates without connecting
func CheckTLSSecurityFromFiles(caFile, certFile, keyFile string) (*TLSSecurityInfo, error) {
	info := &TLSSecurityInfo{
		Encrypted:       false,
		Warnings:        []string{},
		Recommendations: []string{},
		CertChain:       []CertificateInfo{},
	}

	// Analyze CA certificate
	if caFile != "" {
		certs, err := loadCertificatesFromFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load CA file: %v", err)
		}
		for _, cert := range certs {
			certInfo := parseCertificate(cert)
			info.CertChain = append(info.CertChain, certInfo)

			// Check for issues
			if certInfo.IsExpired {
				info.Warnings = append(info.Warnings, fmt.Sprintf("CA certificate '%s' is expired", certInfo.Subject))
			} else if certInfo.DaysUntilExpiry < 30 {
				info.Warnings = append(info.Warnings, fmt.Sprintf("CA certificate '%s' expires in %d days", certInfo.Subject, certInfo.DaysUntilExpiry))
			}
		}
	}

	// Analyze client certificate
	if certFile != "" {
		certs, err := loadCertificatesFromFile(certFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %v", err)
		}
		if len(certs) > 0 {
			clientCert := parseCertificate(certs[0])
			info.ServerCert = &clientCert

			if clientCert.IsExpired {
				info.Warnings = append(info.Warnings, fmt.Sprintf("Client certificate '%s' is expired", clientCert.Subject))
			} else if clientCert.DaysUntilExpiry < 30 {
				info.Warnings = append(info.Warnings, fmt.Sprintf("Client certificate '%s' expires in %d days", clientCert.Subject, clientCert.DaysUntilExpiry))
			}
		}
	}

	// Verify key file is readable
	if keyFile != "" {
		keyData, err := os.ReadFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read key file: %v", err)
		}

		block, _ := pem.Decode(keyData)
		if block == nil {
			info.Warnings = append(info.Warnings, "Key file does not contain valid PEM data")
		} else {
			// Check if key is encrypted
			if strings.Contains(block.Type, "ENCRYPTED") || x509.IsEncryptedPEMBlock(block) {
				info.Recommendations = append(info.Recommendations, "Private key is encrypted - password will be required")
			}
		}
	}

	return info, nil
}

func loadCertificatesFromFile(filename string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var certs []*x509.Certificate
	for {
		block, rest := pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}
			certs = append(certs, cert)
		}
		data = rest
	}

	if len(certs) == 0 {
		// Try DER format
		cert, err := x509.ParseCertificate(data)
		if err != nil {
			return nil, fmt.Errorf("no valid certificates found")
		}
		certs = append(certs, cert)
	}

	return certs, nil
}

func parseCertificate(cert *x509.Certificate) CertificateInfo {
	info := CertificateInfo{
		Subject:            cert.Subject.String(),
		Issuer:             cert.Issuer.String(),
		SerialNumber:       cert.SerialNumber.String(),
		NotBefore:          cert.NotBefore,
		NotAfter:           cert.NotAfter,
		DNSNames:           cert.DNSNames,
		IsCA:               cert.IsCA,
		SignatureAlgorithm: cert.SignatureAlgorithm.String(),
		PublicKeyAlgorithm: cert.PublicKeyAlgorithm.String(),
		KeyUsage:           parseKeyUsage(cert.KeyUsage),
		IsSelfSigned:       cert.Subject.String() == cert.Issuer.String(),
	}

	// Parse IP addresses
	for _, ip := range cert.IPAddresses {
		info.IPAddresses = append(info.IPAddresses, ip.String())
	}

	// Calculate expiry
	now := time.Now()
	info.IsExpired = now.After(cert.NotAfter)
	info.DaysUntilExpiry = int(cert.NotAfter.Sub(now).Hours() / 24)

	return info
}

func parseKeyUsage(usage x509.KeyUsage) []string {
	var usages []string
	if usage&x509.KeyUsageDigitalSignature != 0 {
		usages = append(usages, "DigitalSignature")
	}
	if usage&x509.KeyUsageContentCommitment != 0 {
		usages = append(usages, "ContentCommitment")
	}
	if usage&x509.KeyUsageKeyEncipherment != 0 {
		usages = append(usages, "KeyEncipherment")
	}
	if usage&x509.KeyUsageDataEncipherment != 0 {
		usages = append(usages, "DataEncipherment")
	}
	if usage&x509.KeyUsageKeyAgreement != 0 {
		usages = append(usages, "KeyAgreement")
	}
	if usage&x509.KeyUsageCertSign != 0 {
		usages = append(usages, "CertSign")
	}
	if usage&x509.KeyUsageCRLSign != 0 {
		usages = append(usages, "CRLSign")
	}
	return usages
}

func tlsVersionString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown (0x%04x)", version)
	}
}

func analyzeSecurityIssues(info *TLSSecurityInfo, skipVerify bool) {
	// Check TLS version
	if info.Protocol == "TLS 1.0" || info.Protocol == "TLS 1.1" {
		info.Warnings = append(info.Warnings, fmt.Sprintf("%s is deprecated and insecure", info.Protocol))
		info.Recommendations = append(info.Recommendations, "Upgrade to TLS 1.2 or TLS 1.3")
	}

	// Check cipher suite for known weak ciphers
	cipherLower := strings.ToLower(info.CipherSuite)
	if strings.Contains(cipherLower, "rc4") {
		info.Warnings = append(info.Warnings, "RC4 cipher is weak and should not be used")
	}
	if strings.Contains(cipherLower, "des") && !strings.Contains(cipherLower, "3des") {
		info.Warnings = append(info.Warnings, "DES cipher is weak and should not be used")
	}
	if strings.Contains(cipherLower, "null") {
		info.Warnings = append(info.Warnings, "NULL cipher provides no encryption")
	}
	if strings.Contains(cipherLower, "export") {
		info.Warnings = append(info.Warnings, "Export-grade cipher is weak")
	}

	// Check certificate verification
	if skipVerify {
		info.Warnings = append(info.Warnings, "Certificate verification is disabled")
		info.Recommendations = append(info.Recommendations, "Enable certificate verification in production")
	}

	// Check server certificate
	if info.ServerCert != nil {
		if info.ServerCert.IsExpired {
			info.Warnings = append(info.Warnings, "Server certificate is expired")
			info.Recommendations = append(info.Recommendations, "Renew the server certificate immediately")
		} else if info.ServerCert.DaysUntilExpiry < 30 {
			info.Warnings = append(info.Warnings, fmt.Sprintf("Server certificate expires in %d days", info.ServerCert.DaysUntilExpiry))
			info.Recommendations = append(info.Recommendations, "Plan certificate renewal soon")
		}

		if info.ServerCert.IsSelfSigned {
			info.Warnings = append(info.Warnings, "Server certificate is self-signed")
			info.Recommendations = append(info.Recommendations, "Use certificates signed by a trusted CA in production")
		}

		// Check signature algorithm
		sigAlgo := strings.ToLower(info.ServerCert.SignatureAlgorithm)
		if strings.Contains(sigAlgo, "md5") {
			info.Warnings = append(info.Warnings, "Certificate uses MD5 signature which is insecure")
		}
		if strings.Contains(sigAlgo, "sha1") {
			info.Warnings = append(info.Warnings, "Certificate uses SHA-1 signature which is deprecated")
			info.Recommendations = append(info.Recommendations, "Use SHA-256 or stronger for certificate signatures")
		}
	}
}
