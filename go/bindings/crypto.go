package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
)

// RSAKeyPair represents a generated RSA key pair
type RSAKeyPair struct {
	PublicKey  string `json:"publicKey"`  // PEM encoded
	PrivateKey string `json:"privateKey"` // PEM encoded
	KeySize    int    `json:"keySize"`
}

// EncryptedCredential represents an encrypted credential
type EncryptedCredential struct {
	Ciphertext string `json:"ciphertext"` // Base64 encoded
	Algorithm  string `json:"algorithm"`
}

// GenerateRSAKeyPair generates a new RSA key pair
func GenerateRSAKeyPair(bits int) (*RSAKeyPair, error) {
	if bits < 2048 {
		bits = 2048 // Minimum secure key size
	}

	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, fmt.Errorf("failed to generate RSA key: %v", err)
	}

	// Encode private key to PEM
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	// Encode public key to PEM
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %v", err)
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	return &RSAKeyPair{
		PublicKey:  string(publicKeyPEM),
		PrivateKey: string(privateKeyPEM),
		KeySize:    bits,
	}, nil
}

// EncryptWithPublicKey encrypts plaintext using an RSA public key
func EncryptWithPublicKey(plaintext string, publicKeyPEM string) (*EncryptedCredential, error) {
	// Parse public key
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM block containing public key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		// Try parsing as PKCS1 public key
		pub, err = x509.ParsePKCS1PublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key: %v", err)
		}
	}

	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA public key")
	}

	// Encrypt using OAEP with SHA-256
	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rsaPub, []byte(plaintext), nil)
	if err != nil {
		return nil, fmt.Errorf("encryption failed: %v", err)
	}

	return &EncryptedCredential{
		Ciphertext: base64.StdEncoding.EncodeToString(ciphertext),
		Algorithm:  "RSA-OAEP-SHA256",
	}, nil
}

// DecryptWithPrivateKey decrypts ciphertext using an RSA private key
// Tries SHA-1 first (compatible with Python's PKCS1_OAEP default), then SHA-256
func DecryptWithPrivateKey(ciphertextBase64 string, privateKeyPEM string) (string, error) {
	// Decode base64 ciphertext
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextBase64)
	if err != nil {
		return "", fmt.Errorf("failed to decode ciphertext: %v", err)
	}

	// Parse private key
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return "", fmt.Errorf("failed to parse PEM block containing private key")
	}

	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		// Try PKCS8 format
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return "", fmt.Errorf("failed to parse private key: %v", err)
		}
		var ok bool
		priv, ok = key.(*rsa.PrivateKey)
		if !ok {
			return "", fmt.Errorf("not an RSA private key")
		}
	}

	// Try SHA-1 first (compatible with Python's pycryptodome PKCS1_OAEP default)
	plaintext, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, ciphertext, nil)
	if err == nil {
		return string(plaintext), nil
	}

	// Fall back to SHA-256 if SHA-1 fails
	plaintext, err = rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed: %v", err)
	}

	return string(plaintext), nil
}

// EncryptWithPublicKeyFile encrypts using a public key from file
func EncryptWithPublicKeyFile(plaintext string, publicKeyPath string) (*EncryptedCredential, error) {
	keyData, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read public key file: %v", err)
	}
	return EncryptWithPublicKey(plaintext, string(keyData))
}

// DecryptWithPrivateKeyFile decrypts using a private key from file
func DecryptWithPrivateKeyFile(ciphertextBase64 string, privateKeyPath string) (string, error) {
	keyData, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return "", fmt.Errorf("failed to read private key file: %v", err)
	}
	return DecryptWithPrivateKey(ciphertextBase64, string(keyData))
}

// SaveKeyToFile saves a PEM key to a file
func SaveKeyToFile(keyPEM string, filepath string, permissions os.FileMode) error {
	return os.WriteFile(filepath, []byte(keyPEM), permissions)
}

// LoadKeyFromFile loads a PEM key from a file
func LoadKeyFromFile(filepath string) (string, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
