package steps_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"

	"github.com/axonops/cqlai-node/internal/config"
	"github.com/axonops/cqlai-node/internal/db"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(name string) (*testCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

type sslWorld struct {
	dir      string
	cas      map[string]*testCA
	listener net.Listener
	caFile   string
	err      error
}

func (w *sslWorld) ca(name string) (*testCA, error) {
	if c, ok := w.cas[name]; ok {
		return c, nil
	}
	c, err := newTestCA(name)
	if err != nil {
		return nil, err
	}
	w.cas[name] = c
	return c, nil
}

func (w *sslWorld) serverWith(signer, names string) error {
	ca, err := w.ca(signer)
	if err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, n := range strings.Split(names, ",") {
		kind, value, ok := strings.Cut(strings.TrimSpace(n), ":")
		if !ok {
			return fmt.Errorf("name %q is not DNS:x or IP:x", n)
		}
		if kind == "IP" {
			tmpl.IPAddresses = append(tmpl.IPAddresses, net.ParseIP(value))
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, value)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return err
	}
	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		return err
	}
	w.listener = l
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_ = c.(*tls.Conn).Handshake()
			}()
		}
	}()
	return nil
}

func (w *sslWorld) caFileHolds(name string) error {
	ca, err := w.ca(name)
	if err != nil {
		return err
	}
	w.caFile = filepath.Join(w.dir, "ca.pem")
	return os.WriteFile(w.caFile, ca.pem, 0o600)
}

func (w *sslWorld) connect(validate, check string) error {
	v, c := validate == "true", check == "true"
	// The same mapping the bindings use for the sslValidate and sslCheckHostname options.
	cfg := &config.SSLConfig{
		Enabled:            true,
		CAPath:             w.caFile,
		HostVerification:   v && c,
		InsecureSkipVerify: !v,
	}
	tlsCfg, err := db.NewTLSConfig(cfg, "127.0.0.1")
	if err != nil {
		return err
	}
	dialer := &tls.Dialer{Config: tlsCfg, NetDialer: &net.Dialer{Timeout: 5 * time.Second}}
	conn, err := dialer.DialContext(context.Background(), "tcp", w.listener.Addr().String())
	if err == nil {
		_ = conn.Close()
	}
	w.err = err
	return nil
}

func (w *sslWorld) outcome(want string) error {
	switch want {
	case "succeeds":
		if w.err != nil {
			return fmt.Errorf("expected the handshake to succeed: %v", w.err)
		}
	case "fails":
		if w.err == nil {
			return fmt.Errorf("expected the handshake to fail")
		}
	}
	return nil
}

func TestSSLValidate_BDD(t *testing.T) {
	suite := godog.TestSuite{
		Name: "ssl-validate",
		ScenarioInitializer: func(ctx *godog.ScenarioContext) {
			w := &sslWorld{}
			ctx.Before(func(c context.Context, _ *godog.Scenario) (context.Context, error) {
				*w = sslWorld{dir: t.TempDir(), cas: map[string]*testCA{}}
				return c, nil
			})
			ctx.After(func(c context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
				if w.listener != nil {
					_ = w.listener.Close()
				}
				return c, nil
			})
			ctx.Step(`^a TLS server with a certificate signed by "([^"]*)" that is valid for "([^"]*)"$`, w.serverWith)
			ctx.Step(`^the CA file holds the "([^"]*)" CA$`, w.caFileHolds)
			ctx.Step(`^I connect to it as 127\.0\.0\.1 with validate "([^"]*)" and check_hostname "([^"]*)"$`, w.connect)
			ctx.Step(`^the handshake (succeeds|fails)$`, w.outcome)
		},
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"../features/ssl-validate.feature"},
			TestingT: t,
			Strict:   true,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("BDD scenarios failed — see output above for details")
	}
}
