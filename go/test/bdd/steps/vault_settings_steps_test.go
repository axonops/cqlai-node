package steps_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cucumber/godog"

	"github.com/axonops/cqlai-node/internal/db"
	"github.com/axonops/cqlai-node/internal/vault"
)

// settingsWorld drives vault.FromSettings on top of the cqlai vault world and
// its fake server.
type settingsWorld struct {
	*vaultWorld
	settings   vault.Settings
	mu         sync.Mutex
	namespaces []string
}

// recordNamespaces wraps the fake server so the namespace header of each
// request is kept.
func (w *settingsWorld) recordNamespaces() {
	inner := w.fake.server.Config.Handler
	w.fake.server.Config.Handler = http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.mu.Lock()
		w.namespaces = append(w.namespaces, r.Header.Get("X-Vault-Namespace"))
		w.mu.Unlock()
		inner.ServeHTTP(rw, r)
	})
}

func (w *settingsWorld) optionsWithToken(token string) error {
	w.settings = vault.Settings{Addr: w.fake.server.URL, Token: token}
	return nil
}

func (w *settingsWorld) optionsNoToken() error {
	w.settings = vault.Settings{Addr: w.fake.server.URL}
	return nil
}

func (w *settingsWorld) optionsOnlyToken(token string) error {
	w.settings = vault.Settings{Token: token}
	return nil
}

func (w *settingsWorld) optionsBlank(addr, token string) error {
	w.settings = vault.Settings{Addr: addr, Token: token}
	return nil
}

func (w *settingsWorld) optionsWithNamespace(token, namespace string) error {
	w.settings = vault.Settings{Addr: w.fake.server.URL, Token: token, Namespace: namespace}
	return nil
}

func (w *settingsWorld) optionsWithCACert(token, caCert string) error {
	w.settings = vault.Settings{Addr: w.fake.server.URL, Token: token, CACert: filepath.Join(w.home, caCert)}
	return nil
}

func (w *settingsWorld) optionsWithCACertContent(token, content string) error {
	path := filepath.Join(w.home, "ca.pem")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return err
	}
	w.settings = vault.Settings{Addr: w.fake.server.URL, Token: token, CACert: path}
	return nil
}

func (w *settingsWorld) envUnreachable(token string) error {
	port, err := closedPort()
	if err != nil {
		return err
	}
	if err := os.Setenv("VAULT_ADDR", fmt.Sprintf("http://127.0.0.1:%d", port)); err != nil {
		return err
	}
	return os.Setenv("VAULT_TOKEN", token)
}

func (w *settingsWorld) envVar(name, value string) error {
	return os.Setenv(name, value)
}

func (w *settingsWorld) readWithOptions(path string) error {
	w.client, w.err = vault.FromSettings(w.settings)
	if w.err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.creds, w.err = w.client.ReadCredentials(ctx, path)
	return nil
}

func (w *settingsWorld) openSessionWithOptions(path string) error {
	port, err := closedPort()
	if err != nil {
		return err
	}
	sess, err := db.NewSessionWithOptions(db.SessionOptions{
		Host:           "127.0.0.1",
		Port:           port,
		BatchMode:      true,
		ConnectTimeout: 1,
		RequestTimeout: 1,
		VaultPath:      path,
		Vault:          w.settings,
	})
	if sess != nil {
		sess.Close()
	}
	w.sessionErr = err
	return nil
}

func (w *settingsWorld) sawNamespace(namespace string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.namespaces) == 0 {
		return fmt.Errorf("server saw no request")
	}
	for _, ns := range w.namespaces {
		if ns != namespace {
			return fmt.Errorf("server saw namespace %q, want %q", ns, namespace)
		}
	}
	return nil
}

func initVaultSettingsScenario(t *testing.T) func(*godog.ScenarioContext) {
	return func(ctx *godog.ScenarioContext) {
		w := &settingsWorld{vaultWorld: &vaultWorld{}}
		saved := map[string]*string{}

		ctx.Before(func(c context.Context, _ *godog.Scenario) (context.Context, error) {
			*w.vaultWorld = vaultWorld{home: t.TempDir()}
			w.settings = vault.Settings{}
			w.namespaces = nil
			for _, name := range append(vaultEnvNames, "HOME") {
				if v, ok := os.LookupEnv(name); ok {
					saved[name] = &v
				} else {
					saved[name] = nil
				}
				_ = os.Unsetenv(name)
			}
			return c, os.Setenv("HOME", w.home)
		})
		ctx.After(func(c context.Context, _ *godog.Scenario, err error) (context.Context, error) {
			vault.RevokeAll()
			if w.fake != nil {
				w.fake.server.Close()
			}
			for name, v := range saved {
				if v == nil {
					_ = os.Unsetenv(name)
				} else {
					_ = os.Setenv(name, *v)
				}
			}
			return c, nil
		})

		ctx.Step(`^a fake Vault server that issues user "([^"]*)" with password "([^"]*)" and lease "([^"]*)" at "([^"]*)"$`,
			func(username, password, leaseID, path string) error {
				if err := w.fakeServer(username, password, leaseID, path); err != nil {
					return err
				}
				w.recordNamespaces()
				return nil
			})
		ctx.Step(`^the environment points (VAULT|BAO)_ADDR at the fake server with (?:VAULT|BAO)_TOKEN "([^"]*)"$`, w.envPointsAt)
		ctx.Step(`^the environment points VAULT_ADDR at an unreachable server with VAULT_TOKEN "([^"]*)"$`, w.envUnreachable)
		ctx.Step(`^the environment has (VAULT_TOKEN|VAULT_NAMESPACE) "([^"]*)"$`, w.envVar)
		ctx.Step(`^the file ~/\.vault-token contains "([^"]*)"$`, w.tokenFile)

		ctx.Step(`^the options name the fake server with token "([^"]*)"$`, w.optionsWithToken)
		ctx.Step(`^the options name the fake server with no token$`, w.optionsNoToken)
		ctx.Step(`^the options give only the token "([^"]*)"$`, w.optionsOnlyToken)
		ctx.Step(`^the options give the address "([^"]*)" and the token "([^"]*)"$`, w.optionsBlank)
		ctx.Step(`^the options name the fake server with token "([^"]*)" and namespace "([^"]*)"$`, w.optionsWithNamespace)
		ctx.Step(`^the options name the fake server with token "([^"]*)" and CA certificate "([^"]*)"$`, w.optionsWithCACert)
		ctx.Step(`^the options name the fake server with token "([^"]*)" and a CA certificate file containing "([^"]*)"$`, w.optionsWithCACertContent)

		ctx.Step(`^I read credentials from "([^"]*)" with the options$`, w.readWithOptions)
		ctx.Step(`^I open a session to an unreachable cluster with Vault path "([^"]*)" and the options$`, w.openSessionWithOptions)

		ctx.Step(`^the credentials have username "([^"]*)" and password "([^"]*)"$`, w.credsAre)
		ctx.Step(`^the fake server saw the token "([^"]*)"$`, w.sawToken)
		ctx.Step(`^the fake server saw the namespace "([^"]*)"$`, w.sawNamespace)
		ctx.Step(`^the fake server was asked to revoke lease "([^"]*)" once$`, w.revokedOnce)
		ctx.Step(`^reading fails with an error containing "([^"]*)"$`, w.failsWith)
		ctx.Step(`^the fake server received no read request$`, w.noReads)
		ctx.Step(`^opening the session fails$`, w.sessionFails)
		ctx.Step(`^the fake server received (\d+) read requests?$`, w.readCount)
	}
}

func TestVaultSettings_BDD(t *testing.T) {
	suite := godog.TestSuite{
		Name:                "vault-settings",
		ScenarioInitializer: initVaultSettingsScenario(t),
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"../features/vault-settings.feature"},
			TestingT: t,
			Strict:   true,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("BDD scenarios failed — see output above for details")
	}
}
