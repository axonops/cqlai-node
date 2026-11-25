package main

import (
	"os"
	"path/filepath"
	"testing"
)

func boolPtr(b bool) *bool { return &b }

func TestSSLConfigFor(t *testing.T) {
	tests := []struct {
		name          string
		validate      *bool
		checkHostname *bool
		wantHost      bool
		wantSkip      bool
	}{
		{"default checks the chain only", nil, nil, false, false},
		{"validate true checks the chain only", boolPtr(true), nil, false, false},
		{"validate true with check_hostname", boolPtr(true), boolPtr(true), true, false},
		{"validate false skips verification", boolPtr(false), nil, false, true},
		{"validate false ignores check_hostname", boolPtr(false), boolPtr(true), false, true},
		{"check_hostname alone validates by default", nil, boolPtr(true), true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sslConfigFor(&SessionOptions{SSLValidate: tt.validate, SSLCheckHostname: tt.checkHostname})
			if got.HostVerification != tt.wantHost || got.InsecureSkipVerify != tt.wantSkip {
				t.Errorf("HostVerification=%v InsecureSkipVerify=%v, want %v and %v",
					got.HostVerification, got.InsecureSkipVerify, tt.wantHost, tt.wantSkip)
			}
		})
	}
}

func TestParseCqlshrcCheckHostname(t *testing.T) {
	tests := []struct {
		name          string
		ssl           string
		wantValidate  bool
		wantCheckHost bool
	}{
		{"nothing set", "", true, false},
		{"validate true", "validate = true\n", true, false},
		{"validate false", "validate = false\n", false, false},
		{"check_hostname true", "validate = true\ncheck_hostname = true\n", true, true},
		{"check_hostname upper case", "check_hostname = TRUE\n", true, true},
		{"check_hostname false", "check_hostname = false\n", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cqlshrc")
			if err := os.WriteFile(path, []byte("[ssl]\n"+tt.ssl), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ParseCqlshrc(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.SSL.Validate != tt.wantValidate || cfg.SSL.CheckHostname != tt.wantCheckHost {
				t.Errorf("Validate=%v CheckHostname=%v, want %v and %v",
					cfg.SSL.Validate, cfg.SSL.CheckHostname, tt.wantValidate, tt.wantCheckHost)
			}
		})
	}
}
