# Behaviour of vault.FromSettings: the Vault or OpenBao server and token that
# the Node connect options name (vaultAddr, vaultToken, vaultNamespace,
# vaultCaCert), with the environment used for whatever they leave out. An
# application launched from a desktop has no shell exports to read, so it
# passes these itself.

Feature: Vault server and token passed with the connect options
  As an application using cqlai-node
  I want to give the Vault address and token with the connection
  So that Vault credentials work without VAULT_ADDR and VAULT_TOKEN set

  Background:
    Given a fake Vault server that issues user "v-token-reader-abc" with password "fake-password" and lease "database/creds/reader/lease1" at "database/creds/reader"

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  Scenario: the address and token come from the options alone
    Given the options name the fake server with token "hvs.option-token"
    When I read credentials from "database/creds/reader" with the options
    Then the credentials have username "v-token-reader-abc" and password "fake-password"
    And the fake server saw the token "hvs.option-token"

  Scenario: the options win over the environment
    Given the environment points VAULT_ADDR at an unreachable server with VAULT_TOKEN "hvs.env-token"
    And the options name the fake server with token "hvs.option-token"
    When I read credentials from "database/creds/reader" with the options
    Then the credentials have username "v-token-reader-abc" and password "fake-password"
    And the fake server saw the token "hvs.option-token"

  Scenario: a token left out of the options comes from the environment
    Given the environment has VAULT_TOKEN "hvs.env-token"
    And the options name the fake server with no token
    When I read credentials from "database/creds/reader" with the options
    Then the fake server saw the token "hvs.env-token"

  Scenario: a token left out of the options and the environment comes from ~/.vault-token
    Given the file ~/.vault-token contains "hvs.file-token"
    And the options name the fake server with no token
    When I read credentials from "database/creds/reader" with the options
    Then the fake server saw the token "hvs.file-token"

  Scenario: an address left out of the options comes from the environment
    Given the environment points VAULT_ADDR at the fake server with VAULT_TOKEN "hvs.env-token"
    And the options give only the token "hvs.option-token"
    When I read credentials from "database/creds/reader" with the options
    Then the fake server saw the token "hvs.option-token"

  Scenario: the namespace in the options is sent
    Given the environment has VAULT_NAMESPACE "env-ns"
    And the options name the fake server with token "hvs.option-token" and namespace "team-a"
    When I read credentials from "database/creds/reader" with the options
    Then the fake server saw the namespace "team-a"

  Scenario: a namespace left out of the options comes from the environment
    Given the environment has VAULT_NAMESPACE "env-ns"
    And the options name the fake server with token "hvs.option-token"
    When I read credentials from "database/creds/reader" with the options
    Then the fake server saw the namespace "env-ns"

  Scenario: an empty options value is the same as one left out
    Given the environment points VAULT_ADDR at the fake server with VAULT_TOKEN "hvs.env-token"
    And the options give the address "   " and the token "   "
    When I read credentials from "database/creds/reader" with the options
    Then the fake server saw the token "hvs.env-token"

  # ---------------------------------------------------------------------------
  # Invalid input and failures
  # ---------------------------------------------------------------------------

  Scenario: no address in the options or the environment
    Given the options give only the token "hvs.option-token"
    When I read credentials from "database/creds/reader" with the options
    Then reading fails with an error containing "set VAULT_ADDR or BAO_ADDR"

  Scenario: no token in the options, the environment or ~/.vault-token
    Given the options name the fake server with no token
    When I read credentials from "database/creds/reader" with the options
    Then reading fails with an error containing "set VAULT_TOKEN or BAO_TOKEN"

  Scenario: a CA certificate that does not exist
    Given the options name the fake server with token "hvs.option-token" and CA certificate "does-not-exist.pem"
    When I read credentials from "database/creds/reader" with the options
    Then reading fails with an error containing "reading the Vault CA certificate"
    And the fake server received no read request

  Scenario: a CA certificate file with no certificate in it
    Given the options name the fake server with token "hvs.option-token" and a CA certificate file containing "not a certificate"
    When I read credentials from "database/creds/reader" with the options
    Then reading fails with an error containing "no certificates in"
    And the fake server received no read request

  Scenario: a session given the options revokes its lease when it cannot connect
    Given the options name the fake server with token "hvs.option-token"
    When I open a session to an unreachable cluster with Vault path "database/creds/reader" and the options
    Then opening the session fails
    And the fake server received 1 read request
    And the fake server was asked to revoke lease "database/creds/reader/lease1" once
