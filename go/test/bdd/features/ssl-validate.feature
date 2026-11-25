# Behaviour of the TLS setting built from the SSL connect options and from
# cqlshrc [ssl] validate and check_hostname. As in cqlsh, validate = true checks
# the certificate chain against the CA and not the name of the server; the name
# is checked only when check_hostname is on.

Feature: Certificate validation without a hostname check
  As an application connecting to Cassandra nodes by IP address
  I want validate = true to check the certificate chain only
  So that a certificate without an IP SAN is accepted, as cqlsh accepts it

  Scenario Outline: a server certificate is checked against the CA and, if asked, the host
    Given a TLS server with a certificate signed by "<signer>" that is valid for "<names>"
    And the CA file holds the "trusted" CA
    When I connect to it as 127.0.0.1 with validate "<validate>" and check_hostname "<check>"
    Then the handshake <outcome>

    Examples:
      | signer  | names            | validate | check | outcome   |
      | trusted | DNS:other.local  | true     | false | succeeds  |
      | trusted | IP:10.0.0.10     | true     | false | succeeds  |
      | trusted | IP:127.0.0.1     | true     | false | succeeds  |
      | trusted | DNS:other.local  | true     | true  | fails     |
      | trusted | IP:10.0.0.10     | true     | true  | fails     |
      | trusted | IP:127.0.0.1     | true     | true  | succeeds  |
      | trusted | DNS:other.local  | false    | false | succeeds  |
      | trusted | DNS:other.local  | false    | true  | succeeds  |
      | rogue   | IP:127.0.0.1     | true     | false | fails     |
      | rogue   | IP:127.0.0.1     | true     | true  | fails     |
      | rogue   | IP:127.0.0.1     | false    | false | succeeds  |
