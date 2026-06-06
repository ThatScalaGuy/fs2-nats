#!/usr/bin/env bash
#
# Regenerate the TLS test fixtures used by TlsIntegrationSpec.
#
# Produces, in this directory:
#   ca.crt                 - self-signed test CA (server ca_file + client trust)
#   server.crt/server.key  - server cert (SAN localhost,127.0.0.1), CA-signed
#   client.crt/client.key  - client cert, CA-signed (for mutual TLS)
#   truststore.p12         - PKCS12 holding the CA cert (client trust material)
#   client.p12             - PKCS12 holding the client key + cert (mTLS)
#
# These are throwaway test identities, safe to commit. Certs are long-lived
# (100y) so the fixtures do not rot. Keystore password is "password".
#
# Requires: openssl (>=1.1.1, for -addext) and keytool (JDK).
set -euo pipefail
cd "$(dirname "$0")"

DAYS=36500
PASS=password

# --- CA ---
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout ca.key -out ca.crt -days "$DAYS" \
  -subj "/CN=fs2-nats-test-ca"

# --- server cert (CA-signed, with SAN) ---
openssl req -newkey rsa:2048 -nodes \
  -keyout server.key -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$DAYS" \
  -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1')

# --- client cert (CA-signed) ---
openssl req -newkey rsa:2048 -nodes \
  -keyout client.key -out client.csr \
  -subj "/CN=fs2-nats-test-client"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days "$DAYS"

# --- JVM keystores ---
rm -f truststore.p12 client.p12
keytool -importcert -noprompt -alias ca -file ca.crt \
  -keystore truststore.p12 -storetype PKCS12 -storepass "$PASS"
openssl pkcs12 -export -inkey client.key -in client.crt -certfile ca.crt \
  -name client -out client.p12 -passout pass:"$PASS"

# --- cleanup intermediates ---
rm -f server.csr client.csr ca.srl

echo "Done. Generated CA, server/client certs, truststore.p12, client.p12."
