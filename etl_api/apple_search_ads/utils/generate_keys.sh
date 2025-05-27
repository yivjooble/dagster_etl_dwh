#!/bin/bash

# Generate Apple Search Ads API key pair using OpenSSL
# This creates EC P-256 keys required by Apple

echo "🔑 Generating Apple Search Ads API key pair..."
echo ""

# Generate private key
openssl ecparam -genkey -name prime256v1 -noout -out private_key.pem

# Extract public key from private key
openssl ec -in private_key.pem -pubout -out public_key.pem

echo "✅ Keys generated successfully!"
echo ""
echo "📋 PUBLIC KEY (Upload this to Apple Search Ads):"
echo "=============================================="
cat public_key.pem
echo "=============================================="
echo ""
echo "🔐 PRIVATE KEY (Use this in your code):"
echo "=============================================="
cat private_key.pem
echo "=============================================="
echo ""
echo "⚠️  Files created:"
echo "   - public_key.pem  (upload to Apple)"
echo "   - private_key.pem (use in your code)"
echo ""
echo "🚨 IMPORTANT: Keep the private key secure!"
