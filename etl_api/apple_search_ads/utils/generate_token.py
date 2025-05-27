#!/usr/bin/env python3
"""
Standalone script to generate Apple Search Ads JWT token
"""
import os
import datetime as dt
from authlib.jose import jwt
# Use cryptography library which is more widely available
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from utility_hub.core_tools import get_creds_from_vault

# Apple Search Ads credentials and configuration
private_key_file = "private-key.pem"
public_key_file = "public-key.pem"
client_id = get_creds_from_vault("APPLE_CLIENT_ID")
team_id = get_creds_from_vault("APPLE_TEAM_ID")
key_id = get_creds_from_vault("APPLE_KEY_ID")
audience = "https://appleid.apple.com"
alg = "ES256"

def main():
    # Create the private key if it doesn't already exist.
    private_key = None
    if os.path.isfile(private_key_file):
        print(f"Loading existing private key from {private_key_file}")
        with open(private_key_file, "rb") as file:
            private_key_data = file.read()
            private_key = serialization.load_pem_private_key(
                private_key_data,
                password=None,
                backend=default_backend()
            )
    else:
        print("Generating new private key...")
        private_key = ec.generate_private_key(
            curve=ec.SECP256R1(),  # P-256 curve
            backend=default_backend()
        )
        # Save private key in PEM format
        pem_private = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        with open(private_key_file, 'wb') as file:
            file.write(pem_private)
        print(f"Private key saved to {private_key_file}")

    # Extract and save the public key.
    public_key = private_key.public_key()
    if not os.path.isfile(public_key_file):
        # Save public key in PEM format
        pem_public = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        with open(public_key_file, 'wb') as file:
            file.write(pem_public)
        print(f"Public key saved to {public_key_file}")
    else:
        print(f"Public key already exists at {public_key_file}")

    # Define the issue timestamp.
    issued_at_timestamp = int(dt.datetime.utcnow().timestamp())
    # Define the expiration timestamp, which may not exceed 180 days from the issue timestamp.
    expiration_timestamp = issued_at_timestamp + 86400*180

    # Define the JWT headers.
    headers = dict()
    headers['alg'] = alg
    headers['kid'] = key_id

    # Define the JWT payload.
    payload = dict()
    payload['sub'] = client_id
    payload['aud'] = audience
    payload['iat'] = issued_at_timestamp
    payload['exp'] = expiration_timestamp
    payload['iss'] = team_id

    # Get private key in PEM format for JWT encoding
    pem_private = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Encode the JWT and sign it with the private key.
    client_secret = jwt.encode(
        header=headers,
        payload=payload,
        key=pem_private
    ).decode('UTF-8')

    # Save the client secret to a file.
    with open('client_secret.txt', 'w') as output:
        output.write(client_secret)
    
    print(f"JWT token generated and saved to client_secret.txt")
    print(f"Token valid until: {dt.datetime.fromtimestamp(expiration_timestamp)}")
    print("\nIMPORTANT: Upload the public key to Apple Search Ads console")
    print("and use this client_secret.txt for API authentication")

if __name__ == "__main__":
    main()
