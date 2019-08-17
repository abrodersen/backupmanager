#!/usr/bin/env bash
set -e

gpg2 --batch --gen-key <<EOF
%no-protection
Key-Type:1
Key-Length:2048
Subkey-Type:1
Subkey-Length:2048
Name-Real: Brodersen Backup Key
Name-Email: admin@abrodersen.com
Expire-Date:0
EOF
