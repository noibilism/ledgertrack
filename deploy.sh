#!/bin/bash

# Deployment Script for Ledger Service on Ubuntu

set -e

# Function to prompt for input with a default value
prompt() {
    local prompt_text=$1
    local default_value=$2
    local var_name=$3
    read -p "$prompt_text [$default_value]: " input
    if [ -z "$input" ]; then
        input="$default_value"
    fi
    printf -v "$var_name" '%s' "$input"
}

# Function to prompt for secret input
prompt_secret() {
    local prompt_text=$1
    local default_value=$2
    local var_name=$3
    read -s -p "$prompt_text [$default_value]: " input
    echo "" # New line since -s doesn't print one
    if [ -z "$input" ]; then
        input="$default_value"
    fi
    printf -v "$var_name" '%s' "$input"
}

echo "============================================"
echo "   Ledger Service Deployment Script"
echo "============================================"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit 1
fi

# 1. Install Dependencies
echo "--> Installing dependencies..."
apt-get update
apt-get install -y golang postgresql-client curl jq git make

# 2. Database Parameters
echo "--> Database Configuration"
prompt "Database Host" "localhost" DB_HOST
prompt "Database Port" "5432" DB_PORT
prompt "Database Name" "ledger" DB_NAME
prompt "Database User" "ledger" DB_USER
prompt_secret "Database Password" "ledger" DB_PASSWORD

# URL Encode the password to handle special characters
if command -v python3 &>/dev/null; then
    DB_PASSWORD_ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$DB_PASSWORD''', safe=''))")
elif command -v jq &>/dev/null; then
    DB_PASSWORD_ENCODED=$(jq -nr --arg v "$DB_PASSWORD" '$v|@uri')
else
    echo "Warning: Python3 or jq not found. Password encoding might fail for special characters."
    DB_PASSWORD_ENCODED=$DB_PASSWORD
fi

POSTGRES_URI="postgresql://$DB_USER:$DB_PASSWORD_ENCODED@$DB_HOST:$DB_PORT/$DB_NAME?sslmode=disable"

# 3. Supported Currencies
echo "--> Currency Configuration"
prompt "Supported Currencies (comma separated)" "USD,EUR,GBP" ALLOWED_CURRENCIES

# 4. Channels
echo "--> Channel Configuration"
echo "Enter channels to pre-create (format: 'Name:Currency')."
echo "Example: 'Provider A:USD'. Leave empty to finish."
CHANNELS=()
while true; do
    read -p "Channel (Name:Currency): " channel_input
    if [ -z "$channel_input" ]; then
        break
    fi
    CHANNELS+=("$channel_input")
done

# 5. Build Application
echo "--> Building application..."
if [ ! -f "go.mod" ]; then
    echo "Error: go.mod not found. Please run this script from the project root."
    exit 1
fi
go build -o ledger main.go

# 6. Create .env file
echo "--> Creating .env file..."
cat > .env <<EOF
POSTGRES_URI=$POSTGRES_URI
DEBUG=false
EXPERIMENTAL_FEATURES=true
AUTO_UPGRADE=true
ALLOWED_CURRENCIES=$ALLOWED_CURRENCIES
EOF

# 7. Setup Systemd Service
echo "--> Setting up Systemd service..."
SERVICE_FILE="/etc/systemd/system/ledger.service"
WORKING_DIR=$(pwd)

cat > $SERVICE_FILE <<EOF
[Unit]
Description=Ledger Service
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=$WORKING_DIR
ExecStart=$WORKING_DIR/ledger serve --bind 0.0.0.0:3068
Restart=on-failure
EnvironmentFile=$WORKING_DIR/.env

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ledger
echo "--> Starting service..."
systemctl restart ledger

# Wait for service to start
echo "Waiting for service to start..."
sleep 5

# 8. Create Channels
if [ ${#CHANNELS[@]} -gt 0 ]; then
    echo "--> Creating Channels..."
    for channel in "${CHANNELS[@]}"; do
        # Split Name and Currency
        NAME=$(echo $channel | cut -d':' -f1)
        CURRENCY=$(echo $channel | cut -d':' -f2)
        
        if [ -z "$NAME" ] || [ -z "$CURRENCY" ]; then
            echo "Skipping invalid channel format: $channel"
            continue
        fi

        echo "Creating Channel: $NAME ($CURRENCY)"
        response=$(curl -s -X POST http://localhost:3068/v2/ledgertrack/channels \
            -H "Content-Type: application/json" \
            -d "{\"currency\": \"$CURRENCY\", \"metadata\": {\"name\": \"$NAME\"}}")
        
        echo "Response: $response"
    done
fi

echo "============================================"
echo "   Deployment Complete!"
echo "   Service is running on port 3068"
echo "============================================"
