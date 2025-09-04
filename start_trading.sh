#!/bin/bash
set -e
cd /home/ubuntu/trading-bot

# activate venv
source /home/ubuntu/trading-bot/venv/bin/activate

# 1) Refresh token (writes FYERS_ACCESS_TOKEN to .env)
echo "[$(date)] Starting token refresh..." >> trading.log
/usr/bin/python3 get_auto_token.py >> trading.log 2>&1

# 2) small buffer
sleep 5

# 3) Start Sensex strategy in background
echo "[$(date)] Starting Sensex strategy..." >> trading.log
nohup /usr/bin/python3 sensex_complete.py >> sensex.log 2>&1 &
echo "[$(date)] Launched." >> trading.log
