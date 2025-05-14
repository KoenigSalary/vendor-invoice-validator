
# Koenig Reminder Agent V2

Automated Reminder Agent for Koenig Solutions - Tracks task status, sends follow-up reminders, and auto-replies to emails using Microsoft 365 (Outlook).

## Features
- Reads unread Outlook 365 emails via Microsoft Graph API.
- Auto-replies to status updates based on simple keywords.
- Can be scheduled to run every 10 minutes via GitHub Actions.

## Setup Instructions

### 1. Environment Variables
Copy `.env.template` to `.env` and fill in your Azure App credentials.

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run Locally
```bash
python reminder_agent_v2.py
```

### 4. GitHub Actions Automation
- Add GitHub Secrets: CLIENT_ID, CLIENT_SECRET, TENANT_ID.
- Workflow file is already configured to run every 10 minutes.

## License
MIT
