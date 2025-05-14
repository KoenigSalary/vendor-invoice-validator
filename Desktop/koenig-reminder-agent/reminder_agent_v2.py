
import requests
import msal
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
TENANT_ID = os.getenv('TENANT_ID')
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
SCOPE = ['https://graph.microsoft.com/.default']

app = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
result = app.acquire_token_for_client(scopes=SCOPE)

if 'access_token' not in result:
    print("❌ Failed to authenticate:", result.get('error_description'))
    exit(1)

access_token = result['access_token']
headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}

response = requests.get('https://graph.microsoft.com/v1.0/me/mailFolders/Inbox/messages?$filter=isRead eq false', headers=headers)
messages = response.json().get('value', [])

print(f"📩 Found {len(messages)} unread emails")

for msg in messages:
    subject = msg['subject']
    body_preview = msg['body']['content'].lower()
    sender_email = msg['from']['emailAddress']['address']

    if 'working' in body_preview or 'not done' in body_preview or 'pending' in body_preview:
        reply_text = "Thank you for your update. By when do you expect to complete this task? Kindly provide a deadline and make sure you adhere to it."
    elif 'will complete' in body_preview or 'by deadline' in body_preview:
        reply_text = "Thanks for your revert. Please inform me once the task is completed."
    else:
        continue

    send_data = {
        "message": {
            "subject": f"Re: {subject}",
            "body": {"contentType": "Text", "content": reply_text},
            "toRecipients": [{"emailAddress": {"address": sender_email}}]
        },
        "saveToSentItems": "true"
    }

    send_response = requests.post('https://graph.microsoft.com/v1.0/me/sendMail', headers=headers, json=send_data)
    if send_response.status_code == 202:
        print(f"✅ Auto-reply sent to {sender_email}")
    else:
        print(f"❌ Failed to reply to {sender_email}: {send_response.text}")
