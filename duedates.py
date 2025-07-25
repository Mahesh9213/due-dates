import os
import json
import logging
import re
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from base64 import urlsafe_b64decode
import pytz
import csv
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def auto_authenticate_primary_gmail():
    """Authenticates with primary Gmail account (token.json)"""
    print("\n=== Authenticating Primary Gmail Account ===")
    creds = None
    token_path = 'token.json'
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    
    if os.path.exists(token_path):
        print("Found primary token file, loading credentials...")
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            if creds.expired and creds.refresh_token:
                print("Primary credentials expired, refreshing...")
                creds.refresh(Request())
        except Exception as e:
            print(f"Error loading primary credentials: {e}")
            creds = None
    
    if not creds or not creds.valid:
        print("No valid primary credentials found, initiating OAuth flow...")
        try:
            flow = InstalledAppFlow.from_client_secrets_file('client.json', SCOPES)
            creds = flow.run_local_server(port=8080)
            token_data = json.loads(creds.to_json())
            token_data['creation_time'] = datetime.now(pytz.UTC).isoformat()
            with open(token_path, 'w') as token:
                json.dump(token_data, token)
            print("Primary authentication successful! Token saved.")
        except Exception as e:
            print(f"Primary authentication failed: {e}")
            raise
    
    try:
        print("Building primary Gmail service...")
        gmail_service = build('gmail', 'v1', credentials=creds)
        print("Primary Gmail service ready!")
        return gmail_service
    except Exception as e:
        print(f"Failed to build primary Gmail service: {e}")
        raise

def auto_authenticate_secondary_gmail():
    """Authenticates with secondary Gmail account (token1.json)"""
    print("\n=== Authenticating Secondary Gmail Account ===")
    creds = None
    token_path = 'token1.json'
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    
    if os.path.exists(token_path):
        print("Found secondary token file, loading credentials...")
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            if creds.expired and creds.refresh_token:
                print("Secondary credentials expired, refreshing...")
                creds.refresh(Request())
        except Exception as e:
            print(f"Error loading secondary credentials: {e}")
            creds = None
    
    if not creds or not creds.valid:
        print("No valid secondary credentials found, initiating OAuth flow...")
        try:
            flow = InstalledAppFlow.from_client_secrets_file('client1.json', SCOPES)
            creds = flow.run_local_server(port=8081)
            token_data = json.loads(creds.to_json())
            token_data['creation_time'] = datetime.now(pytz.UTC).isoformat()
            with open(token_path, 'w') as token:
                json.dump(token_data, token)
            print("Secondary authentication successful! Token saved.")
        except Exception as e:
            print(f"Secondary authentication failed: {e}")
            raise
    
    try:
        print("Building secondary Gmail service...")
        gmail_service = build('gmail', 'v1', credentials=creds)
        print("Secondary Gmail service ready!")
        return gmail_service
    except Exception as e:
        print(f"Failed to build secondary Gmail service: {e}")
        raise

def get_recent_emails(service, max_results=30):
    """Fetches the most recent emails from the inbox"""
    print(f"\n=== Fetching {max_results} most recent emails ===")
    try:
        results = service.users().messages().list(
            userId="me",
            labelIds=['INBOX'],
            maxResults=max_results
        ).execute()
        messages = results.get('messages', [])
        print(f"Found {len(messages)} emails in inbox")
        return messages
    except Exception as e:
        print(f"Error fetching emails: {e}")
        return []

def decode_base64(data):
    """Decodes base64 email content with proper padding"""
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    return urlsafe_b64decode(data)

def extract_email_body(service, message_id):
    """Extracts and decodes the email body content"""
    try:
        msg = service.users().messages().get(
            userId="me",
            id=message_id,
            format='full'
        ).execute()
        
        payload = msg.get('payload', {})
        
        # Extract body from payload
        body = ""
        if 'body' in payload and 'data' in payload['body']:
            body = decode_base64(payload['body']['data']).decode('utf-8', errors='ignore')
        elif 'parts' in payload:
            for part in payload['parts']:
                if part.get('mimeType') in ['text/plain', 'text/html']:
                    if 'body' in part and 'data' in part['body']:
                        body = decode_base64(part['body']['data']).decode('utf-8', errors='ignore')
                        break
                elif 'parts' in part:
                    for subpart in part['parts']:
                        if subpart.get('mimeType') in ['text/plain', 'text/html']:
                            if 'body' in subpart and 'data' in subpart['body']:
                                body = decode_base64(subpart['body']['data']).decode('utf-8', errors='ignore')
                                break
        
        return body if body else None
    except Exception as e:
        print(f"Error processing email {message_id}: {e}")
        return None

def extract_job_details(body):
    """Enhanced version specifically targeting Job ID fields in emails"""
    job_data = {
        'Title': None,
        'Job_ID': None,
        'Due_date': None
    }
    
    if not body:
        return job_data
    
    # Title extraction pattern (unchanged)
    title_pattern = r'(?i)((?:Hybrid|Onsite|Remote)(?:\s*/\s*Local)?[^(]*\(.*?\)|(?:Hybrid|Onsite|Remote)(?:\s*/\s*Local)?.*?)(?=\s+with\s+|\s*\(|$|\n)'
    title_match = re.search(title_pattern, body)
    if title_match:
        job_data['Title'] = title_match.group(1).strip()
    
    # NEW: Focused Job ID extraction - looks for specific patterns indicating a Job ID field
    job_id_patterns = [
        r'(?i)(?:job\s*|req\s*|position\s*)?(?:id\s*|#\s*|num\s*|number\s*)[:=\s]*([A-Za-z]{2,}-?\d{3,})',  # "Job ID: VA-123456"
        r'(?i)(?:job\s*|req\s*|position\s*)?(?:id\s*|#\s*|num\s*|number\s*)[:=\s]*([A-Za-z]{2,}\s*\d{3,})',  # "Job ID VA 123456"
        r'(?i)\b(?:job\s*|req\s*|position\s*)?(?:id\s*|#\s*|num\s*|number\s*)\b\s*[:-]?\s*([A-Za-z]{2,}-?\d{3,})',  # "Job-ID:VA-123456"
        r'(?<!\w)([A-Za-z]{2,}-?\d{3,})(?!\w)',  # Standalone ID as last resort
    ]
    
    for pattern in job_id_patterns:
        id_match = re.search(pattern, body)
        if id_match:
            job_id = id_match.group(1).strip()
            # Clean up the job ID (remove spaces, normalize format)
            job_id = re.sub(r'\s+', '', job_id)  # Remove any spaces
            job_id = job_id.upper()  # Convert to uppercase for consistency
            job_data['Job_ID'] = job_id
            break
    
    # Date extraction only if we found a Job ID
    if job_data['Job_ID']:
        # Look for dates in parentheses after Job ID
        date_match = re.search(
            r'{}\s*\((\d+)\)'.format(re.escape(job_data['Job_ID'])),
            body
        )
        if date_match:
            full_number = date_match.group(1)
            last_four = full_number[-4:]
            if len(last_four) == 4:
                job_data['Due_date'] = f"{last_four[:2]}/{last_four[2:]}"
    
    return job_data

def count_emails_for_job_id(service, job_id):
    """Count emails containing the job ID in the inbox."""
    try:
        query = f'"{job_id}"'  # Using quotes for exact matching
        results = service.users().messages().list(
            userId="me",
            q=query,
            labelIds=['INBOX']
        ).execute()
        return results.get('resultSizeEstimate', 0)
    except Exception as e:
        logging.error(f"Error counting emails for {job_id}: {e}")
        return 0

def process_job_ids(csv_path, service):
    """Process job IDs from CSV and update with email counts."""
    try:
        with open(csv_path, 'r') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            fieldnames = reader.fieldnames
            
        if 'No_of_emails' not in fieldnames:
            fieldnames.append('No_of_emails')
            for row in rows:
                row['No_of_emails'] = '0'
    
        for row in rows:
            job_id = row.get('Job ID', '').strip() or row.get('Job_ID', '').strip()
            if job_id:
                logging.info(f"Searching for emails with job ID: {job_id}")
                email_count = count_emails_for_job_id(service, job_id)
                row['No_of_emails'] = str(email_count)
                logging.info(f"Found {email_count} emails for {job_id}")
                time.sleep(0.5)  # Rate limiting
    
        with open(csv_path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    
        logging.info(f"Successfully updated CSV file: {csv_path}")
        
    except Exception as e:
        logging.error(f"Error processing CSV file: {e}")
        raise

def send_results_email(excel_path, recipient_email):
    """Send the results Excel file as an email attachment"""
    try:
        # Email configuration - REPLACE THESE WITH YOUR DETAILS
        sender_email = "gorintalakavya@gmail.com"
        password = "tihr qpwm pwwv dtzf"  # Use App Password if 2FA is enabled
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        
        # Create message container
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = "Job Application Tracker Results"
        
        # Email body
        body = f"""Please find attached the latest job application tracking results.
        
        Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        This file contains:
        - Job Titles
        - Job IDs
        - Due Dates
        - Email Counts
        """
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach Excel file
        with open(excel_path, 'rb') as file:
            part = MIMEApplication(file.read(), Name="Job_Tracker_Report.xlsx")
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(excel_path)}"'
        msg.attach(part)
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        
        print(f"\nResults successfully emailed to {recipient_email}")
        
    except Exception as e:
        print(f"\nFailed to send email: {str(e)}")

def main():
    """Main function to process both Gmail accounts"""
    try:
        # Initialize results table
        results = []
        
        # Step 1: Authenticate with primary Gmail (extraction account)
        primary_service = auto_authenticate_primary_gmail()
        
        # Step 2: Get recent emails from primary account
        messages = get_recent_emails(primary_service, max_results=25)
        
        if not messages:
            print("No emails found in primary inbox")
            return
        
        # Step 3: Process each email from primary account
        print("\n=== Processing Emails from Primary Account ===")
        for i, message in enumerate(messages, 1):
            message_id = message['id']
            print(f"\nProcessing Email #{i} (ID: {message_id})")
            
            body = extract_email_body(primary_service, message_id)
            
            if body:
                job_details = extract_job_details(body)
                job_details['Email ID'] = message_id  # Temporarily keep for processing
                results.append(job_details)
                
                print(f"Title: {job_details['Title']}")
                print(f"Job_ID: {job_details['Job_ID']}")
                print(f"Due_date: {job_details['Due_date']}")
            else:
                print("No body content could be extracted")
        
        # Create DataFrame and filter rows where Title is missing
        df = pd.DataFrame(results)
        df = df.dropna(subset=['Title'])  # Remove rows with missing Title
        
        if not df.empty:
            # Create temporary CSV for secondary account processing
            temp_csv = 'temp_duedates.csv'
            df = df.drop(columns=['Email ID'], errors='ignore')
            df.to_csv(temp_csv, index=False)
            
            # Step 4: Authenticate with secondary Gmail (counting account)
            secondary_service = auto_authenticate_secondary_gmail()
            
            # Step 5: Process CSV with secondary account to add No_of_emails
            process_job_ids(temp_csv, secondary_service)
            
            # Load the final data
            final_df = pd.read_csv(temp_csv)
            
            # Remove temporary CSV
            os.remove(temp_csv)
            
            # Display final table
            print("\n" + "="*100)
            print("FINAL RESULTS".center(100))
            print("="*100)
            
            # Configure display options
            pd.set_option('display.max_colwidth', 40)
            pd.set_option('display.width', 120)
            pd.set_option('display.colheader_justify', 'center')
            
            # Create formatted table
            table = final_df.to_markdown(
                tablefmt="grid",
                stralign="left",
                numalign="left",
                index=False
            )
            
            # Add left margin to each line
            margined_table = [f"    {line}" for line in table.split('\n')]
            print('\n'.join(margined_table))
            print("="*100)
            
            # Save to Excel with formatting
            excel_path = 'duedates_formatted.xlsx'
            try:
                import xlsxwriter
                writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
                final_df.to_excel(writer, index=False, sheet_name='Job Details')
                
                workbook = writer.book
                worksheet = writer.sheets['Job Details']
                
                # Formatting
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'align': 'center',
                    'border': 1
                })
                
                cell_format = workbook.add_format({
                    'text_wrap': True,
                    'valign': 'top',
                    'align': 'left',
                    'border': 1
                })
                
                # Apply formatting
                for col_num, value in enumerate(final_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                for row in range(1, len(final_df)+1):
                    for col in range(len(final_df.columns)):
                        worksheet.write(row, col, str(final_df.iloc[row-1, col]), cell_format)
                
                # Auto-adjust column widths
                for i, col in enumerate(final_df.columns):
                    max_len = max((
                        final_df[col].astype(str).map(len).max(),
                        len(col)
                    )) + 2
                    worksheet.set_column(i, i, max_len)
                
                writer.close()
                print(f"\nFinal results saved to '{excel_path}'")
                  
                # Send email with results - REPLACE WITH ACTUAL RECIPIENT
                send_results_email(excel_path, "rtr1@googlegroups.com")
                
            except ImportError:
                print("\nError: xlsxwriter not installed - cannot create Excel file")
                print("Install with: pip install xlsxwriter")
                # Fallback to CSV if Excel fails
                csv_path = 'duedates.csv'
                final_df.to_csv(csv_path, index=False)
                print(f"Results saved to '{csv_path}'")
            
        else:
            print("\nNo valid job details found (all rows filtered out)")
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()