from flask import Flask, render_template, send_file, redirect, url_for, flash, jsonify
import os
import pandas as pd
import threading
import logging
from duedates import auto_authenticate_primary_gmail, auto_authenticate_secondary_gmail, get_recent_emails, extract_email_body, extract_job_details, process_job_ids, send_results_email

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Replace with a secure key

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Global variables to track processing status, completion, and progress
processing = False
results_file = None
process_completed = False
progress_state = []

def process_emails_background():
    """Background task to process emails and generate results"""
    global processing, results_file, process_completed, progress_state
    progress_state = ["Accessing Gmails..."]
    try:
        logging.info("Starting background email processing")
        results = []
        
        # Authenticate primary Gmail
        primary_service = auto_authenticate_primary_gmail()
        progress_state = ["Processing inside emails..."]
        
        # Get recent emails
        messages = get_recent_emails(primary_service, max_results=25)
        
        if not messages:
            logging.warning("No emails found in primary inbox")
            return
        
        # Process emails
        progress_state = ["Extracting email body data..."]
        for i, message in enumerate(messages, 1):
            message_id = message['id']
            logging.info(f"Processing Email #{i} (ID: {message_id})")
            body = extract_email_body(primary_service, message_id)
            if body:
                job_details = extract_job_details(body)
                job_details['Email ID'] = message_id
                results.append(job_details)
        
        # Create DataFrame and filter
        df = pd.DataFrame(results)
        df = df.dropna(subset=['Title'])
        
        if not df.empty:
            # Save to temporary CSV
            temp_csv = 'temp_duedates.csv'
            df = df.drop(columns=['Email ID'], errors='ignore')
            df.to_csv(temp_csv, index=False)
            
            # Authenticate secondary Gmail and process email counts
            secondary_service = auto_authenticate_secondary_gmail()
            progress_state = ["Counting no of submissions using job IDs..."]
            process_job_ids(temp_csv, secondary_service)
            
            # Load final data
            final_df = pd.read_csv(temp_csv)
            os.remove(temp_csv)
            
            # Save to Excel with formatting
            excel_path = 'duedates_formatted.xlsx'
            try:
                import xlsxwriter
                writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
                final_df.to_excel(writer, index=False, sheet_name='Job Details')
                
                workbook = writer.book
                worksheet = writer.sheets['Job Details']
                
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
                
                for col_num, value in enumerate(final_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                for row in range(1, len(final_df)+1):
                    for col in range(len(final_df.columns)):
                        worksheet.write(row, col, str(final_df.iloc[row-1, col]), cell_format)
                
                for i, col in enumerate(final_df.columns):
                    max_len = max((final_df[col].astype(str).map(len).max(), len(col))) + 2
                    worksheet.set_column(i, i, max_len)
                
                writer.close()
                results_file = excel_path
                logging.info(f"Results saved to {excel_path}")
                
                # Send email
                progress_state = ["Sending final output as email to receiver Gmail..."]
                send_results_email(excel_path, "rtr1@googlegroups.com")
                
            except ImportError as e:
                logging.error("xlsxwriter not installed, falling back to CSV")
                csv_path = 'duedates.csv'
                final_df.to_csv(csv_path, index=False)
                results_file = csv_path
                logging.info(f"Results saved to {csv_path}")
                
        else:
            logging.warning("No valid job details found")
        
    except Exception as e:
        logging.error(f"Error in background processing: {e}")
        flash(f"Error during processing: {str(e)}", "error")
    finally:
        processing = False
        progress_state = []  # Clear progress state after completion
        process_completed = True if results_file and os.path.exists(results_file) else False

@app.route('/')
def index():
    """Render the main dashboard"""
    global results_file
    data = []
    if results_file and os.path.exists(results_file):
        try:
            if results_file.endswith('.xlsx'):
                df = pd.read_excel(results_file)
            elif results_file.endswith('.csv'):
                df = pd.read_csv(results_file)
            else:
                raise ValueError("Unsupported file format")
            data = df.to_dict('records')
        except Exception as e:
            logging.error(f"Error reading results file: {e}")
            flash("Error loading results file, falling back to no data", "error")
    
    return render_template('index.html', data=data, processing=processing, process_completed=process_completed, progress_state=progress_state)

@app.route('/process_emails')
def process_emails():
    """Start email processing in the background"""
    global processing, process_completed, progress_state
    if not processing:
        processing = True
        process_completed = False
        progress_state = ["Accessing Gmails..."]  # Initial progress state
        threading.Thread(target=process_emails_background, daemon=True).start()
    else:
        flash("Processing already in progress", "warning")
    return redirect(url_for('index'))

@app.route('/download')
def download_file():
    """Download the results file"""
    global results_file
    if results_file and os.path.exists(results_file):
        return send_file(results_file, as_attachment=True)
    flash("No results file available", "error")
    return redirect(url_for('index'))

@app.route('/status')
def status():
    """Check processing status and return latest data"""
    global processing, results_file, process_completed, progress_state
    data = []
    if results_file and os.path.exists(results_file):
        try:
            if results_file.endswith('.xlsx'):
                df = pd.read_excel(results_file)
            elif results_file.endswith('.csv'):
                df = pd.read_csv(results_file)
            else:
                raise ValueError("Unsupported file format")
            data = df.to_dict('records')
        except Exception as e:
            logging.error(f"Error reading results file: {e}")
            return jsonify({'processing': processing, 'data': [], 'error': str(e), 'completed': process_completed, 'progress': progress_state})
    
    return jsonify({'processing': processing, 'data': data, 'completed': process_completed, 'progress': progress_state})

if __name__ == '__main__':
    app.run(debug=True, port=5000)