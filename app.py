from flask import Flask, render_template, send_file, redirect, url_for, flash, jsonify
import os
import pandas as pd
import threading
import logging
from duedates import (
    auto_authenticate_primary_gmail,
    auto_authenticate_secondary_gmail,
    get_recent_emails,
    extract_email_body,
    extract_job_details,
    process_job_ids,
    send_results_email
)

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'your-secret-key')  # Securely handled via env variable

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

processing = False
results_file = None
process_completed = False
progress_state = []


def process_emails_background():
    """Background processing logic"""
    global processing, results_file, process_completed, progress_state
    progress_state = ["Accessing Gmails..."]
    try:
        logging.info("Starting background email processing")
        results = []

        # Step 1: Primary Gmail Auth
        primary_service = auto_authenticate_primary_gmail()
        progress_state = ["Processing inside emails..."]
        messages = get_recent_emails(primary_service, max_results=25)

        if not messages:
            logging.warning("No emails found")
            return

        # Step 2: Process Emails
        progress_state = ["Extracting email body data..."]
        for i, msg in enumerate(messages, 1):
            body = extract_email_body(primary_service, msg['id'])
            if body:
                job_details = extract_job_details(body)
                job_details['Email ID'] = msg['id']
                results.append(job_details)

        df = pd.DataFrame(results).dropna(subset=['Title'])

        if not df.empty:
            temp_csv = 'temp_duedates.csv'
            df.drop(columns=['Email ID'], errors='ignore').to_csv(temp_csv, index=False)

            # Step 3: Secondary Gmail Auth
            secondary_service = auto_authenticate_secondary_gmail()
            progress_state = ["Counting submissions using job IDs..."]
            process_job_ids(temp_csv, secondary_service)

            final_df = pd.read_csv(temp_csv)
            os.remove(temp_csv)

            # Step 4: Save to Excel
            excel_path = 'duedates_formatted.xlsx'
            writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
            final_df.to_excel(writer, index=False, sheet_name='Job Details')

            workbook = writer.book
            worksheet = writer.sheets['Job Details']

            header_format = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'top',
                'align': 'center', 'border': 1
            })
            cell_format = workbook.add_format({
                'text_wrap': True, 'valign': 'top',
                'align': 'left', 'border': 1
            })

            for col_num, value in enumerate(final_df.columns):
                worksheet.write(0, col_num, value, header_format)

            for row in range(1, len(final_df)+1):
                for col in range(len(final_df.columns)):
                    worksheet.write(row, col, str(final_df.iloc[row-1, col]), cell_format)

            for i, col in enumerate(final_df.columns):
                max_len = max((final_df[col].astype(str).map(len).max(), len(col))) + 2
                worksheet.set_column(i, i, max_len)

            writer.close()
            results_file = excel_path
            logging.info(f"Saved output: {excel_path}")

            # Step 5: Send Email
            progress_state = ["Sending result file to group email..."]
            send_results_email(excel_path, "rtr1@googlegroups.com")

        else:
            logging.warning("No valid job details found")

    except Exception as e:
        logging.error(f"Processing error: {e}")
        flash(f"Error: {str(e)}", "error")
    finally:
        processing = False
        progress_state = []
        process_completed = bool(results_file and os.path.exists(results_file))


@app.route('/')
def index():
    global results_file
    data = []
    if results_file and os.path.exists(results_file):
        try:
            if results_file.endswith('.xlsx'):
                df = pd.read_excel(results_file)
            else:
                df = pd.read_csv(results_file)
            data = df.to_dict('records')
        except Exception as e:
            flash("Could not load file", "error")
            logging.error(f"Read error: {e}")
    return render_template('index.html', data=data, processing=processing, process_completed=process_completed, progress_state=progress_state)


@app.route('/process_emails')
def process_emails():
    global processing, process_completed, progress_state
    if not processing:
        processing = True
        process_completed = False
        progress_state = ["Accessing Gmails..."]
        threading.Thread(target=process_emails_background, daemon=True).start()
    else:
        flash("Already processing", "warning")
    return redirect(url_for('index'))


@app.route('/download')
def download_file():
    global results_file
    if results_file and os.path.exists(results_file):
        return send_file(results_file, as_attachment=True)
    flash("No results available", "error")
    return redirect(url_for('index'))


@app.route('/status')
def status():
    global processing, results_file, process_completed, progress_state
    data = []
    if results_file and os.path.exists(results_file):
        try:
            if results_file.endswith('.xlsx'):
                df = pd.read_excel(results_file)
            else:
                df = pd.read_csv(results_file)
            data = df.to_dict('records')
        except Exception as e:
            return jsonify({'processing': processing, 'data': [], 'error': str(e), 'completed': process_completed, 'progress': progress_state})

    return jsonify({'processing': processing, 'data': data, 'completed': process_completed, 'progress': progress_state})
