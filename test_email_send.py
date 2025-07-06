from email_report import send_email_report

report_path = "data/2025-07-06/validation_result.xlsx"
zip_path = "data/2025-07-06/invoices.zip"

send_email_report(report_path, zip_path)

