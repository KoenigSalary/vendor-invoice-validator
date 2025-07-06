def send_email_report(excel_path, zip_path, delta_report=None):
    print(f"📧 Email sent with: {excel_path} and {zip_path}")
    if delta_report:
        print("📝 Delta Summary:")
        print(f"➕ New: {len(delta_report.get('added', []))}")
        print(f"✏️ Modified: {len(delta_report.get('modified', []))}")
        print(f"❌ Deleted: {len(delta_report.get('deleted', []))}")