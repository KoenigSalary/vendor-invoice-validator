# email_notifier.py
import os
import re
import smtplib
import glob
import logging
import zipfile
from typing import List, Optional, Union
from pathlib import Path
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders

__all__ = ["EnhancedEmailSystem", "EmailNotifier"]


class EnhancedEmailSystem:
    """
    Enhanced email system with professional HTML templates and robust error handling.
    Reads SMTP creds from SMTP_* or EMAIL_* env vars.
    """

    def __init__(self, smtp_server: Optional[str] = None, smtp_port: Optional[int] = None,
                 username: Optional[str] = None, password: Optional[str] = None):

        # SMTP configuration (prefer SMTP_*; fallback to EMAIL_*)
        self.smtp_server = (
            smtp_server
            or os.getenv("SMTP_HOST")
            or os.getenv("SMTP_SERVER", "smtp.office365.com")
        )
        self.smtp_port = int(smtp_port or os.getenv("SMTP_PORT", "587"))
        self.username = username or os.getenv("SMTP_USER") or os.getenv("EMAIL_USERNAME")
        self.password = password or os.getenv("SMTP_PASS") or os.getenv("EMAIL_PASSWORD")

        # Recipients configuration
        recipients_str = (
            os.getenv("AP_TEAM_EMAIL_LIST")
            or os.getenv("EMAIL_RECIPIENTS")
            or os.getenv("TEAM_EMAIL_LIST")
            or ""
        )
        self.default_recipients = self._validate_email_list(recipients_str) if recipients_str else []

        # Logging
        self.logger = logging.getLogger(__name__)

    # ---------- validation helpers ----------

    _EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

    def _is_valid_email(self, email: str) -> bool:
        return bool(email and self._EMAIL_RE.match(email.strip()))

    def _validate_email_list(self, recipients_str: str) -> List[str]:
        """Parse and validate a comma/semicolon-separated list of emails."""
        emails: List[str] = []
        for raw in recipients_str.replace(";", ",").split(","):
            e = raw.strip()
            if not e:
                continue
            if self._is_valid_email(e):
                emails.append(e)
            else:
                self.logger.warning(f"Invalid email format ignored: {e}")
        return emails

    # ---------- content builders ----------

    def create_professional_html_template(self, validation_data: dict, deadline_date: datetime) -> str:
        """Create professional HTML email template with enhanced formatting."""

        critical_count = int(validation_data.get("failed", 0))
        warning_count = int(validation_data.get("warnings", 0))
        passed_count = int(validation_data.get("passed", 0))
        total_count = critical_count + warning_count + passed_count

        pass_rate = (passed_count / total_count * 100) if total_count > 0 else 0.0

        # Sample amounts (replace with actual totals if desired)
        critical_amount = "₹1,50,000"
        warning_amount = "₹1,40,000"

        # Keep the layout simple to avoid rendering issues
        html_template = f"""
<!DOCTYPE html>
<html>
  <body style="font-family: Arial, sans-serif; color:#111;">
    <h2>🏢 KOENIG INVOICE VALIDATION REPORT</h2>
    <p>Automated Processing Summary - {datetime.now().strftime('%B %d, %Y')}</p>

    <h3>📊 EXECUTIVE SUMMARY</h3>
    <p><b>Processing Rate:</b> {pass_rate:.1f}% ({passed_count}/{total_count} invoices processed successfully)</p>

    <table border="1" cellspacing="0" cellpadding="6">
      <tr><th>Status</th><th>Count</th><th>Financial Impact</th></tr>
      <tr><td>🚨 CRITICAL ISSUES</td><td>{critical_count}</td><td>{critical_amount}</td></tr>
      <tr><td>⚠️ WARNING ITEMS</td><td>{warning_count}</td><td>{warning_amount}</td></tr>
      <tr><td>✅ SUCCESSFULLY PROCESSED</td><td>{passed_count}</td><td>Ready for Payment</td></tr>
    </table>

    <h3>🚨 IMMEDIATE ACTION REQUIRED</h3>
    <p><b>Response Deadline:</b> {deadline_date.strftime('%B %d, %Y at %I:%M %p IST')}</p>
    <p>Non-response will trigger automatic escalation to Finance Head</p>

    <h3>🎯 REQUIRED ACTIONS</h3>
    <ul>
      <li>Review Failed Invoices: Check attached Excel report for detailed validation errors</li>
      <li>Provide Corrections: Submit corrected invoice data or explanations for exceptions</li>
      <li>Vendor Updates: Update vendor master data if validation issues are due to outdated information</li>
      <li>Approval Status: Confirm approval status for pending invoices</li>
      <li>Documentation: Provide supporting documents for flagged transactions</li>
    </ul>

    <h3>📎 ATTACHMENTS INCLUDED</h3>
    <ul>
      <li>Excel Validation Report</li>
      <li>Invoice Files ZIP</li>
      <li>Processing Summary</li>
    </ul>

    <h3>📞 FOR QUESTIONS OR SUPPORT</h3>
    <p>Finance Team: Accounts@koenig-solutions.com<br/>
       System Support: tax@koenig-solutions.com</p>

    <hr/>
    <p>Koenig Solutions Pvt. Ltd. | Generated by Invoice Management System<br/>
       This is an automated report containing confidential information</p>
  </body>
</html>
"""
        return html_template

    # ---------- zipping ----------

    def create_invoice_zip(self, invoice_files: Optional[List[Union[str, Path]]] = None,
                           validation_period: Optional[str] = None) -> Optional[str]:
        """Create ZIP file with invoice copies and the latest Excel report."""
        zip_filename: Optional[str] = None
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            period = validation_period or "current"
            zip_filename = f"invoice_validation_{period}_{timestamp}.zip"

            with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
                # Add Excel validation report (latest)
                excel_files = glob.glob("invoice_validation_report_*.xlsx")
                if excel_files:
                    latest_excel = max(excel_files, key=os.path.getctime)
                    if os.path.exists(latest_excel):
                        zipf.write(latest_excel, "validation_report.xlsx")
                        self.logger.info(f"Added Excel report to ZIP: {latest_excel}")
                    else:
                        self.logger.warning(f"Excel file not found: {latest_excel}")

                # Add invoice files if provided / present in invoice_files dir
                invoice_count = 0
                if invoice_files:
                    paths = [Path(p) for p in invoice_files]
                else:
                    # Fallback: any files under invoice_files/
                    base_dir = Path("invoice_files")
                    paths = list(base_dir.rglob("*")) if base_dir.exists() else []

                for p in paths:
                    if p.is_file() and p.suffix.lower() in (".pdf", ".png", ".jpg", ".jpeg"):
                        try:
                            zipf.write(str(p), f"invoice_files/{p.name}")
                            invoice_count += 1
                        except Exception as e:
                            self.logger.warning(f"Failed to add file {p}: {e}")

                # Verify ZIP was created
                if os.path.exists(zip_filename) and os.path.getsize(zip_filename) > 0:
                    self.logger.info(f"ZIP created successfully: {zip_filename} ({invoice_count} invoice files)")
                    return zip_filename

                self.logger.error("ZIP file creation failed or file is empty")
                return None

        except Exception as e:
            self.logger.error(f"Error creating ZIP: {e}")
            if zip_filename and os.path.exists(zip_filename):
                try:
                    os.remove(zip_filename)
                except Exception:
                    pass
            return None

    # ---------- sending ----------

    def _clean_recipients(self, recipients: Optional[List[str]]) -> List[str]:
        # Prefer explicit recipients, else defaults
        raw = recipients or self.default_recipients
        clean: List[str] = []
        for r in raw:
            if self._is_valid_email(r):
                clean.append(r)
            else:
                self.logger.warning(f"Ignoring invalid recipient: {r}")
        return clean

    def send_email_with_attachments(self, recipients, subject, html_body, zip_file):
    """Send professional HTML email with ZIP attachment - Enhanced error handling"""
    try:
        # Normalize recipients → flat list[str]
        flat_recipients = []
        if isinstance(recipients, (list, tuple, set)):
            items = list(recipients)
        elif recipients is None:
            items = []
        else:
            items = [recipients]

        for r in items:
            # each r may be a list, a single email, or a comma/semicolon string
            if isinstance(r, (list, tuple, set)):
                for x in r:
                    flat_recipients.extend(self._validate_email_list(str(x)))
            else:
                flat_recipients.extend(self._validate_email_list(str(r)))

        # De-dupe while preserving order
        seen = set()
        valid_recipients = []
        for e in flat_recipients:
            if e not in seen:
                seen.add(e)
                valid_recipients.append(e)

        if not valid_recipients:
            self.logger.error("No valid recipients found")
            return False

        if not self.username or not self.password:
            self.logger.error("SMTP credentials not configured")
            return False

        # Ensure HTML body is a string
        if not isinstance(html_body, str):
            try:
                if isinstance(html_body, (list, tuple)):
                    html_body = "\n".join(str(x) for x in html_body)
                else:
                    html_body = str(html_body)
            except Exception:
                html_body = "<p>(No content)</p>"

        msg = MIMEMultipart('mixed')
        msg['From'] = self.username
        msg['To'] = ', '.join(valid_recipients)
        msg['Subject'] = str(subject) if subject is not None else "Invoice Validation Report"

        # Attach HTML body
        html_part = MIMEText(html_body, 'html', 'utf-8')
        msg.attach(html_part)

        # Attach ZIP (optional)
        if zip_file and os.path.exists(str(zip_file)):
            try:
                file_size = os.path.getsize(str(zip_file))
                if file_size > 25 * 1024 * 1024:
                    self.logger.warning(f"ZIP file too large ({file_size} bytes), skipping attachment")
                else:
                    with open(str(zip_file), 'rb') as attachment:
                        part = MIMEBase('application', 'zip')
                        part.set_payload(attachment.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(str(zip_file))}"')
                        msg.attach(part)
                        self.logger.info(f"ZIP attachment added: {os.path.basename(str(zip_file))} ({file_size} bytes)")
            except Exception as e:
                self.logger.error(f"Failed to attach ZIP file: {e}")

        # Send
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)

            self.logger.info(f"Email sent successfully to: {', '.join(valid_recipients)}")
            return True

        except smtplib.SMTPAuthenticationError:
            self.logger.error("SMTP authentication failed - check credentials")
            return False
        except smtplib.SMTPRecipientsRefused:
            self.logger.error("All recipients were refused by server")
            return False
        except smtplib.SMTPException as e:
            self.logger.error(f"SMTP error occurred: {e}")
            return False

    except Exception as e:
        self.logger.error(f"Error sending email: {e}")
        return False
    finally:
        # Cleanup temporary ZIP file if caller passed a temp path (wrapper cleans its own)
        if zip_file and os.path.exists(str(zip_file)):
            # Only remove temp zips created by wrapper; we cannot tell here reliably,
            # so leave cleanup to the wrapper. No-op.
            pass

    # ---------- config validation ----------

    def validate_email_config(self) -> List[str]:
        issues: List[str] = []
        if not self.username:
            issues.append("Missing SMTP_USER or EMAIL_USERNAME")
        if not self.password:
            issues.append("Missing SMTP_PASS or EMAIL_PASSWORD")
        if not self._clean_recipients(self.default_recipients):
            issues.append("No valid email recipients configured (AP_TEAM_EMAIL_LIST/EMAIL_RECIPIENTS)")
        if not self.smtp_server:
            issues.append("Missing SMTP_HOST/SMTP_SERVER")
        return issues


# ---------------- Compatibility wrapper ----------------

class EmailNotifier:
    """
    Compatibility wrapper around EnhancedEmailSystem.

    Methods provided:
      - send(subject, html_body, attachments=None, recipients=None, from_email=None)
      - send_report(...)
      - send_validation_email(...)
      - send_validation_report(...)
    """

    def __init__(self, smtp_host: Optional[str] = None, smtp_port: Optional[int] = None,
                 smtp_user: Optional[str] = None, smtp_pass: Optional[str] = None,
                 use_tls: Optional[bool] = None, from_name: Optional[str] = None,
                 recipients: Optional[List[str]] = None):

        host = smtp_host or os.getenv("SMTP_HOST") or os.getenv("SMTP_SERVER", "smtp.office365.com")
        port = int(smtp_port or os.getenv("SMTP_PORT", "587"))
        user = smtp_user or os.getenv("SMTP_USER") or os.getenv("EMAIL_USERNAME")
        pwd = smtp_pass or os.getenv("SMTP_PASS") or os.getenv("EMAIL_PASSWORD")

        self._engine = EnhancedEmailSystem(
            smtp_server=host,
            smtp_port=port,
            username=user,
            password=pwd,
        )
        self._from_name = from_name or os.getenv("SMTP_FROM_NAME", "")

        if recipients:
            # Override default recipients if provided explicitly
            clean = [r for r in (recipients if isinstance(recipients, list) else [recipients]) if self._engine._is_valid_email(r)]
            if clean:
                self._engine.default_recipients = clean

    def _zip_attachments_if_needed(self, attachments: Optional[Union[str, Path, List[Union[str, Path]]]]) -> Optional[str]:
        """
        Accepts:
          - None
          - path to a .zip (str/Path)
          - path to a single file (will be zipped)
          - list of file paths (zipped)
        Returns a zip path or None.
        """
        if attachments is None:
            return None

        def _to_str(p: Union[str, Path]) -> str:
            return str(p)

        # Single path-like
        if isinstance(attachments, (str, Path)):
            s = _to_str(attachments)
            if os.path.isfile(s):
                if s.lower().endswith(".zip"):
                    return s
                # bundle single file
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                zip_path = f"email_attachments_{ts}.zip"
                try:
                    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                        zf.write(s, arcname=os.path.basename(s))
                    return zip_path
                except Exception as e:
                    logging.error(f"EmailNotifier: Failed to bundle single attachment: {e}")
                    try:
                        if os.path.exists(zip_path):
                            os.remove(zip_path)
                    except Exception:
                        pass
                    return None
            # Not a file path -> ignore
            logging.warning("EmailNotifier: attachment path not found; ignoring.")
            return None

        # List of paths
        if isinstance(attachments, list):
            paths = [p for p in attachments if isinstance(p, (str, Path))]
            paths = [p for p in paths if os.path.isfile(_to_str(p))]
            if not paths:
                return None
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_path = f"email_attachments_{ts}.zip"
            try:
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for p in paths:
                        sp = _to_str(p)
                        zf.write(sp, arcname=os.path.basename(sp))
                return zip_path if os.path.getsize(zip_path) > 0 else None
            except Exception as e:
                logging.error(f"EmailNotifier: Failed to bundle attachments list: {e}")
                try:
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
                except Exception:
                    pass
                return None

        # Anything else (e.g., int) -> ignore
        logging.warning(f"EmailNotifier: attachments of unsupported type {type(attachments).__name__}; ignoring.")
        return None

    def send(self, subject: str, html_body: str,
             attachments: Optional[Union[str, Path, List[Union[str, Path]]]] = None,
             recipients: Optional[List[str]] = None,
             from_email: Optional[str] = None) -> bool:
        if recipients:
            self._engine.default_recipients = [r for r in (recipients if isinstance(recipients, list) else [recipients]) if self._engine._is_valid_email(r)]

        zip_file = self._zip_attachments_if_needed(attachments)
        return self._engine.send_email_with_attachments(
            self._engine.default_recipients,
            subject,
            html_body,
            zip_file
        )

    # Backward-compatible aliases
    def send_report(self, subject: str, html_body: str, attachments=None, recipients: Optional[List[str]] = None) -> bool:
        return self.send(subject, html_body, attachments, recipients)

    def send_validation_email(self, subject: str, html_body: str, attachments=None, recipients: Optional[List[str]] = None) -> bool:
        return self.send(subject, html_body, attachments, recipients)

    def send_validation_report(self, subject: str, html_body: str, attachments=None, recipients: Optional[List[str]] = None) -> bool:
        return self.send(subject, html_body, attachments, recipients)

# ---------------- Optional local test ----------------

def _main_test() -> str:
    print("🧪 Testing enhanced email system...")

    email_system = EnhancedEmailSystem()
    issues = email_system.validate_email_config()
    if issues:
        print("⚠️ Configuration issues found:")
        for i in issues:
            print(f"  - {i}")
        return "Configuration incomplete"

    print(f"✅ Email system initialized with {len(email_system.default_recipients)} recipients")

    excel_files = glob.glob("invoice_validation_report_*.xlsx")
    if not excel_files:
        print("⚠️ No Excel reports found (test will send without attachment)")
        zip_path = None
    else:
        zip_path = email_system.create_invoice_zip(validation_period="test")

    validation_data = {"failed": 10, "warnings": 2, "passed": 88}
    deadline_date = datetime.now() + timedelta(days=3)
    html_body = email_system.create_professional_html_template(validation_data, deadline_date)
    subject = f"Invoice Validation - Action Required by {deadline_date.strftime('%b %d, %Y')}"

    print("📧 Sending email...")
    ok = email_system.send_email_with_attachments(None, subject, html_body, zip_path)
    print("✅ Email sent!" if ok else "❌ Email send failed")
    return "Success" if ok else "Failed"


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("email_system.log"), logging.StreamHandler()],
    )
    res = _main_test()
    print(f"📈 Email system result: {res}")
