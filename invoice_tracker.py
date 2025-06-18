import sqlite3
import hashlib
from datetime import datetime

def calculate_invoice_hash(invoice):
    """Create a hash of invoice data for change detection."""
    key_fields = [
        invoice["invoice_no"],
        invoice["vendor_name"],
        invoice["invoice_date"],
        invoice["gstin"],
        invoice["pan"],
        invoice["hsn_code"],
        str(invoice["taxable_value"]),
        str(invoice["total_amount"])
    ]
    joined = "|".join(key_fields)
    return hashlib.sha256(joined.encode()).hexdigest()

def save_invoice_snapshot(invoice_list, run_date):
    conn = sqlite3.connect("data/invoice_data.db")
    cursor = conn.cursor()

    for invoice in invoice_list:
        invoice_hash = calculate_invoice_hash(invoice)
        cursor.execute("""
            INSERT OR REPLACE INTO invoice_snapshots (
                invoice_no, vendor_name, invoice_date, gstin, pan,
                hsn_code, taxable_value, total_amount,
                hash, run_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            invoice["invoice_no"], invoice["vendor_name"], invoice["invoice_date"],
            invoice["gstin"], invoice["pan"], invoice["hsn_code"],
            invoice["taxable_value"], invoice["total_amount"],
            invoice_hash, run_date
        ))

    conn.commit()
    conn.close()

def record_run_window(start_date, end_date):
    conn = sqlite3.connect("data/invoice_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO run_log (start_date, end_date, run_timestamp)
        VALUES (?, ?, ?)
    """, (start_date, end_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

def get_all_snapshots():
    conn = sqlite3.connect("data/invoice_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM invoice_snapshots")
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_snapshots_by_date_range(start_date, end_date):
    conn = sqlite3.connect("data/invoice_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM invoice_snapshots
        WHERE invoice_date BETWEEN ? AND ?
    """, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_last_run_date():
    conn = sqlite3.connect("data/invoice_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(end_date) FROM run_log")
    row = cursor.fetchone()
    conn.close()
    return row[0] if row and row[0] else None

def get_all_run_windows():
    conn = sqlite3.connect("data/invoice_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT start_date, end_date FROM run_log ORDER BY start_date ASC")
    rows = cursor.fetchall()
    conn.close()
    return rows

