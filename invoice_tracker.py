# invoice_tracker.py

import sqlite3
import hashlib
import json
from datetime import datetime, timedelta
import os   

DB_PATH = "data/invoice_data.db"

def update_database_schema():
    """Update existing database tables to include new columns"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get existing columns in invoice_snapshots table
        cursor.execute("PRAGMA table_info(invoice_snapshots)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        # Add missing columns to invoice_snapshots
        new_columns = {
            'run_type': 'TEXT DEFAULT "standard"',
            'batch_start': 'TEXT',
            'batch_end': 'TEXT',
            'cumulative_start': 'TEXT',
            'cumulative_end': 'TEXT',
            'archived': 'INTEGER DEFAULT 0',
            'archived_date': 'TEXT',
            'created_at': 'TEXT'
        }
        
        for column_name, column_definition in new_columns.items():
            if column_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE invoice_snapshots ADD COLUMN {column_name} {column_definition}")
                    print(f"✅ Added column {column_name} to invoice_snapshots")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" not in str(e).lower():
                        print(f"⚠️ Could not add column {column_name}: {e}")
        
        # Get existing columns in run_log table
        cursor.execute("PRAGMA table_info(run_log)")
        existing_log_columns = [row[1] for row in cursor.fetchall()]
        
        # Add missing columns to run_log
        log_new_columns = {
            'run_date': 'TEXT',
            'run_type': 'TEXT DEFAULT "standard"',
            'cumulative_start': 'TEXT',
            'cumulative_end': 'TEXT',
            'total_days_validated': 'INTEGER DEFAULT 1',
            'archived': 'INTEGER DEFAULT 0',
            'archived_date': 'TEXT',
            'created_at': 'TEXT'
        }
        
        for column_name, column_definition in log_new_columns.items():
            if column_name not in existing_log_columns:
                try:
                    cursor.execute(f"ALTER TABLE run_log ADD COLUMN {column_name} {column_definition}")
                    print(f"✅ Added column {column_name} to run_log")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" not in str(e).lower():
                        print(f"⚠️ Could not add column {column_name}: {e}")
        
        conn.commit()
        conn.close()
        
        print("✅ Database schema updated successfully")
        
    except Exception as e:
        print(f"❌ Failed to update database schema: {str(e)}")

# === Ensure required tables exist ===  
def create_tables():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
        
    # Create invoice_snapshots table (enhanced)
    cursor.execute("""    
        CREATE TABLE IF NOT EXISTS invoice_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_no TEXT,
            vendor_name TEXT,
            invoice_date TEXT,
            gstin TEXT,
            pan TEXT,
            hsn_code TEXT,
            taxable_value REAL,
            total_amount REAL,
            hash TEXT,
            run_date TEXT,
            run_type TEXT DEFAULT 'standard',
            batch_start TEXT,
            batch_end TEXT,
            cumulative_start TEXT,
            cumulative_end TEXT,
            archived INTEGER DEFAULT 0,
            archived_date TEXT,
            created_at TEXT
        )
    """)
        
    # Create run_log table (enhanced)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS run_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_date TEXT,
            end_date TEXT,
            run_date TEXT,
            run_timestamp TEXT,
            run_type TEXT DEFAULT 'standard',
            cumulative_start TEXT,
            cumulative_end TEXT,
            total_days_validated INTEGER DEFAULT 1,
            archived INTEGER DEFAULT 0,
            archived_date TEXT,
            created_at TEXT
        )
    """)

    # Create run_windows table (for compatibility with main.py)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS run_windows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            run_date TEXT NOT NULL,
            run_type TEXT DEFAULT 'standard',
            cumulative_start TEXT,
            cumulative_end TEXT,
            total_days_validated INTEGER DEFAULT 1,
            archived INTEGER DEFAULT 0,
            archived_date TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(start_date, end_date, run_date)
        )
    """)
            
    conn.commit()
    conn.close()
    
    print("✅ Database tables created/verified")
    
    # Update schema for existing tables
    update_database_schema()

# === Create a hash for each invoice ===
def calculate_invoice_hash(invoice):
    key_fields = [
        str(invoice.get("invoice_no", "")),
        str(invoice.get("vendor_name", "")),    
        str(invoice.get("invoice_date", "")),
        str(invoice.get("gstin", "")),
        str(invoice.get("pan", "")),    
        str(invoice.get("hsn_code", "")),
        str(invoice.get("taxable_value", 0)),
        str(invoice.get("total_amount", 0))
    ]
    joined = "|".join(key_fields)
    return hashlib.sha256(joined.encode()).hexdigest()

# === Save invoice snapshot (enhanced) ===
def save_invoice_snapshot(invoice_list, run_date, run_type="standard", **kwargs):
    """Save invoice snapshot with enhanced metadata"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Extract additional metadata from kwargs
        batch_start = kwargs.get('batch_start', run_date)
        batch_end = kwargs.get('batch_end', run_date)
        cumulative_start = kwargs.get('cumulative_start', run_date)
        cumulative_end = kwargs.get('cumulative_end', run_date)
        
        for invoice in invoice_list:
            invoice_hash = calculate_invoice_hash(invoice)
            
            # Handle both dict and DataFrame row objects
            if hasattr(invoice, 'get'):
                # It's a dictionary
                invoice_data = invoice
            else:
                # It's likely a pandas Series or similar, convert to dict
                invoice_data = dict(invoice) if hasattr(invoice, 'to_dict') else invoice
                
            cursor.execute("""
                INSERT OR REPLACE INTO invoice_snapshots (
                    invoice_no, vendor_name, invoice_date, gstin, pan,
                    hsn_code, taxable_value, total_amount, hash, run_date,
                    run_type, batch_start, batch_end, cumulative_start, 
                    cumulative_end, archived, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """, (
                str(invoice_data.get("Invoice_Number", invoice_data.get("invoice_no", ""))), 
                str(invoice_data.get("Vendor_Name", invoice_data.get("vendor_name", ""))), 
                str(invoice_data.get("Invoice_Date", invoice_data.get("invoice_date", ""))),
                str(invoice_data.get("GST_Number", invoice_data.get("gstin", ""))), 
                str(invoice_data.get("pan", "")), 
                str(invoice_data.get("hsn_code", "")),
                float(invoice_data.get("Amount", invoice_data.get("taxable_value", 0))), 
                float(invoice_data.get("Amount", invoice_data.get("total_amount", 0))),
                invoice_hash, 
                run_date,
                run_type,
                batch_start,
                batch_end, 
                cumulative_start,
                cumulative_end,
                datetime.now().isoformat()
            ))

        conn.commit()
        conn.close()
        print(f"✅ Invoice snapshot saved for {run_date} ({run_type}) - {len(invoice_list)} invoices")
        
    except Exception as e:
        print(f"❌ Failed to save invoice snapshot: {str(e)}")

# === Record run window (enhanced) ===
def record_run_window(start_date, end_date, run_type="standard", **kwargs):
    """Record run window with enhanced metadata"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Extract additional metadata from kwargs
        cumulative_start = kwargs.get('cumulative_start', start_date)
        cumulative_end = kwargs.get('cumulative_end', end_date)
        total_days_validated = kwargs.get('total_days_validated', 1)
        current_run_date = datetime.now().strftime("%Y-%m-%d")
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Insert into both tables for compatibility
        
        # Insert into run_log (legacy)
        cursor.execute("""
            INSERT INTO run_log (
                start_date, end_date, run_date, run_timestamp, run_type,
                cumulative_start, cumulative_end, total_days_validated, 
                archived, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (
            start_date, 
            end_date, 
            current_run_date,
            current_timestamp,
            run_type,
            cumulative_start,
            cumulative_end,
            total_days_validated,
            datetime.now().isoformat()
        ))
        
        # Insert into run_windows (new)
        cursor.execute("""
            INSERT OR REPLACE INTO run_windows (
                start_date, end_date, run_date, run_type, cumulative_start, 
                cumulative_end, total_days_validated, archived, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (
            start_date, 
            end_date, 
            current_run_date,
            run_type,
            cumulative_start,
            cumulative_end,
            total_days_validated,
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Run window recorded: {start_date} to {end_date} ({run_type})")
        
    except Exception as e:
        print(f"❌ Failed to record run window: {str(e)}")

# === Retrieve all snapshot entries ===
def get_all_snapshots():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM invoice_snapshots WHERE archived = 0 OR archived IS NULL")
    rows = cursor.fetchall()
    conn.close()
    return rows

# === Retrieve snapshots by date ===
def get_snapshots_by_date_range(start_date, end_date):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM invoice_snapshots
        WHERE invoice_date BETWEEN ? AND ? 
        AND (archived = 0 OR archived IS NULL)
    """, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()
    return rows

# === Retrieve latest run date ===
def get_last_run_date():
    """Get the date of the last validation run"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Try run_windows first (new table)
        cursor.execute("""
            SELECT MAX(run_date) 
            FROM run_windows 
            WHERE archived = 0 OR archived IS NULL
        """)
        result = cursor.fetchone()
        
        if not result or not result[0]:
            # Fallback to run_log (legacy table)
            cursor.execute("SELECT MAX(end_date) FROM run_log WHERE archived = 0 OR archived IS NULL")
            result = cursor.fetchone()
        
        conn.close()
        
        if result and result[0]:
            return result[0]
        else:
            return None
            
    except Exception as e:
        print(f"Error getting last run date: {str(e)}")
        return None

# === Retrieve all run windows ===
def get_all_run_windows():
    """Get all run windows from database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Try run_windows first (new table)
        cursor.execute("""
            SELECT start_date, end_date, run_date, run_type
            FROM run_windows 
            WHERE archived = 0 OR archived IS NULL
            ORDER BY start_date
        """)
        
        results = cursor.fetchall()
        
        if not results:
            # Fallback to run_log (legacy table)
            cursor.execute("""
                SELECT start_date, end_date, start_date as run_date, 
                       COALESCE(run_type, 'standard') as run_type
                FROM run_log 
                WHERE archived = 0 OR archived IS NULL
                ORDER BY start_date ASC
            """)
            results = cursor.fetchall()
        
        conn.close()
        
        windows = []
        for row in results:
            windows.append({
                'start_date': row[0],
                'end_date': row[1], 
                'run_date': row[2],
                'run_type': row[3] if len(row) > 3 else 'standard'
            })
        
        return windows
        
    except Exception as e:
        print(f"Error getting run windows: {str(e)}")
        return []

# === NEW FUNCTIONS REQUIRED BY MAIN.PY ===

def get_first_validation_date():
    """Get the date of the very first validation run"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Try run_windows first (new table)
        cursor.execute("""
            SELECT MIN(start_date) 
            FROM run_windows 
            WHERE archived = 0 OR archived IS NULL
        """)
        result = cursor.fetchone()
        
        if not result or not result[0]:
            # Fallback to run_log (legacy table)
            cursor.execute("""
                SELECT MIN(start_date) 
                FROM run_log 
                WHERE archived = 0 OR archived IS NULL
            """)
            result = cursor.fetchone()
        
        conn.close()
        
        if result and result[0]:
            return result[0]
        else:
            return None
            
    except Exception as e:
        print(f"Error getting first validation date: {str(e)}")
        return None

def get_validation_date_ranges(active_only=True):
    """Get all validation date ranges, optionally only active (non-archived)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if active_only:
            # Try run_windows first
            cursor.execute("""
                SELECT start_date, end_date, run_date, run_type
                FROM run_windows 
                WHERE archived = 0 OR archived IS NULL
                ORDER BY start_date
            """)
            results = cursor.fetchall()
            
            if not results:
                # Fallback to run_log
                cursor.execute("""
                    SELECT start_date, end_date, start_date as run_date, 
                           COALESCE(run_type, 'standard') as run_type
                    FROM run_log 
                    WHERE archived = 0 OR archived IS NULL
                    ORDER BY start_date
                """)
                results = cursor.fetchall()
        else:
            # Get all records
            cursor.execute("""
                SELECT start_date, end_date, run_date, run_type
                FROM run_windows 
                ORDER BY start_date
            """)
            results = cursor.fetchall()
            
            if not results:
                # Fallback to run_log
                cursor.execute("""
                    SELECT start_date, end_date, start_date as run_date,
                           COALESCE(run_type, 'standard') as run_type
                    FROM run_log 
                    ORDER BY start_date
                """)
                results = cursor.fetchall()
        
        conn.close()
        
        date_ranges = []
        for row in results:
            date_ranges.append({
                'start_date': row[0],
                'end_date': row[1], 
                'run_date': row[2],
                'run_type': row[3] if len(row) > 3 else 'standard'
            })
        
        return date_ranges
        
    except Exception as e:
        print(f"Error getting validation date ranges: {str(e)}")
        return []

def archive_validation_records_before_date(cutoff_date):
    """Mark database records as archived before the cutoff date"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Mark run_windows as archived
        cursor.execute("""
            UPDATE run_windows 
            SET archived = 1, archived_date = ?
            WHERE start_date < ? AND (archived = 0 OR archived IS NULL)
        """, (current_date, cutoff_date))
        
        run_windows_archived = cursor.rowcount
        
        # Mark run_log as archived
        cursor.execute("""
            UPDATE run_log 
            SET archived = 1, archived_date = ?
            WHERE start_date < ? AND (archived = 0 OR archived IS NULL)
        """, (current_date, cutoff_date))
        
        run_log_archived = cursor.rowcount
        
        # Mark invoice_snapshots as archived
        cursor.execute("""
            UPDATE invoice_snapshots 
            SET archived = 1, archived_date = ?
            WHERE run_date < ? AND (archived = 0 OR archived IS NULL)
        """, (current_date, cutoff_date))
        
        snapshots_archived = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        total_archived = run_windows_archived + run_log_archived + snapshots_archived
        print(f"✅ Marked {total_archived} database records as archived before {cutoff_date}")
        return total_archived
        
    except Exception as e:
        print(f"Error archiving validation records: {str(e)}")
        return 0

def get_invoice_snapshots_by_date_range(start_date, end_date):
    """Get invoice snapshots within a date range for comparison"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT run_date, invoice_no, vendor_name, invoice_date, 
                   gstin, pan, hsn_code, taxable_value, total_amount, 
                   hash, run_type
            FROM invoice_snapshots 
            WHERE run_date >= ? AND run_date <= ? 
            AND (archived = 0 OR archived IS NULL)
            ORDER BY run_date
        """, (start_date, end_date))
        
        results = cursor.fetchall()
        conn.close()
        
        snapshots = []
        for row in results:
            invoice_data = {
                'invoice_no': row[1],
                'vendor_name': row[2],
                'invoice_date': row[3],
                'gstin': row[4],
                'pan': row[5],
                'hsn_code': row[6],
                'taxable_value': row[7],
                'total_amount': row[8],
                'hash': row[9]
            }
            
            snapshots.append({
                'run_date': row[0],
                'invoice_data': [invoice_data],  # Wrap in list for compatibility
                'run_type': row[10] if len(row) > 10 else 'standard'
            })
        
        return snapshots
        
    except Exception as e:
        print(f"Error getting invoice snapshots: {str(e)}")
        return []

# === UTILITY FUNCTIONS ===

def cleanup_old_data(days_to_keep=90):
    """Clean up data older than specified days (optional utility function)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
        
        # Delete old archived records
        cursor.execute("""
            DELETE FROM run_windows 
            WHERE archived = 1 AND archived_date < ?
        """, (cutoff_date,))
        
        cursor.execute("""
            DELETE FROM run_log 
            WHERE archived = 1 AND archived_date < ?
        """, (cutoff_date,))
        
        cursor.execute("""
            DELETE FROM invoice_snapshots 
            WHERE archived = 1 AND archived_date < ?
        """, (cutoff_date,))
        
        conn.commit()
        deleted_count = cursor.rowcount
        conn.close()
        
        print(f"✅ Cleaned up {deleted_count} old archived records")
        return deleted_count
        
    except Exception as e:
        print(f"Error cleaning up old data: {str(e)}")
        return 0

def test_database_connection():
    """Test database connection and table structure"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Test run_windows table
        cursor.execute("SELECT COUNT(*) FROM run_windows")
        run_windows_count = cursor.fetchone()[0]
        
        # Test invoice_snapshots table
        cursor.execute("SELECT COUNT(*) FROM invoice_snapshots")
        snapshots_count = cursor.fetchone()[0]
        
        # Test run_log table
        cursor.execute("SELECT COUNT(*) FROM run_log")
        run_log_count = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"✅ Database connection successful")
        print(f"   📊 Run windows: {run_windows_count}")
        print(f"   📊 Run log: {run_log_count}")
        print(f"   📋 Invoice snapshots: {snapshots_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    # Test the database when run directly
    create_tables()
    test_database_connection()

