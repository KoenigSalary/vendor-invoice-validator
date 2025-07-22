# snapshot_handler.py

import os
import pandas as pd
import hashlib
import json
from datetime import datetime, timedelta
import logging
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def compare_with_snapshot(df, snapshot_dir, today, primary_key='InvID'):
    """
    Enhanced comparison with current dataframe against previous snapshot
    with better error handling and flexible key matching
    
    Args:
        df: Current dataframe
        snapshot_dir: Directory containing snapshots
        today: Current date (string or datetime)
        primary_key: Column to use for record identification
    
    Returns:
        dict: Contains 'added', 'modified', 'deleted' DataFrames and statistics
    """
    try:
        # Ensure today is string format
        if isinstance(today, datetime):
            today_str = today.strftime("%Y-%m-%d")
        else:
            today_str = str(today)
        
        logger.info(f"📊 Comparing current data with snapshot for {today_str}")
        
        # Validate input dataframe
        if df is None or df.empty:
            logger.warning("⚠️ Current dataframe is empty")
            return {
                "added": pd.DataFrame(),
                "modified": pd.DataFrame(), 
                "deleted": pd.DataFrame(),
                "stats": {"added": 0, "modified": 0, "deleted": 0, "unchanged": 0}
            }
        
        # Create snapshot directory if it doesn't exist
        os.makedirs(snapshot_dir, exist_ok=True)
        
        # Find the most recent snapshot
        previous_snapshot_path = find_latest_snapshot(snapshot_dir, today_str)
        
        if not previous_snapshot_path:
            logger.info("📄 No previous snapshot found. Treating all records as new.")
            return {
                "added": df.copy(),
                "modified": pd.DataFrame(),
                "deleted": pd.DataFrame(),
                "stats": {"added": len(df), "modified": 0, "deleted": 0, "unchanged": 0}
            }
        
        # Load previous snapshot
        try:
            logger.info(f"📂 Loading previous snapshot: {previous_snapshot_path}")
            previous_df = pd.read_excel(previous_snapshot_path)
        except Exception as e:
            logger.error(f"❌ Could not load previous snapshot: {str(e)}")
            return {
                "added": df.copy(),
                "modified": pd.DataFrame(),
                "deleted": pd.DataFrame(),
                "stats": {"added": len(df), "modified": 0, "deleted": 0, "unchanged": 0}
            }
        
        # Validate previous dataframe
        if previous_df.empty:
            logger.info("📄 Previous snapshot is empty. Treating all records as new.")
            return {
                "added": df.copy(),
                "modified": pd.DataFrame(),
                "deleted": pd.DataFrame(),
                "stats": {"added": len(df), "modified": 0, "deleted": 0, "unchanged": 0}
            }
        
        # Determine primary key to use
        effective_primary_key = determine_primary_key(df, previous_df, primary_key)
        
        if not effective_primary_key:
            logger.error("❌ No suitable primary key found for comparison")
            return {
                "added": df.copy(),
                "modified": pd.DataFrame(),
                "deleted": pd.DataFrame(),
                "stats": {"added": len(df), "modified": 0, "deleted": 0, "unchanged": 0}
            }
        
        logger.info(f"🔑 Using primary key: {effective_primary_key}")
        
        # Align columns for comparison (only common columns)
        common_columns = list(set(df.columns) & set(previous_df.columns))
        if not common_columns:
            logger.warning("⚠️ No common columns found between current and previous data")
            return {
                "added": df.copy(),
                "modified": pd.DataFrame(),
                "deleted": pd.DataFrame(),
                "stats": {"added": len(df), "modified": 0, "deleted": 0, "unchanged": 0}
            }
        
        # Filter to common columns
        df_common = df[common_columns].copy()
        previous_df_common = previous_df[common_columns].copy()
        
        # Clean data for comparison
        df_clean = clean_dataframe_for_comparison(df_common)
        previous_df_clean = clean_dataframe_for_comparison(previous_df_common)
        
        # Perform comparison
        comparison_result = perform_detailed_comparison(
            df_clean, previous_df_clean, effective_primary_key
        )
        
        # Map results back to original dataframes with all columns
        result = map_comparison_to_original(df, previous_df, comparison_result, effective_primary_key)
        
        # Log statistics
        stats = result["stats"]
        logger.info(f"📈 Comparison complete:")
        logger.info(f"  Added: {stats['added']} records")
        logger.info(f"  Modified: {stats['modified']} records") 
        logger.info(f"  Deleted: {stats['deleted']} records")
        logger.info(f"  Unchanged: {stats['unchanged']} records")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error during snapshot comparison: {str(e)}")
        return {
            "added": df.copy() if df is not None and not df.empty else pd.DataFrame(),
            "modified": pd.DataFrame(),
            "deleted": pd.DataFrame(),
            "stats": {"added": len(df) if df is not None else 0, "modified": 0, "deleted": 0, "unchanged": 0}
        }

def find_latest_snapshot(snapshot_dir, exclude_date=None):
    """Find the most recent snapshot file, optionally excluding a specific date"""
    try:
        snapshot_files = []
        
        for file in os.listdir(snapshot_dir):
            if file.startswith("snapshot_") and file.endswith(".xlsx"):
                try:
                    # Extract date from filename
                    date_part = file.replace("snapshot_", "").replace(".xlsx", "")
                    
                    # Skip if this is the date we want to exclude
                    if exclude_date and date_part == exclude_date:
                        continue
                    
                    # Validate date format
                    datetime.strptime(date_part, "%Y-%m-%d")
                    
                    file_path = os.path.join(snapshot_dir, file)
                    snapshot_files.append((date_part, file_path))
                    
                except ValueError:
                    continue  # Skip invalid date formats
        
        if not snapshot_files:
            return None
        
        # Sort by date and return most recent
        snapshot_files.sort(key=lambda x: x[0], reverse=True)
        return snapshot_files[0][1]
        
    except Exception as e:
        logger.error(f"❌ Error finding latest snapshot: {str(e)}")
        return None

def determine_primary_key(df, previous_df, preferred_key='InvID'):
    """Determine the best primary key to use for comparison"""
    
    # List of potential primary keys in order of preference
    potential_keys = [
        preferred_key,
        'InvID',
        'Invoice_No', 
        'PurchaseInvNo',
        'VoucherNo',
        'Voucher_No'
    ]
    
    for key in potential_keys:
        if key in df.columns and key in previous_df.columns:
            # Check if this key has reasonable uniqueness
            current_unique = df[key].nunique()
            current_total = len(df[key].dropna())
            
            if current_total > 0 and current_unique / current_total > 0.8:  # At least 80% unique
                return key
    
    return None

def clean_dataframe_for_comparison(df):
    """Clean dataframe for more accurate comparison"""
    df_clean = df.copy()
    
    # Convert all columns to string for consistent comparison
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].astype(str).str.strip()
        # Replace NaN/None representations
        df_clean[col] = df_clean[col].replace(['nan', 'None', 'NaT'], '')
    
    return df_clean

def perform_detailed_comparison(current_df, previous_df, primary_key):
    """Perform detailed record-by-record comparison"""
    try:
        # Get unique IDs from both dataframes
        current_ids = set(current_df[primary_key].dropna().unique())
        previous_ids = set(previous_df[primary_key].dropna().unique())
        
        # Find added and deleted IDs
        added_ids = current_ids - previous_ids
        deleted_ids = previous_ids - current_ids
        common_ids = current_ids & previous_ids
        
        # Create result sets
        added_records = current_df[current_df[primary_key].isin(added_ids)].index.tolist()
        deleted_records = previous_df[previous_df[primary_key].isin(deleted_ids)].index.tolist()
        
        # Find modified records among common IDs
        modified_records = []
        unchanged_records = []
        
        for record_id in common_ids:
            if pd.isna(record_id) or record_id == '':
                continue
                
            try:
                # Get current and previous versions of this record
                current_record = current_df[current_df[primary_key] == record_id]
                previous_record = previous_df[previous_df[primary_key] == record_id]
                
                if current_record.empty or previous_record.empty:
                    continue
                
                # Compare records (excluding the primary key column for comparison)
                comparison_columns = [col for col in current_record.columns if col != primary_key]
                
                current_values = current_record[comparison_columns].iloc[0].to_dict()
                previous_values = previous_record[comparison_columns].iloc[0].to_dict()
                
                # Check if any values have changed
                has_changes = False
                for col in comparison_columns:
                    if str(current_values.get(col, '')) != str(previous_values.get(col, '')):
                        has_changes = True
                        break
                
                if has_changes:
                    modified_records.extend(current_record.index.tolist())
                else:
                    unchanged_records.extend(current_record.index.tolist())
                    
            except Exception as e:
                logger.warning(f"⚠️ Error comparing record {record_id}: {str(e)}")
                continue
        
        return {
            'added_indices': added_records,
            'modified_indices': modified_records,
            'deleted_indices': deleted_records,
            'unchanged_indices': unchanged_records
        }
        
    except Exception as e:
        logger.error(f"❌ Error in detailed comparison: {str(e)}")
        return {
            'added_indices': list(current_df.index),
            'modified_indices': [],
            'deleted_indices': [],
            'unchanged_indices': []
        }

def map_comparison_to_original(current_df, previous_df, comparison_result, primary_key):
    """Map comparison results back to original DataFrames with all columns"""
    
    # Extract DataFrames based on indices
    added_df = current_df.iloc[comparison_result['added_indices']] if comparison_result['added_indices'] else pd.DataFrame()
    modified_df = current_df.iloc[comparison_result['modified_indices']] if comparison_result['modified_indices'] else pd.DataFrame()
    deleted_df = previous_df.iloc[comparison_result['deleted_indices']] if comparison_result['deleted_indices'] else pd.DataFrame()
    
    # Create statistics
    stats = {
        'added': len(comparison_result['added_indices']),
        'modified': len(comparison_result['modified_indices']), 
        'deleted': len(comparison_result['deleted_indices']),
        'unchanged': len(comparison_result['unchanged_indices'])
    }
    
    return {
        'added': added_df.reset_index(drop=True),
        'modified': modified_df.reset_index(drop=True),
        'deleted': deleted_df.reset_index(drop=True),
        'stats': stats
    }

def convert_to_json_serializable(obj):
    """Convert numpy/pandas types to JSON serializable types"""
    if isinstance(obj, (np.integer, pd.Int64Dtype)):
        return int(obj)
    elif isinstance(obj, (np.floating, pd.Float64Dtype)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    elif hasattr(obj, 'item'):  # numpy scalars
        return obj.item()
    else:
        return obj

def make_json_serializable(data):
    """Recursively convert data structure to JSON serializable format"""
    if isinstance(data, dict):
        return {key: make_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [make_json_serializable(item) for item in data]
    else:
        return convert_to_json_serializable(data)

def save_snapshot(df, snapshot_dir, today, include_metadata=True):
    """
    Enhanced snapshot saving with metadata and backup management
    FIXED: JSON serialization issue resolved
    
    Args:
        df: DataFrame to save
        snapshot_dir: Directory to save snapshot
        today: Date for filename (string or datetime)
        include_metadata: Whether to save additional metadata
    
    Returns:
        str: Path to saved snapshot file
    """
    try:
        # Ensure directory exists
        os.makedirs(snapshot_dir, exist_ok=True)
        
        # Ensure today is string format
        if isinstance(today, datetime):
            today_str = today.strftime("%Y-%m-%d")
        else:
            today_str = str(today)
        
        # Create snapshot filename
        timestamp = datetime.now().strftime("%H%M%S")
        snapshot_filename = f"snapshot_{today_str}_{timestamp}.xlsx"
        snapshot_path = os.path.join(snapshot_dir, snapshot_filename)
        
        # Also create a "latest" version for easy access
        latest_path = os.path.join(snapshot_dir, f"snapshot_{today_str}.xlsx")
        
        if df is None or df.empty:
            logger.warning("⚠️ Attempting to save empty DataFrame as snapshot")
            # Create empty file to maintain consistency
            pd.DataFrame().to_excel(snapshot_path, index=False)
            return snapshot_path
        
        # Save the main snapshot
        df.to_excel(snapshot_path, index=False, engine='openpyxl')
        
        # Save as latest (overwrite if exists)
        df.to_excel(latest_path, index=False, engine='openpyxl')
        
        # Save metadata if requested
        if include_metadata:
            try:
                # Get potential primary keys with proper type conversion
                potential_keys = get_potential_primary_keys(df)
                
                metadata = {
                    'timestamp': datetime.now().isoformat(),
                    'record_count': int(len(df)),  # Ensure it's a regular Python int
                    'columns': list(df.columns),
                    'date_range': today_str,
                    'file_size': int(os.path.getsize(snapshot_path)),  # Ensure it's a regular Python int
                    'primary_keys_detected': make_json_serializable(potential_keys)  # Make JSON safe
                }
                
                metadata_path = os.path.join(snapshot_dir, f"metadata_{today_str}.json")
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2, default=str)  # Use default=str as fallback
                    
                logger.debug(f"📄 Metadata saved: {metadata_path}")
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to save metadata (non-critical): {str(e)}")
        
        logger.info(f"✅ Snapshot saved: {snapshot_path} ({len(df)} records)")
        
        # Clean up old snapshots (keep last 30 days)
        try:
            cleanup_old_snapshots(snapshot_dir)
        except Exception as e:
            logger.warning(f"⚠️ Cleanup failed (non-critical): {str(e)}")
        
        return snapshot_path
        
    except Exception as e:
        logger.error(f"❌ Error saving snapshot: {str(e)}")
        return None

def get_potential_primary_keys(df):
    """Identify potential primary key columns in the dataframe"""
    potential_keys = []
    
    key_candidates = ['InvID', 'Invoice_No', 'PurchaseInvNo', 'VoucherNo', 'ID']
    
    for col in df.columns:
        if col in key_candidates:
            unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
            non_null_count = df[col].count()
            
            if unique_ratio > 0.8:  # At least 80% unique
                potential_keys.append({
                    'column': col,
                    'uniqueness': float(unique_ratio),  # Ensure it's a regular float
                    'non_null_count': int(non_null_count)  # Ensure it's a regular int
                })
    
    return potential_keys

def cleanup_old_snapshots(snapshot_dir, keep_days=30):
    """Remove snapshots older than specified days"""
    try:
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        
        for filename in os.listdir(snapshot_dir):
            if filename.startswith("snapshot_") and filename.endswith(".xlsx"):
                try:
                    # Extract date from filename
                    date_part = filename.replace("snapshot_", "").split("_")[0]  # Get date part before timestamp
                    file_date = datetime.strptime(date_part, "%Y-%m-%d")
                    
                    if file_date < cutoff_date:
                        file_path = os.path.join(snapshot_dir, filename)
                        os.remove(file_path)
                        logger.info(f"🗑️ Removed old snapshot: {filename}")
                        
                except ValueError:
                    continue  # Skip files with invalid date format
                except Exception as e:
                    logger.warning(f"⚠️ Could not remove {filename}: {str(e)}")
                    
    except Exception as e:
        logger.error(f"❌ Error during snapshot cleanup: {str(e)}")

def generate_change_summary(comparison_result):
    """Generate a human-readable summary of changes"""
    try:
        stats = comparison_result['stats']
        
        summary = []
        summary.append(f"📊 **Change Summary**")
        summary.append(f"  - **Added**: {stats['added']} new records")
        summary.append(f"  - **Modified**: {stats['modified']} updated records")
        summary.append(f"  - **Deleted**: {stats['deleted']} removed records")
        summary.append(f"  - **Unchanged**: {stats['unchanged']} records with no changes")
        
        total_changes = stats['added'] + stats['modified'] + stats['deleted']
        summary.append(f"  - **Total changes**: {total_changes}")
        
        # Add details if there are changes
        if total_changes > 0:
            summary.append(f"\n🔍 **Change Details:**")
            
            if stats['added'] > 0:
                added_df = comparison_result['added']
                if not added_df.empty and 'Vendor' in added_df.columns:
                    top_vendors = added_df['Vendor'].value_counts().head(3)
                    summary.append(f"  - Top vendors with new records: {', '.join(top_vendors.index.tolist())}")
            
            if stats['modified'] > 0:
                summary.append(f"  - {stats['modified']} existing records have been updated")
            
            if stats['deleted'] > 0:
                summary.append(f"  - {stats['deleted']} records are no longer present")
        
        return '\n'.join(summary)
        
    except Exception as e:
        logger.error(f"❌ Error generating change summary: {str(e)}")
        return "Error generating change summary"

# Test function
def test_snapshot_handler():
    """Test the snapshot handler with sample data"""
    try:
        logger.info("🧪 Testing snapshot handler...")
        
        # Create sample current data
        current_data = pd.DataFrame({
            'InvID': ['INV001', 'INV002', 'INV003', 'INV004'],
            'Vendor': ['ABC Corp', 'XYZ Ltd', 'DEF Inc', 'GHI Co'],
            'Amount': [10000, 15000, 8000, 12000],
            'Status': ['VALID', 'FLAGGED', 'VALID', 'VALID']
        })
        
        # Create sample previous data (simulating some changes)
        previous_data = pd.DataFrame({
            'InvID': ['INV001', 'INV002', 'INV005'],  # INV005 deleted, INV003/004 are new
            'Vendor': ['ABC Corp', 'XYZ Limited', 'OLD Vendor'],  # XYZ changed name
            'Amount': [10000, 15000, 5000],
            'Status': ['VALID', 'VALID', 'VALID']  # XYZ status changed
        })
        
        test_dir = "test_snapshots"
        today = "2025-07-21"
        
        # Save previous snapshot first
        save_snapshot(previous_data, test_dir, "2025-07-20")
        
        # Compare with current
        result = compare_with_snapshot(current_data, test_dir, today)
        
        # Print results
        print("\n" + "="*50)
        print("🧪 SNAPSHOT COMPARISON TEST RESULTS")
        print("="*50)
        
        summary = generate_change_summary(result)
        print(summary)
        
        print("\n📊 Detailed Results:")
        print(f"Added records:\n{result['added']}\n")
        print(f"Modified records:\n{result['modified']}\n")
        print(f"Deleted records:\n{result['deleted']}\n")
        
        # Save current snapshot
        save_snapshot(current_data, test_dir, today)
        
        logger.info("✅ Test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}")
        return False

# For backward compatibility with existing code
def compare_with_snapshot_simple(df, snapshot_dir, today):
    """Simple wrapper maintaining original function signature"""
    result = compare_with_snapshot(df, snapshot_dir, today)
    return {
        "added": result["added"],
        "modified": result["modified"], 
        "deleted": result["deleted"]
    }

if __name__ == "__main__":
    # Run test
    test_snapshot_handler()
    
    # Example usage with real data (commented out)
    """
    today = "2025-07-21"
    SNAPSHOT_DIR = "snapshots"
    
    try:
        current_df = pd.read_excel("data/2025-07-21/validation_result.xlsx")
        
        delta = compare_with_snapshot(current_df, SNAPSHOT_DIR, today)
        
        print("\n" + generate_change_summary(delta))
        
        save_snapshot(current_df, SNAPSHOT_DIR, today)
        
    except FileNotFoundError:
        print("❌ Validation result file not found. Please run the validation first.")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    """
