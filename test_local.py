#!/usr/bin/env python3
"""
Local testing script for invoice validation system
This will test each component step by step
"""
import os
import sys
import logging
from pathlib import Path

# Set up basic logging for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_imports():
    """Test all required imports"""
    logger.info("Testing imports...")
    
    try:
        import pandas as pd
        logger.info("✅ pandas imported successfully")
    except ImportError as e:
        logger.error(f"❌ pandas import failed: {e}")
        return False
    
    try:
        import numpy as np
        logger.info("✅ numpy imported successfully")
    except ImportError as e:
        logger.error(f"❌ numpy import failed: {e}")
        return False
    
    try:
        import openpyxl
        logger.info("✅ openpyxl imported successfully")
    except ImportError as e:
        logger.error(f"❌ openpyxl import failed: {e}")
        return False
    
    try:
        import fitz  # PyMuPDF
        logger.info("✅ PyMuPDF (fitz) imported successfully")
    except ImportError as e:
        logger.error(f"❌ PyMuPDF import failed: {e}")
        return False
    
    try:
        from selenium import webdriver
        logger.info("✅ selenium imported successfully")
    except ImportError as e:
        logger.error(f"❌ selenium import failed: {e}")
        return False
    
    try:
        import sqlite3
        logger.info("✅ sqlite3 imported successfully")
    except ImportError as e:
        logger.error(f"❌ sqlite3 import failed: {e}")
        return False
    
    logger.info("✅ All imports successful!")
    return True

def test_directories():
    """Test directory creation"""
    logger.info("Testing directory creation...")
    
    test_dirs = ['downloads', 'logs', 'archive', 'snapshots', 'reports']
    
    for directory in test_dirs:
        try:
            Path(directory).mkdir(exist_ok=True, parents=True)
            logger.info(f"✅ Created directory: {directory}")
        except Exception as e:
            logger.error(f"❌ Failed to create directory {directory}: {e}")
            return False
    
    logger.info("✅ All directories created successfully!")
    return True

def test_database():
    """Test database connection and creation"""
    logger.info("Testing database operations...")
    
    try:
        import sqlite3
        conn = sqlite3.connect("test_invoice_validation.db")
        cursor = conn.cursor()
        
        # Test table creation
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Test insert
        cursor.execute("INSERT INTO test_table (name) VALUES (?)", ("test_record",))
        
        # Test select
        cursor.execute("SELECT * FROM test_table")
        results = cursor.fetchall()
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Database test successful - {len(results)} records found")
        
        # Clean up test database
        os.remove("test_invoice_validation.db")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        return False

def test_pandas_operations():
    """Test pandas data processing"""
    logger.info("Testing pandas operations...")
    
    try:
        import pandas as pd
        import numpy as np
        
        # Create test data
        test_data = {
            'Invoice_Number': ['INV-001', 'INV-002', 'INV-003'],
            'Vendor_Name': ['Test Vendor 1', 'Test Vendor 2', 'Test Vendor 3'],
            'Amount': [1000.50, 2500.75, 750.25]
        }
        
        df = pd.DataFrame(test_data)
        logger.info(f"✅ Created test DataFrame with {len(df)} rows")
        
        # Test CSV operations
        csv_path = Path("downloads") / "test_data.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"✅ Saved test CSV to {csv_path}")
        
        # Test Excel operations
        excel_path = Path("downloads") / "test_data.xlsx"
        df.to_excel(excel_path, index=False, engine='openpyxl')
        logger.info(f"✅ Saved test Excel to {excel_path}")
        
        # Test reading back
        df_csv = pd.read_csv(csv_path)
        df_excel = pd.read_excel(excel_path, engine='openpyxl')
        
        logger.info(f"✅ Successfully read back CSV ({len(df_csv)} rows) and Excel ({len(df_excel)} rows)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pandas operations test failed: {e}")
        return False

def test_main_import():
    """Test importing the main script"""
    logger.info("Testing main script import...")
    
    try:
        # Add current directory to path so we can import main
        sys.path.insert(0, '.')
        
        # Try to import main module
        import main
        logger.info("✅ Main script imported successfully")
        
        # Test basic classes
        if hasattr(main, 'Config'):
            config = main.Config()
            logger.info("✅ Config class created successfully")
        
        if hasattr(main, 'DatabaseManager'):
            db_manager = main.DatabaseManager()
            logger.info("✅ DatabaseManager created successfully")
        
        if hasattr(main, 'ProductionInvoiceValidationSystem'):
            system = main.ProductionInvoiceValidationSystem()
            logger.info("✅ ProductionInvoiceValidationSystem created successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Main script import failed: {e}")
        logger.error("This might be due to the PyPDF2 import conflict we need to fix")
        return False

def run_all_tests():
    """Run all tests and report results"""
    logger.info("🚀 Starting Invoice Validation System Local Tests")
    logger.info("=" * 60)
    
    tests = [
        ("Import Tests", test_imports),
        ("Directory Tests", test_directories),
        ("Database Tests", test_database),
        ("Pandas Tests", test_pandas_operations),
        ("Main Script Tests", test_main_import),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- Running {test_name} ---")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"❌ {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST RESULTS SUMMARY:")
    logger.info("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Your environment is ready.")
        return True
    else:
        logger.error("⚠️ Some tests failed. Check the logs above for details.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)