# direct_runner.py
"""
Direct runner for invoice validation with emergency fix
"""
# Import emergency fix first, before anything else
import emergency_fix
from emergency_fix import EmergencyFixInvoiceValidationSystem

# Apply fix to InvoiceValidationSystem directly in __main__
import sys
sys.modules['__main__'].InvoiceValidationSystem = EmergencyFixInvoiceValidationSystem

print("🚀 Running invoice validation with emergency fix...")

# Execute main.py directly
try:
    print("📂 Executing main.py directly...")
    
    # Create a clean namespace
    exec_globals = {
        '__name__': '__main__',
        'InvoiceValidationSystem': EmergencyFixInvoiceValidationSystem
    }
    
    # Execute main.py
    with open('main.py', 'r') as f:
        exec(f.read(), exec_globals)
    
    print("✅ Execution completed successfully!")
    
except Exception as e:
    print(f"❌ Error executing main.py: {str(e)}")
    import traceback
    traceback.print_exc()