# run_fixed.py
"""
Simplified runner for invoice validation with emergency fix
"""
# Import emergency fix first, before anything else
import emergency_fix

# Now import from main.py
from main import run_invoice_validation

print("🚀 Running invoice validation with emergency fix...")

# Run the validation
try:
    run_invoice_validation()
    print("✅ Validation completed successfully!")
except Exception as e:
    print(f"❌ Error running validation: {str(e)}")
    import traceback
    traceback.print_exc()
