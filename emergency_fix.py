# emergency_fix.py
"""
Emergency fix for InvoiceValidationSystem
"""

# Create a class that will replace the problematic one
class EmergencyFixInvoiceValidationSystem:
    """
    Emergency replacement for InvoiceValidationSystem
    """
    # Class level REQUIRED_FIELDS
    REQUIRED_FIELDS = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
    
    def __init__(self):
        """Initialize with guaranteed REQUIRED_FIELDS"""
        # Instance level REQUIRED_FIELDS
        self.REQUIRED_FIELDS = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
        print("🚑 EmergencyFixInvoiceValidationSystem initialized with REQUIRED_FIELDS")
    
    def _validate_required_fields(self, invoice_data, results):
        """Emergency implementation of _validate_required_fields"""
        missing_fields = []
        for field in self.REQUIRED_FIELDS:
            if field not in invoice_data or invoice_data[field] in [None, '', 'N/A']:
                missing_fields.append(field)
        
        if missing_fields:
            results['errors'].extend([f"Missing required field: {field}" for field in missing_fields])
    
    def validate_invoice_data(self, invoice_data):
        """Simplified validate_invoice_data implementation"""
        results = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Validate required fields
            self._validate_required_fields(invoice_data, results)
            
            # Set is_valid based on errors
            results['is_valid'] = len(results['errors']) == 0
            
        except Exception as e:
            results['is_valid'] = False
            results['errors'].append(f"Validation error: {str(e)}")
        
        return results
    
    def generate_validation_report(self, validation_results):
        """Generate a simple validation report"""
        lines = []
        lines.append("="*60)
        lines.append("INVOICE VALIDATION REPORT")
        lines.append("="*60)
        
        # Overall status
        status = "✅ PASSED" if validation_results['is_valid'] else "❌ FAILED"
        lines.append(f"Overall Status: {status}")
        lines.append("")
        
        # Errors
        if validation_results['errors']:
            lines.append("❌ ERRORS:")
            for error in validation_results['errors']:
                lines.append(f"  • {error}")
        
        # Warnings
        if validation_results['warnings']:
            lines.append("⚠️ WARNINGS:")
            for warning in validation_results['warnings']:
                lines.append(f"  • {warning}")
        
        lines.append("="*60)
        
        return "\n".join(lines)

# Auto-apply the fix to any module that uses InvoiceValidationSystem
import sys
import types

def apply_emergency_fix():
    """Apply the emergency fix to all modules"""
    print("\n🚑 APPLYING EMERGENCY FIX FOR InvoiceValidationSystem...")
    
    # Find all InvoiceValidationSystem classes
    for name, module in list(sys.modules.items()):
        if hasattr(module, 'InvoiceValidationSystem'):
            print(f"🔄 Replacing InvoiceValidationSystem in {name}")
            
            # Replace the class
            module.InvoiceValidationSystem = EmergencyFixInvoiceValidationSystem
            
            # If there are any instances already created, replace their class
            for attr_name in dir(module):
                try:
                    attr = getattr(module, attr_name)
                    if hasattr(attr, '__class__') and attr.__class__.__name__ == 'InvoiceValidationSystem':
                        attr.__class__ = EmergencyFixInvoiceValidationSystem
                        print(f"🔄 Fixed existing instance in {name}.{attr_name}")
                except:
                    pass
    
    # Also replace InvoiceValidationSystem in this module for importing
    globals()['InvoiceValidationSystem'] = EmergencyFixInvoiceValidationSystem
    
    # Special handling for the __main__ module
    main_module = sys.modules['__main__']
    setattr(main_module, 'InvoiceValidationSystem', EmergencyFixInvoiceValidationSystem)
    
    print("🚑 EMERGENCY FIX APPLIED")

# Apply the fix immediately
apply_emergency_fix()

# Export the fixed class
InvoiceValidationSystem = EmergencyFixInvoiceValidationSystem
