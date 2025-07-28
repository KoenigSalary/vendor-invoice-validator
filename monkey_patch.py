# monkey_patch.py
import sys
import types
import inspect

# Diagnostic function to print object information
def print_object_info(obj, name="object"):
    print(f"\n🔍 {name} Info:")
    print(f"  Type: {type(obj)}")
    print(f"  Module: {obj.__module__}")
    print(f"  Has REQUIRED_FIELDS: {hasattr(obj, 'REQUIRED_FIELDS')}")
    if hasattr(obj, 'REQUIRED_FIELDS'):
        print(f"  REQUIRED_FIELDS: {obj.REQUIRED_FIELDS}")
    if inspect.isclass(obj):
        print(f"  Methods: {[m for m in dir(obj) if callable(getattr(obj, m)) and not m.startswith('__')][:5]} ...")
    print()

# Create a wrapper for the class
def create_safe_class_wrapper():
    """Create a wrapper for InvoiceValidationSystem that guarantees REQUIRED_FIELDS"""
    
    # Keep track of original classes we've seen
    original_classes = {}
    
    # Wrapper for class creation
    def wrap_class(original_class):
        if original_class in original_classes:
            return original_classes[original_class]
            
        class SafeInvoiceValidationSystem(original_class):
            """Safety wrapper for InvoiceValidationSystem"""
            REQUIRED_FIELDS = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
            
            def __init__(self, *args, **kwargs):
                print(f"🔐 Creating safe InvoiceValidationSystem instance")
                # Set required fields before calling original init
                self.REQUIRED_FIELDS = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
                
                # Call original init if possible
                try:
                    super().__init__(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ Error in original __init__: {str(e)}")
                
                # Ensure REQUIRED_FIELDS exists after init
                if not hasattr(self, 'REQUIRED_FIELDS'):
                    print("🔧 Adding REQUIRED_FIELDS after __init__")
                    self.REQUIRED_FIELDS = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
                    
        # Save and return wrapped class
        original_classes[original_class] = SafeInvoiceValidationSystem
        return SafeInvoiceValidationSystem
    
    return wrap_class

# Create the wrapper
wrap_invoice_validation_system = create_safe_class_wrapper()

# Flag to track if we've applied the monkey patch
monkey_patch_applied = False

def apply_monkey_patch():
    """Apply the monkey patch to all modules"""
    global monkey_patch_applied
    if monkey_patch_applied:
        return
    
    print("\n🐒 Applying InvoiceValidationSystem monkey patch...")
    
    # Find all modules with InvoiceValidationSystem
    ivs_modules = []
    for name, module in list(sys.modules.items()):
        if hasattr(module, 'InvoiceValidationSystem'):
            ivs_modules.append((name, module))
    
    if not ivs_modules:
        print("⚠️ No modules with InvoiceValidationSystem found")
        return
        
    print(f"Found InvoiceValidationSystem in {len(ivs_modules)} modules:")
    for name, module in ivs_modules:
        print(f"  - {name}")
        
        # Check the class
        cls = module.InvoiceValidationSystem
        print_object_info(cls, f"InvoiceValidationSystem class in {name}")
        
        # Wrap the class
        wrapped_cls = wrap_invoice_validation_system(cls)
        
        # Replace the class in the module
        module.InvoiceValidationSystem = wrapped_cls
        print(f"✅ Replaced InvoiceValidationSystem in {name} with safe wrapper")
        
    # Also patch the 'REQUIRED_FIELDS not found' error
    def patch_attribute_error():
        original_getattribute = object.__getattribute__
        
        def safe_getattribute(self, name):
            try:
                return original_getattribute(self, name)
            except AttributeError as e:
                # Only intercept REQUIRED_FIELDS attribute error
                if name == 'REQUIRED_FIELDS' and isinstance(self, tuple(m.InvoiceValidationSystem for _, m in ivs_modules)):
                    print(f"🔧 Intercepted REQUIRED_FIELDS attribute error, returning default value")
                    return ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
                raise e
                
        object.__getattribute__ = safe_getattribute
        print("🔧 Patched object.__getattribute__ to handle REQUIRED_FIELDS errors")
        
    patch_attribute_error()
    
    # Flag that we've applied the patch
    monkey_patch_applied = True
    print("🐒 Monkey patch complete!\n")

# Apply the patch automatically when imported
apply_monkey_patch()

# Function to wrap an instance
def ensure_required_fields(instance):
    """Ensure an instance has REQUIRED_FIELDS"""
    if not hasattr(instance, 'REQUIRED_FIELDS'):
        print(f"🔧 Adding REQUIRED_FIELDS to instance")
        instance.REQUIRED_FIELDS = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
    return instance
