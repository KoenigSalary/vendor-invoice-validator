# find_main_function.py
import inspect

# Try to import main module
try:
    import main
    
    # Find all functions defined in main.py
    print("📋 Functions defined in main.py:")
    for name, obj in inspect.getmembers(main):
        if inspect.isfunction(obj) and obj.__module__ == 'main':
            print(f"  - {name}")
            # Try to look at the source code to identify if it might be the main function
            try:
                source = inspect.getsource(obj)
                lines = source.split('\n')
                if len(lines) > 3:
                    print(f"    First few lines:")
                    for line in lines[:3]:
                        print(f"      {line}")
                    print(f"      ...")
                else:
                    print(f"    Source: {source}")
            except:
                print(f"    (Source code not available)")
    
    # Check if there's a main block
    print("\n📋 Is there a main block?")
    with open('main.py', 'r') as f:
        content = f.read()
        if '__name__ == "__main__"' in content or "__name__ == '__main__'" in content:
            print("  ✅ Found main block")
            # Try to extract the code in the main block
            import re
            pattern = r'if __name__\s*==\s*[\'"]__main__[\'"]\s*:(.+?)(?=$|\nif \w|def \w|\nclass \w)'
            matches = re.findall(pattern, content, re.DOTALL)
            if matches:
                print("  Main block code:")
                main_block = matches[0].strip()
                lines = main_block.split('\n')
                for i, line in enumerate(lines[:10]):  # Show first 10 lines
                    print(f"    {line}")
                if len(lines) > 10:
                    print("    ...")
            else:
                print("  Could not extract main block code")
        else:
            print("  ❌ No main block found")
    
except ImportError as e:
    print(f"❌ Error importing main module: {str(e)}")
    
except Exception as e:
    print(f"❌ Error analyzing main.py: {str(e)}")
    import traceback
    traceback.print_exc()
