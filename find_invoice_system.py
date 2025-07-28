# find_invoice_system.py
import os
import sys

def search_files(directory='.', term='InvoiceValidationSystem'):
    """Search for the term in all Python files in the directory tree."""
    found_files = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if term in content:
                            found_files.append((file_path, content))
                            print(f"Found '{term}' in: {file_path}")
                except UnicodeDecodeError:
                    print(f"Couldn't read {file_path} (encoding issue)")
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    
    return found_files

if __name__ == "__main__":
    search_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    found = search_files(search_dir)
    
    if not found:
        print(f"No files containing 'InvoiceValidationSystem' were found.")
    else:
        print(f"\nFound the term in {len(found)} files.")
