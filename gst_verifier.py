import re

# -------------------------
# GSTIN & PAN Verification
# -------------------------

def is_valid_gstin(gstin):
    """
    Validates GSTIN structure:
    - 15 characters total
    - First 2: State Code (digits)
    - Next 10: PAN (5 letters + 4 digits + 1 letter)
    - Next 1: Entity code (alphanumeric)
    - 14th character: Always 'Z'
    - 15th character: Checksum (alphanumeric)
    """
    if not gstin or len(gstin.strip()) != 15:
        return False
    pattern = r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$"
    return bool(re.match(pattern, gstin.strip().upper()))

def extract_pan_from_gstin(gstin):
    """
    Extracts the PAN (10 characters) from a valid 15-character GSTIN.
    """
    if not gstin or len(gstin.strip()) != 15:
        return None
    return gstin.strip().upper()[2:12]

def match_pan_with_invoice(pan_from_gstin, invoice_pan):
    """
    Checks if the PAN extracted from GSTIN matches the PAN from invoice.
    Comparison is case-insensitive and whitespace-trimmed.
    """
    if not pan_from_gstin or not invoice_pan:
        return False
    return pan_from_gstin.strip().upper() == invoice_pan.strip().upper()

# -------------------------
# GST Portal API Stub
# -------------------------

def verify_with_gst_portal(gstin):
    """
    Simulates GSTIN verification via GST portal or third-party API.
    Replace this stub with real API logic (e.g., ClearTax) when available.
    """
    gstin = gstin.strip().upper()

    if not is_valid_gstin(gstin):
        return {
            "gstin": gstin,
            "valid": False,
            "status": "Invalid Format",
            "registered_name": None
        }

    # Simulated valid response
    return {
        "gstin": gstin,
        "valid": True,
        "status": "Active",
        "registered_name": "Mock Vendor Name Pvt. Ltd."
    }

# -------------------------
# Example usage (testing)
# -------------------------

if __name__ == "__main__":
    sample_gstin = "07ABCDE1234F1Z5"
    sample_pan = "ABCDE1234F"

    result = verify_with_gst_portal(sample_gstin)
    print("GST Portal Response:", result)

    extracted_pan = extract_pan_from_gstin(sample_gstin)
    print("Extracted PAN:", extracted_pan)

    is_match = match_pan_with_invoice(extracted_pan, sample_pan)
    print("PAN Matches:", is_match)
