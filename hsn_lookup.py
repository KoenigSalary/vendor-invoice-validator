# hsn_lookup.py

# Format: (type, gst_rate)
hsn_sac_gst_rates = {
    "998313": ("SAC", 18),
    "998596": ("SAC", 18),
    "997331": ("SAC", 12),
    "9992": ("SAC", 0),
    "2106": ("HSN", 18),
    "3004": ("HSN", 12),
    "8517": ("HSN", 18),
    "8415": ("HSN", 28),
    "8703": ("HSN", 28),
    "8528": ("HSN", 18),
    "9603": ("HSN", 12),
    "0401": ("HSN", 5),
}

def get_expected_gst_rate(hsn_code):
    """
    Returns the expected GST rate for a given HSN/SAC code.
    """
    entry = hsn_sac_gst_rates.get(hsn_code.strip())
    if entry:
        return entry[1]
    return None

def get_code_type(hsn_code):
    """
    Returns whether the code is HSN or SAC.
    """
    entry = hsn_sac_gst_rates.get(hsn_code.strip())
    if entry:
        return entry[0]
    return None
