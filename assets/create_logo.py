from PIL import Image, ImageDraw, ImageFont
import os

# Create a simple logo
img = Image.new('RGB', (300, 100), color='#2E86C1')
draw = ImageDraw.Draw(img)

try:
    # Try to use a system font
    font = ImageFont.truetype("arial.ttf", 24)
except:
    font = ImageFont.load_default()

# Draw text
draw.text((20, 30), "KOENIG SOLUTIONS", fill='white', font=font)
draw.text((20, 60), "Invoice Validation System", fill='#F39C12', font=font)

# Save logo
img.save('assets/koenig_logo.png')
print("✅ Logo created successfully")
