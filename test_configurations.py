# Create test_configurations.py
cat > test_configurations.py << 'EOL'
import os
import sys
sys.path.append('.')
from email_notifier import EnhancedEmailSystem

# Test configurations for Office365
configs = [
    {
        'name': 'Standard Office365',
        'server': 'smtp.office365.com',
        'port': 587,
        'tls': True
    },
    {
        'name': 'Alternative