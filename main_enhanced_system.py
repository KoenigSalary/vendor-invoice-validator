import schedule
import time
from datetime import datetime, timedelta
import os
import json
from enhanced_report_generator import EnhancedReportGenerator, HistoricalDataTracker
from enhanced_email_system import EnhancedEmailSystem

class EnhancedInvoiceValidationSystem:
    def __init__(self, config_file='enhanced_config.json'):
        self.load_configuration(config_file)
        self.report_generator = EnhancedReportGenerator()
        self.history_tracker = HistoricalDataTracker()
        self.email_system = EnhancedEmailSystem(
            self.config['email']['smtp_server'],
            self.config['email']['smtp_port'],
            self.config['email']['username'],
            self.config['email']['password']
        )
        
    def load_configuration(self, config_file):
        """Load system configuration"""
        default_config = {
            "email": {
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "username": "your-email@gmail.com",
                "password": "your-app-password",
                "recipients": [
                    "finance@koenig-solutions.com",
                    "accounts@koenig-solutions.com",
                    "management@koenig-solutions.com"
                ]
            },
            "rms": {
                "api_endpoint": "your-rms-api-endpoint",
                "api_key": "your-rms-api-key"
            },
            "validation": {
                "run_interval_days": 4,
                "historical_check_months": 3,
                "due_date_alert_days": 5
            }
        }
        
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        else:
            self.config = default_config
            with open(config_file, 'w') as f:
                json.dump(default_config, f, indent=4)
    
    def fetch_rms_data(self):
        """Fetch invoice data from RMS system with enhanced fields"""
        # This would connect to your actual RMS system
        # For now, returning mock data structure with all new fields
        mock_data = [
            {
                'invoice_number': 'INV-2025-001',
                'invoice_id': 'rms_12345',
                'vendor_name': 'Tech Vendor Ltd',
                'invoice_date': datetime.now() - timedelta(days=2),
                'total_amount': 100000,
                'currency': 'INR',
                'location': 'Delhi HO',
                'scid': 'SCID001',
                'mode_of_payment': 'Bank Transfer',
                'account_head': 'IT Equipment',
                'payment_terms': 30,
                'uploaded_by': 'finance.team@koenig.com',
                'supplier_state_code': '07',  # Delhi
                'buyer_state_code': '07',     # Delhi
                'rms_invoice_id': 'RMS_INV_001'
            }
            # Add more mock data as needed
        ]
        return mock_data
    
    def run_enhanced_validation(self):
        """Run the complete enhanced validation process"""
        print(f"🚀 Starting Enhanced Invoice Validation - {datetime.now()}")
        
        try:
            # 1. Fetch current invoice data
            current_invoices = self.fetch_rms_data()
            
            # 2. Fetch historical data for comparison
            historical_cutoff = datetime.now() - timedelta(days=90)  # 3 months
            previous_invoices = self.fetch_historical_invoices(historical_cutoff)
            
            # 3. Track changes
            changes_detected = self.history_tracker.track_changes(current_invoices, previous_invoices)
            
            # 4. Run validation logic (your existing validation)
            validation_results = self.validate_invoices(current_invoices)
            
            # 5. Generate enhanced Excel report
            excel_report = self.report_generator.generate_enhanced_report(
                current_invoices, validation_results
            )
            
            # 6. Create invoice ZIP file
            invoice_files = self.collect_invoice_files(current_invoices)
            invoice_zip = self.email_system.create_invoice_zip(
                invoice_files, 
                f"{datetime.now().strftime('%Y-%m-%d')}_4days"
            )
            
            # 7. Generate additional reports
            changes_csv = self.generate_changes_csv(changes_detected)
            compliance_pdf = self.generate_compliance_pdf(validation_results)
            
            # 8. Prepare validation summary
            validation_summary = self.prepare_validation_summary(
                validation_results, current_invoices, changes_detected
            )
            
            # 9. Send enhanced email
            attachments = [excel_report, invoice_zip, changes_csv, compliance_pdf]
            success, message = self.email_system.send_enhanced_email(
                self.config['email']['recipients'],
                validation_summary,
                changes_detected,
                attachments
            )
            
            if success:
                print(f"✅ Enhanced validation completed successfully - {datetime.now()}")
            else:
                print(f"❌ Email sending failed: {message}")
            
            # 10. Cleanup temporary files
            self.cleanup_temp_files(attachments)
            
        except Exception as e:
            print(f"❌ Enhanced validation failed: {str(e)}")
            # Send error notification
            self.send_error_notification(str(e))
    
    def validate_invoices(self, invoices):
        """Your existing validation logic enhanced"""
        validation_results = []
        for invoice in invoices:
            result = {
                'status': 'Valid',
                'issues': []
            }
            
            # Add your existing validation rules here
            # Plus new validations for enhanced fields
            
            if not invoice.get('scid'):
                result['issues'].append('Missing SCID')
                result['status'] = 'Invalid'
            
            if not invoice.get('mode_of_payment'):
                result['issues'].append('Missing Mode of Payment')
                result['status'] = 'Invalid'
            
            # GST/VAT validation
            if not self.validate_tax_calculation(invoice):
                result['issues'].append('Tax calculation mismatch')
                result['status'] = 'Invalid'
            
            validation_results.append(result)
        
        return validation_results
    
    def validate_tax_calculation(self, invoice):
        """Validate GST/VAT calculations"""
        # Implement your tax validation logic here
        return True  # Placeholder
    
    def prepare_validation_summary(self, validation_results, invoices, changes):
        """Prepare comprehensive validation summary"""
        total_issues = sum(1 for result in validation_results if result['status'] == 'Invalid')
        currencies = list(set(inv.get('currency', 'INR') for inv in invoices))
        
        # Count locations
        locations = set()
        for inv in invoices:
            location, _ = self.report_generator.determine_location_and_entity(inv)
            locations.add(location.split(' -')[0])
        
        # Due date alerts
        due_date_alerts = []
        for inv in invoices:
            due_date, notification_needed = self.report_generator.check_due_date_notification(
                inv.get('invoice_date'), inv.get('payment_terms')
            )
            if notification_needed:
                due_date_alerts.append({
                    'invoice_number': inv.get('invoice_number'),
                    'due_date': due_date.strftime('%Y-%m-%d') if due_date else 'N/A',
                    'vendor': inv.get('vendor_name')
                })
        
        return {
            'total_issues': total_issues,
            'locations_count': len(locations),
            'currencies': currencies,
            'due_date_alerts': due_date_alerts,
            'tax_breakdown': {}  # Add tax breakdown logic here
        }
    
    def setup_scheduler(self):
        """Setup automatic scheduling every 4 days"""
        schedule.every(4).days.at("18:00").do(self.run_enhanced_validation)
        
        print("📅 Enhanced Invoice Validation System scheduled to run every 4 days at 6:00 PM")
        
        while True:
            schedule.run_pending()
            time.sleep(3600)  # Check every hour

if __name__ == "__main__":
    system = EnhancedInvoiceValidationSystem()
    
    # Run once immediately for testing
    system.run_enhanced_validation()
    
    # Start scheduler
    system.setup_scheduler()