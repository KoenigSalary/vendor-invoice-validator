import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import sys
from pathlib import Path
import glob

class EnhancedReconciliation:
    def __init__(self):
        # Branch mapping for Koenig Solutions locations
        self.branch_mapping = {
            'gurgaon': 'Gurgaon',
            'dehradun': 'Dehradun', 
            'goa': 'Goa',
            'chennai': 'Chennai',
            'bangalore': 'Bangalore'
        }
        self.default_branch = 'Delhi'
        
        # Designation mapping
        self.designation_mapping = {
            # Technical roles
            'trainer': 'Technical Trainer',
            'instructor': 'Technical Trainer',
            'technical': 'Technical Staff',
            'developer': 'Developer',
            'engineer': 'Engineer',
            'architect': 'Technical Architect',
            
            # Management roles
            'manager': 'Manager',
            'director': 'Director',
            'head': 'Department Head',
            'lead': 'Team Lead',
            'supervisor': 'Supervisor',
            'vice president': 'Vice President',
            'vp': 'Vice President',
            
            # Support roles
            'admin': 'Admin Staff',
            'hr': 'HR Staff',
            'accounts': 'Accounts Staff',
            'finance': 'Finance Staff',
            'sales': 'Sales Staff',
            'marketing': 'Marketing Staff',
            'support': 'Support Staff',
            'operations': 'Operations Staff',
            
            # Others
            'executive': 'Executive',
            'associate': 'Associate',
            'assistant': 'Assistant',
            'coordinator': 'Coordinator',
            'specialist': 'Specialist',
            'analyst': 'Analyst',
            'consultant': 'Consultant'
        }
        self.default_designation = 'Other Staff'
    
    def map_employee_to_branch(self, location):
        """Map employee location to branch"""
        if not location or pd.isna(location):
            return self.default_branch
            
        location_lower = str(location).lower().strip()
        
        for key, branch in self.branch_mapping.items():
            if key in location_lower:
                return branch
                
        return self.default_branch
    
    def map_employee_to_designation(self, designation_text):
        """Map employee designation to standardized categories"""
        if not designation_text or pd.isna(designation_text):
            return self.default_designation
            
        designation_lower = str(designation_text).lower().strip()
        
        for key, designation in self.designation_mapping.items():
            if key in designation_lower:
                return designation
                
        return self.default_designation
    
    def detect_file_columns(self, df, file_type):
        """Detect relevant columns based on file type"""
        columns = {}
        
        if file_type == 'salary':
            # Employee ID
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['empcode', 'employee id', 'emp_id', 'id']):
                    columns['employee_id'] = col
                    break
            
            # Employee Name
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['employeename', 'employee name', 'name']):
                    columns['employee_name'] = col
                    break
            
            # Location
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['baselocation', 'location', 'branch']):
                    columns['location'] = col
                    break
            
            # Designation
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['designation', 'role', 'position']):
                    columns['designation'] = col
                    break
            
            # Department
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['department', 'dept']):
                    columns['department'] = col
                    break
            
            # Salary amounts
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['basic', 'salary', 'net']):
                    columns['basic_salary'] = col
                    break
                    
        elif file_type == 'bank':
            # Employee column with Name-ID format
            for col in df.columns:
                col_lower = str(col).lower()
                if 'employee' in col_lower:
                    columns['employee'] = col
                    break
            
            # Amount
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['amount', 'salary', 'paid']):
                    columns['amount'] = col
                    break
                    
        elif file_type in ['epf', 'nps']:
            # Employee ID
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['employee', 'emp', 'id', 'uan', 'pran']):
                    columns['employee_id'] = col
                    break
            
            # Amount
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['amount', 'contribution', 'deduction']):
                    columns['amount'] = col
                    break
                    
        elif file_type == 'tds':
            # Employee ID
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['employee', 'emp', 'id']):
                    columns['employee_id'] = col
                    break
            
            # TDS Amount
            for col in df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['tds', 'tax', 'deducted']):
                    columns['tds_amount'] = col
                    break
        
        return columns
    
    def read_file_smart(self, file_path, file_type):
        """Smart file reader that handles different formats"""
        try:
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext in ['.xls', '.xlsx']:
                # Check if it's actually HTML
                with open(file_path, 'rb') as f:
                    first_bytes = f.read(20)
                    if b'<Table>' in first_bytes or b'<table>' in first_bytes:
                        # HTML table
                        tables = pd.read_html(file_path)
                        df = max(tables, key=len)
                        
                        # Fix headers if needed
                        if df.columns.dtype == 'int64':
                            for row_idx in range(min(10, len(df))):
                                row_data = df.iloc[row_idx]
                                text_count = sum(1 for cell in row_data if isinstance(cell, str) and len(str(cell)) > 2)
                                if text_count > len(row_data) * 0.5:
                                    df.columns = row_data.astype(str)
                                    df = df.iloc[row_idx+1:].reset_index(drop=True)
                                    break
                    else:
                        # Regular Excel
                        try:
                            df = pd.read_excel(file_path, engine='openpyxl')
                        except:
                            df = pd.read_excel(file_path, engine='xlrd')
            
            elif file_ext == '.csv':
                df = pd.read_csv(file_path)
            
            elif file_ext == '.txt':
                # Try tab-separated first
                try:
                    df = pd.read_csv(file_path, sep='\t')
                except:
                    df = pd.read_csv(file_path)
            
            else:
                # Try as tab-separated (for .xls that are actually TSV)
                try:
                    df = pd.read_csv(file_path, sep='\t', encoding='utf-8')
                except:
                    df = pd.read_csv(file_path, encoding='utf-8')
            
            # Clean column names
            df.columns = [str(col).strip() for col in df.columns]
            
            print(f"✅ Loaded {file_type} file: {df.shape[0]} rows, {df.shape[1]} columns")
            return df
            
        except Exception as e:
            print(f"❌ Error reading {file_type} file {file_path}: {e}")
            return None
    
    def process_bank_file(self, bank_df):
        """Extract employee IDs from bank file Employee column (Name-ID format)"""
        employee_ids = []
        
        # Find Employee column
        employee_col = None
        for col in bank_df.columns:
            if 'employee' in str(col).lower():
                employee_col = col
                break
        
        if not employee_col:
            return []
        
        # Extract IDs from Name-ID format
        for value in bank_df[employee_col]:
            if pd.isna(value):
                continue
            
            value_str = str(value).strip()
            if '-' in value_str:
                employee_id = value_str.split('-')[-1].strip()
                employee_ids.append(employee_id)
            else:
                employee_ids.append('')
        
        return employee_ids
    
    def reconcile_six_files(self, files):
        """
        Enhanced 6-file reconciliation:
        files = {
            'salary': 'path/to/salary.xls',
            'tds': 'path/to/tds.xls', 
            'bank_kotak': 'path/to/kotak.xls',
            'bank_deutsche': 'path/to/deutsche.xls',
            'epf': 'path/to/epf.xlsx',
            'nps': 'path/to/nps.xlsx'
        }
        """
        
        print("🚀 Starting Enhanced 6-File Reconciliation Process...")
        
        # Load all files
        data = {}
        for file_type, file_path in files.items():
            if file_path and os.path.exists(file_path):
                print(f"📁 Loading {file_type} file...")
                data[file_type] = self.read_file_smart(file_path, file_type)
            else:
                print(f"⚠️ {file_type} file not found: {file_path}")
                data[file_type] = None
        
        # Main salary dataframe
        salary_df = data['salary']
        if salary_df is None:
            raise Exception("Salary file is required for reconciliation")
        
        # Detect columns
        salary_cols = self.detect_file_columns(salary_df, 'salary')
        
        # Add analysis columns
        if 'location' in salary_cols:
            salary_df['Branch'] = salary_df[salary_cols['location']].apply(self.map_employee_to_branch)
        else:
            salary_df['Branch'] = self.default_branch
        
        if 'designation' in salary_cols:
            salary_df['Designation_Category'] = salary_df[salary_cols['designation']].apply(self.map_employee_to_designation)
            salary_df['Original_Designation'] = salary_df[salary_cols['designation']]
        else:
            salary_df['Designation_Category'] = self.default_designation
            salary_df['Original_Designation'] = 'Not Specified'
        
        # Add Department if available
        if 'department' in salary_cols:
            salary_df['Department'] = salary_df[salary_cols['department']]
        else:
            salary_df['Department'] = 'General'
        
        # Initialize reconciliation status columns
        status_columns = ['Bank_Match_Status', 'TDS_Match_Status', 'EPF_Match_Status', 'NPS_Match_Status']
        for col in status_columns:
            salary_df[col] = 'Pending'
        
        # Reconciliation tracking
        matches = {'bank': 0, 'tds': 0, 'epf': 0, 'nps': 0}
        discrepancies = []
        
        # Get employee IDs from salary
        if 'employee_id' not in salary_cols:
            print("❌ Employee ID column not found in salary data")
            return None, [], {}
        
        salary_emp_ids = salary_df[salary_cols['employee_id']].astype(str)
        
        # Process Bank Files
        bank_employee_ids = set()
        for bank_type in ['bank_kotak', 'bank_deutsche']:
            if data[bank_type] is not None:
                print(f"🏦 Processing {bank_type}...")
                bank_ids = self.process_bank_file(data[bank_type])
                bank_employee_ids.update(bank_ids)
                print(f"   Found {len(bank_ids)} records")
        
        # Match with bank
        for idx, emp_id in enumerate(salary_emp_ids):
            if str(emp_id) in bank_employee_ids:
                salary_df.at[idx, 'Bank_Match_Status'] = 'Matched'
                matches['bank'] += 1
            else:
                salary_df.at[idx, 'Bank_Match_Status'] = 'Not Found'
        
        # Process TDS
        if data['tds'] is not None:
            print("💰 Processing TDS file...")
            tds_df = data['tds']
            tds_cols = self.detect_file_columns(tds_df, 'tds')
            
            if 'employee_id' in tds_cols:
                tds_emp_ids = set(tds_df[tds_cols['employee_id']].astype(str))
                
                for idx, emp_id in enumerate(salary_emp_ids):
                    if str(emp_id) in tds_emp_ids:
                        salary_df.at[idx, 'TDS_Match_Status'] = 'Matched'
                        matches['tds'] += 1
                    else:
                        salary_df.at[idx, 'TDS_Match_Status'] = 'Not Found'
        
        # Process EPF
        if data['epf'] is not None:
            print("🏛️ Processing EPF file...")
            epf_df = data['epf']
            epf_cols = self.detect_file_columns(epf_df, 'epf')
            
            if 'employee_id' in epf_cols:
                epf_emp_ids = set(epf_df[epf_cols['employee_id']].astype(str))
                
                for idx, emp_id in enumerate(salary_emp_ids):
                    if str(emp_id) in epf_emp_ids:
                        salary_df.at[idx, 'EPF_Match_Status'] = 'Matched'
                        matches['epf'] += 1
                    else:
                        salary_df.at[idx, 'EPF_Match_Status'] = 'Not Found'
        
        # Process NPS
        if data['nps'] is not None:
            print("🏛️ Processing NPS file...")
            nps_df = data['nps']
            nps_cols = self.detect_file_columns(nps_df, 'nps')
            
            if 'employee_id' in nps_cols:
                nps_emp_ids = set(nps_df[nps_cols['employee_id']].astype(str))
                
                for idx, emp_id in enumerate(salary_emp_ids):
                    if str(emp_id) in nps_emp_ids:
                        salary_df.at[idx, 'NPS_Match_Status'] = 'Matched'
                        matches['nps'] += 1
                    else:
                        salary_df.at[idx, 'NPS_Match_Status'] = 'Not Found'
        
        # Create comprehensive discrepancies
        for idx, row in salary_df.iterrows():
            issues = []
            if row['Bank_Match_Status'] == 'Not Found':
                issues.append('Bank SOA')
            if row['TDS_Match_Status'] == 'Not Found':
                issues.append('TDS')
            if row['EPF_Match_Status'] == 'Not Found':
                issues.append('EPF')
            if row['NPS_Match_Status'] == 'Not Found':
                issues.append('NPS')
            
            if issues:
                discrepancies.append({
                    'Employee_ID': row.get(salary_cols['employee_id'], ''),
                    'Employee_Name': row.get(salary_cols.get('employee_name', ''), ''),
                    'Branch': row.get('Branch', 'Delhi'),
                    'Department': row.get('Department', 'General'),
                    'Designation': row.get('Designation_Category', 'Other Staff'),
                    'Missing_From': ', '.join(issues),
                    'Bank_Status': row['Bank_Match_Status'],
                    'TDS_Status': row['TDS_Match_Status'],
                    'EPF_Status': row['EPF_Match_Status'],
                    'NPS_Status': row['NPS_Match_Status'],
                    'Basic_Salary': row.get(salary_cols.get('basic_salary', ''), 0)
                })
        
        total_employees = len(salary_df)
        print(f"\n✅ 6-File Reconciliation Completed!")
        print(f"📊 Total Employees: {total_employees}")
        print(f"🏦 Bank Matches: {matches['bank']}")
        print(f"💰 TDS Matches: {matches['tds']}")
        print(f"🏛️ EPF Matches: {matches['epf']}")
        print(f"🏛️ NPS Matches: {matches['nps']}")
        print(f"❌ Total Discrepancies: {len(discrepancies)}")
        
        return salary_df, discrepancies, matches
    
    def generate_branch_summary(self, salary_df):
        """Generate branch-wise summary"""
        try:
            # Find salary column
            salary_col = 'Basic'
            for col in salary_df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['basic', 'salary', 'amount']):
                    salary_col = col
                    break
            
            summary = salary_df.groupby('Branch').agg({
                salary_col: ['count', 'sum'],
                'Bank_Match_Status': lambda x: (x == 'Matched').sum(),
                'TDS_Match_Status': lambda x: (x == 'Matched').sum(),
                'EPF_Match_Status': lambda x: (x == 'Matched').sum(),
                'NPS_Match_Status': lambda x: (x == 'Matched').sum()
            }).reset_index()
            
            summary.columns = ['Branch', 'Total_Employees', 'Total_Salary', 'Bank_Matched', 'TDS_Matched', 'EPF_Matched', 'NPS_Matched']
            
            # Calculate match rates
            summary['Bank_Match_Rate_%'] = round((summary['Bank_Matched'] / summary['Total_Employees']) * 100, 2)
            summary['TDS_Match_Rate_%'] = round((summary['TDS_Matched'] / summary['Total_Employees']) * 100, 2)
            summary['EPF_Match_Rate_%'] = round((summary['EPF_Matched'] / summary['Total_Employees']) * 100, 2)
            summary['NPS_Match_Rate_%'] = round((summary['NPS_Matched'] / summary['Total_Employees']) * 100, 2)
            
            return summary
            
        except Exception as e:
            print(f"⚠️ Error in branch summary: {e}")
            return pd.DataFrame()
    
    def generate_designation_summary(self, salary_df):
        """Generate designation-wise summary"""
        try:
            # Find salary column
            salary_col = 'Basic'
            for col in salary_df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['basic', 'salary', 'amount']):
                    salary_col = col
                    break
            
            summary = salary_df.groupby('Designation_Category').agg({
                salary_col: ['count', 'sum', 'mean'],
                'Bank_Match_Status': lambda x: (x == 'Matched').sum(),
                'TDS_Match_Status': lambda x: (x == 'Matched').sum(),
                'EPF_Match_Status': lambda x: (x == 'Matched').sum(),
                'NPS_Match_Status': lambda x: (x == 'Matched').sum()
            }).reset_index()
            
            summary.columns = ['Designation', 'Total_Employees', 'Total_Salary', 'Avg_Salary', 'Bank_Matched', 'TDS_Matched', 'EPF_Matched', 'NPS_Matched']
            
            # Round numeric columns
            for col in ['Total_Salary', 'Avg_Salary']:
                summary[col] = round(summary[col], 2)
            
            return summary.sort_values('Total_Salary', ascending=False)
            
        except Exception as e:
            print(f"⚠️ Error in designation summary: {e}")
            return pd.DataFrame()
    
    def generate_department_summary(self, salary_df):
        """Generate department-wise summary"""
        try:
            # Find salary column
            salary_col = 'Basic'
            for col in salary_df.columns:
                col_lower = str(col).lower()
                if any(term in col_lower for term in ['basic', 'salary', 'amount']):
                    salary_col = col
                    break
            
            summary = salary_df.groupby('Department').agg({
                salary_col: ['count', 'sum', 'mean'],
                'Bank_Match_Status': lambda x: (x == 'Matched').sum(),
                'TDS_Match_Status': lambda x: (x == 'Matched').sum(),
                'EPF_Match_Status': lambda x: (x == 'Matched').sum(),
                'NPS_Match_Status': lambda x: (x == 'Matched').sum()
            }).reset_index()
            
            summary.columns = ['Department', 'Total_Employees', 'Total_Salary', 'Avg_Salary', 'Bank_Matched', 'TDS_Matched', 'EPF_Matched', 'NPS_Matched']
            
            # Round numeric columns
            for col in ['Total_Salary', 'Avg_Salary']:
                summary[col] = round(summary[col], 2)
            
            return summary.sort_values('Total_Salary', ascending=False)
            
        except Exception as e:
            print(f"⚠️ Error in department summary: {e}")
            return pd.DataFrame()
    
    def generate_comprehensive_report(self, files, output_prefix="Complete_Salary_Reconciliation"):
        """Generate comprehensive 6-file reconciliation report"""
        
        # Perform 6-file reconciliation
        salary_df, discrepancies, matches = self.reconcile_six_files(files)
        
        if salary_df is None:
            raise Exception("Reconciliation failed - salary data could not be processed")
        
        # Generate summaries
        branch_summary = self.generate_branch_summary(salary_df)
        designation_summary = self.generate_designation_summary(salary_df)
        department_summary = self.generate_department_summary(salary_df)
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime('%B_%Y')
        output_file = f"{output_prefix}_{timestamp}.xlsx"
        
        print(f"📝 Generating comprehensive Excel report...")
        
        # Create Excel file with multiple tabs
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Tab 1: Complete salary data with all reconciliation status
            salary_df.to_excel(writer, sheet_name='Complete_Salary_Data', index=False)
            
            # Tab 2: Branch Summary
            if not branch_summary.empty:
                branch_summary.to_excel(writer, sheet_name='Branch_Analysis', index=False)
            
            # Tab 3: Designation Summary
            if not designation_summary.empty:
                designation_summary.to_excel(writer, sheet_name='Designation_Analysis', index=False)
            
            # Tab 4: Department Summary
            if not department_summary.empty:
                department_summary.to_excel(writer, sheet_name='Department_Analysis', index=False)
            
            # Tab 5: Discrepancies
            if discrepancies:
                pd.DataFrame(discrepancies).to_excel(writer, sheet_name='Discrepancies_Detail', index=False)
            
            # Tab 6: Overall Summary
            total_employees = len(salary_df)
            total_discrepancies = len(discrepancies)
            
            summary_data = {
                'Metric': [
                    'Total Employees',
                    'Bank Matches',
                    'TDS Matches', 
                    'EPF Matches',
                    'NPS Matches',
                    'Total Discrepancies',
                    'Bank Match Rate (%)',
                    'TDS Match Rate (%)',
                    'EPF Match Rate (%)',
                    'NPS Match Rate (%)',
                    'Overall Compliance Score (%)',
                    'Total Branches',
                    'Total Departments',
                    'Report Generated On'
                ],
                'Value': [
                    total_employees,
                    matches.get('bank', 0),
                    matches.get('tds', 0),
                    matches.get('epf', 0),
                    matches.get('nps', 0),
                    total_discrepancies,
                    f"{round((matches.get('bank', 0)/total_employees)*100, 2)}%",
                    f"{round((matches.get('tds', 0)/total_employees)*100, 2)}%",
                    f"{round((matches.get('epf', 0)/total_employees)*100, 2)}%",
                    f"{round((matches.get('nps', 0)/total_employees)*100, 2)}%",
                    f"{round(sum(matches.values())/(total_employees*4)*100, 2)}%",
                    len(branch_summary) if not branch_summary.empty else 0,
                    len(department_summary) if not department_summary.empty else 0,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Executive_Summary', index=False)
        
        print(f"\n✅ Comprehensive 6-File Reconciliation Report Generated!")
        print(f"📄 Report saved: {output_file}")
        
        return output_file, {
            'total_employees': total_employees,
            'matches': matches,
            'discrepancies': total_discrepancies,
            'branch_summary': branch_summary,
            'designation_summary': designation_summary,
            'department_summary': department_summary
        }

# Main functions for compatibility
def main():
    """Main function for standalone execution"""
    reconciler = EnhancedReconciliation()
    
    # Example file configuration for testing
    files = {
        'salary': 'Salary_Sheet_June_2025.xls',
        'tds': 'TDS_Report_June_2025.xlsx',
        'bank_kotak': 'SOA_KotakOD0317_01-Jul-2025_to_26-Jul-2025.xls',
        'bank_deutsche': 'SOA_DeutscheOD100008_01-Jul-2025_to_26-Jul-2025.xls',
        'epf': None,  # To be uploaded manually
        'nps': None   # To be uploaded manually
    }
    
    # Check which files exist
    available_files = {}
    for file_type, file_path in files.items():
        if file_path and os.path.exists(file_path):
            available_files[file_type] = file_path
        else:
            print(f"⚠️ {file_type} file not available")
    
    if 'salary' not in available_files:
        print("❌ Salary file is required for reconciliation")
        return
    
    try:
        output_file, summary = reconciler.generate_comprehensive_report(available_files)
        print(f"🎉 Success! Check: {output_file}")
        return output_file, summary
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

# Compatibility functions
def perform_reconciliation():
    return main()

def run_reconciliation():
    return main()

def reconcile_with_files(files):
    """Reconcile with specific files"""
    reconciler = EnhancedReconciliation()
    return reconciler.generate_comprehensive_report(files)

if __name__ == "__main__":
    main()
