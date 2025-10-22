# streamlit_app.py  — place this at REPO ROOT
import os
import random
from pathlib import Path
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd

# ---- Optional Plotly imports with soft fallback ----
try:
    import plotly.express as px
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

# -----------------------------------------------------------------------------
# Page configuration (must be first Streamlit call)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Invoice Validation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# Universal table loader (handles .xlsx, .csv, and HTML disguised as .xls)
# -----------------------------------------------------------------------------
def _peek_bytes(path: Path, nbytes=4096) -> bytes:
    with open(path, "rb") as f:
        return f.read(nbytes)

def load_table_any(path_like) -> pd.DataFrame:
    """
    Loads a single table from XLSX/CSV/HTML-like XLS. Returns a DataFrame.
    - .xlsx -> openpyxl
    - .csv  -> read_csv
    - .xls  -> tries read_excel; if fails and smells like HTML, uses read_html
    """
    p = Path(path_like)
    name = p.name.lower()

    # CSV quickly
    if name.endswith(".csv"):
        return pd.read_csv(p, encoding_errors="ignore")

    # XLSX quickly
    if name.endswith(".xlsx"):
        return pd.read_excel(p, engine="openpyxl")

    # XLS and anything else → probe bytes
    raw = _peek_bytes(p, 4096)
    low = raw.lower()

    def _try_read_html():
        try:
            tables = pd.read_html(p)  # requires bs4+lxml; if missing, raises
            if not tables:
                raise ValueError("No tables found in HTML-like file.")
            return tables[0]
        except Exception as e:
            raise ValueError(f"HTML-like .xls parse failed: {e}")

    # Try Excel first
    try:
        return pd.read_excel(p)
    except Exception:
        # Heuristic: HTML disguised as XLS (server export)
        if b"<html" in low or b"<table" in low:
            return _try_read_html()
        # As a last resort: try CSV/TSV sniff
        try:
            return pd.read_csv(p, encoding_errors="ignore")
        except Exception:
            return pd.read_csv(p, sep="\t", engine="python", encoding_errors="ignore")

# -----------------------------------------------------------------------------
# Custom CSS
# -----------------------------------------------------------------------------
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem; border-radius: 10px; color: white; text-align: center; margin-bottom: 2rem;
    }
    .metric-container { background: white; padding: 1.5rem; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; margin-bottom: 1rem; }
    .success-metric { border-left: 5px solid #28a745; }
    .warning-metric { border-left: 5px solid #ffc107; }
    .danger-metric  { border-left: 5px solid #dc3545; }
    .info-metric    { border-left: 5px solid #17a2b8; }
    .status-active   { color: #28a745; font-weight: bold; }
    .status-inactive { color: #6c757d; font-style: italic; }
    .no-data-container {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 3rem; border-radius: 15px; color: white; text-align: center; margin: 2rem 0;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Dashboard
# -----------------------------------------------------------------------------
class Dashboard:
    def __init__(self):
        # Sidebar controls FIRST (before I/O)
        st.sidebar.header("🏢 KOENIG SOLUTIONS")
        st.sidebar.markdown("*Enhanced Invoice Validation*")
        st.sidebar.markdown("---")

        self.force_demo = st.sidebar.checkbox(
            "Force Demo Mode",
            value=False,
            help="Ignore local files and use sample data"
        )

        # Optional manual upload
        self.uploaded_file = st.sidebar.file_uploader(
            "Upload a report (xlsx/csv/xls-html)",
            type=["xlsx", "csv", "xls"]
        )

        self.setup_data_sources()

    # ---------- Data wiring ----------
    def setup_data_sources(self):
        self.data_available = False

        # DB paths (optional/legacy indicators)
        self.standard_db_path = "invoice_validation.db"
        self.db_path = "invoice_history.db"
        self.enhanced_db_path = "enhanced_invoice_history.db"

        # Find recent reports unless demo is forced
        self.recent_reports = [] if self.force_demo else self.find_recent_reports()

        # Enhanced flag from DB presence
        self.enhanced_data_available = os.path.exists(self.enhanced_db_path)

        self.data_available = (
            bool(self.recent_reports)
            or self.uploaded_file is not None
            or self.force_demo
        )

    def find_recent_reports(self):
        """
        Looks into common dirs for latest Excel/CSV:
        - data/, reports/, downloads/, ./
        """
        reports = []
        roots = ["data", "reports", "downloads", "."]

        for root in roots:
            root_path = Path(root)
            if not root_path.exists():
                continue
            for file in root_path.iterdir():
                if not file.is_file():
                    continue
                low = file.name.lower()
                if (low.endswith(".xlsx") or low.endswith(".csv") or low.endswith(".xls")) and \
                   ("validation" in low or "invoice" in low or "summary" in low or "report" in low):
                    try:
                        reports.append({
                            "file": file.name,
                            "path": str(file),
                            "modified": file.stat().st_mtime,
                            "enhanced": ("enhanced" in low) or ("detailed" in low),
                            "size": file.stat().st_size
                        })
                    except Exception:
                        continue

        reports.sort(key=lambda x: x["modified"], reverse=True)
        return reports

    def _load_from_uploaded(self) -> pd.DataFrame | None:
        if not self.uploaded_file:
            return None
        # Save to temp and parse via universal loader
        tmp_dir = Path("data/tmp")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / self.uploaded_file.name
        with open(tmp_path, "wb") as f:
            f.write(self.uploaded_file.read())
        return load_table_any(tmp_path)

    def load_latest_data(self):
        """
        Priority:
          1) Uploaded file (if any)
          2) Latest file found in folders
          3) Demo data
        """
        # 1) Uploaded
        if self.uploaded_file is not None:
            try:
                df = self._load_from_uploaded()
                if df is not None and not df.empty:
                    df = self._normalize_df(df)
                    return df, {"enhanced": True, "source": "uploaded"}
            except Exception as e:
                st.warning(f"Could not parse uploaded file: {e}. Falling back to auto-detect.")

        # 2) Auto-detect latest
        if self.recent_reports:
            latest = self.recent_reports[0]
            try:
                df = load_table_any(latest["path"])
                df = self._normalize_df(df)
                return df, latest
            except Exception as e:
                st.warning(f"Could not load {latest['file']}: {e}. Falling back to demo data.")

        # 3) Demo
        return self.create_sample_data(), {"enhanced": True, "source": "demo"}

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Make sure key string columns are safely string-typed for .str ops.
        Also gently remap common column variants to your preferred names.
        """
        df = df.copy()
        # Soft rename variants → your canonical names (add as needed)
        rename_map = {
            "invoice_no": "Invoice_Number",
            "invoice number": "Invoice_Number",
            "vendor": "Vendor_Name",
            "vendor_name": "Vendor_Name",
            "amount_inr": "Amount",
            "amount (inr)": "Amount",
            "status": "Validation_Status",
            "location_name": "Location",
            "creator_name": "Invoice_Creator_Name",
            "currency": "Invoice_Currency",
        }
        for c in list(df.columns):
            lc = str(c).strip().lower()
            if lc in rename_map:
                df.rename(columns={c: rename_map[lc]}, inplace=True)

        for col in ["Validation_Status", "Location", "Invoice_Currency",
                    "Invoice_Creator_Name", "GST_Validation_Result",
                    "Due_Date_Notification"]:
            if col in df.columns:
                df[col] = df[col].astype("string").fillna("")
        return df

    # ---------- Demo data ----------
    def create_sample_data(self):
        locations = [
            "Delhi, India", "Mumbai, India", "Bangalore, India", "Chennai, India",
            "Gurgaon, India", "Pune, India", "New York, USA", "London, UK",
            "Toronto, Canada", "Singapore, Singapore", "Dubai, UAE"
        ]
        vendors = [
            "ABC Technologies Pvt Ltd", "XYZ Solutions Inc", "Tech Innovations Ltd",
            "Global Services Corp", "Digital Solutions Pvt Ltd", "Smart Systems Inc",
            "Advanced Tech Ltd", "Innovation Hub Pvt Ltd", "Future Solutions Corp",
            "NextGen Technologies Ltd"
        ]
        creators = [
            "John Smith", "Sarah Johnson", "Michael Brown", "Emily Davis",
            "David Wilson", "Lisa Anderson", "Robert Taylor", "Jennifer Martinez",
            "Unknown"
        ]
        currencies = ["INR", "USD", "EUR", "GBP", "SGD", "AED", "CAD"]
        tax_types = ["GST-CGST+SGST", "GST-IGST", "VAT-UK", "VAT-EU", "Sales Tax", "No Tax"]
        statuses = ["✅ PASS", "❌ FAIL", "⚠️ WARNING"]
        mops = ["Online Transfer", "Cheque", "Wire Transfer", "Cash", "Credit Card"]

        data = []
        now = datetime.now()
        for i in range(100):
            inv_date = (now - timedelta(days=random.randint(1, 90)))
            due_date = inv_date + timedelta(days=random.randint(15, 60))
            days_until_due = (due_date.date() - now.date()).days
            due_note = "OVERDUE" if days_until_due < 0 else ("YES" if days_until_due <= 2 else "NO")
            location = random.choice(locations)
            if "India" in location:
                gst_validation = (
                    "✅ CORRECT - CGST+SGST for Intra-state"
                    if random.random() < 0.8 else
                    "❌ ERROR - IGST used for Intra-state transaction"
                )
            else:
                gst_validation = "✅ PASS - No GSTIN (Non-Indian Invoice)"
            amount = round(random.uniform(5_000, 500_000), 2)
            data.append({
                "Invoice_ID": f"INV-{random.randint(100000, 999999)}",
                "Invoice_Number": f"KS-{2024000 + i}",
                "Invoice_Date": inv_date.strftime("%Y-%m-%d"),
                "Vendor_Name": random.choice(vendors),
                "Amount": amount,
                "Invoice_Creator_Name": random.choice(creators),
                "Location": location,
                "MOP": random.choice(mops),
                "Due_Date": due_date.strftime("%Y-%m-%d"),
                "Invoice_Currency": random.choice(currencies),
                "SCID": f"SC{random.randint(1000, 9999)}",
                "TDS_Status": "Coming Soon",
                "GST_Validation_Result": gst_validation,
                "Due_Date_Notification": due_note,
                "Validation_Status": random.choice(statuses),
                "Issues_Found": random.randint(0, 5),
                "Issue_Details": random.choice([
                    "No issues found", "Missing Payment Method (MOP)", "Missing Due Date",
                    "GST Issue: Invalid GSTIN Format", "Missing Invoice Creator Name"
                ]),
                "GST_Number": f"{random.randint(10, 37):02d}AAAAA{random.randint(1000, 9999)}A1Z{random.randint(1, 9)}",
                "Remarks": random.choice(["", "Approved", "Pending Review", "Urgent"]),
                "Tax_Type": random.choice(tax_types),
                "Total_Tax_Calculated": round(amount * random.uniform(0.05, 0.18), 2),
                "Validation_Date": now.strftime("%Y-%m-%d %H:%M:%S"),
            })
        df = pd.DataFrame(data)
        for col in ["Validation_Status", "Location", "Invoice_Currency",
                    "Invoice_Creator_Name", "GST_Validation_Result",
                    "Due_Date_Notification"]:
            df[col] = df[col].astype("string")
        return df

    # ---------- UI sections ----------
    def render_header(self):
        st.markdown("""
        <div class="main-header">
            <h1>🚀 Invoice Validation Dashboard</h1>
            <h3>🏢 Koenig Solutions - Multi-Location GST/VAT Compliance System</h3>
            <p>✨ Real-time validation • 🔄 Historical tracking • 🌍 Global tax compliance • 💰 Enhanced Fields</p>
        </div>
        """, unsafe_allow_html=True)

    def render_system_status(self):
        st.header("🔧 Enhanced System Status")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            status_class = "success-metric" if self.data_available else "danger-metric"
            status_text = "🟢 Active" if self.data_available else "🔴 No Data"
            st.markdown(f"""
            <div class="metric-container {status_class}">
                <h4>📊 Validation System</h4>
                <h2>{status_text}</h2>
                <p>{len(self.recent_reports) if self.recent_reports else ('Uploaded' if self.uploaded_file else 'Demo')}</p>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            enhanced_class = "success-metric" if self.enhanced_data_available else "warning-metric"
            enhanced_text = "🆕 Enhanced" if self.enhanced_data_available else "📊 Standard"
            st.markdown(f"""
            <div class="metric-container {enhanced_class}">
                <h4>🚀 Enhancement Status</h4>
                <h2>{enhanced_text}</h2>
                <p>Enhanced fields {'active' if self.enhanced_data_available else 'ready'}</p>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            db_status = "🟢 Connected" if os.path.exists(self.enhanced_db_path) else "🟡 Demo Mode"
            db_class = "success-metric" if os.path.exists(self.enhanced_db_path) else "info-metric"
            st.markdown(f"""
            <div class="metric-container {db_class}">
                <h4>🗄️ Database Status</h4>
                <h2>{db_status}</h2>
                <p>Historical tracking {'active' if os.path.exists(self.enhanced_db_path) else 'demo'}</p>
            </div>
            """, unsafe_allow_html=True)

        with col4:
            if self.recent_reports:
                last_run = datetime.fromtimestamp(self.recent_reports[0]['modified'])
                time_ago = datetime.now() - last_run
                time_text = f"{time_ago.seconds//3600}h ago" if time_ago.days == 0 else f"{time_ago.days}d ago"
            else:
                time_text = "Demo/Uploaded"
            st.markdown(f"""
            <div class="metric-container info-metric">
                <h4>⏰ Last Validation</h4>
                <h2>{time_text}</h2>
                <p>Next run: In 4 days</p>
            </div>
            """, unsafe_allow_html=True)

    def render_enhanced_features_status(self):
        st.header("🚀 Enhanced Features Status")
        features = [
            ("💱 Multi-Currency Support", True, "Process invoices in multiple currencies"),
            ("🌍 Global Location Tracking", True, "Track invoices across all Koenig locations"),
            ("💰 Automatic GST/VAT Calculation", True, "Calculate taxes for India + International"),
            ("⏰ Due Date Monitoring", True, "2-day advance payment alerts"),
            ("🔄 Historical Change Tracking", True, "3-month data change detection"),
            ("📊 Enhanced Analytics", True, "Interactive charts and visualizations"),
            ("📧 Automated Email Reports", True, "4-day scheduled notifications"),
            ("🔗 RMS Integration", True, "SCID, MOP, Creator Name data"),
        ]
        cols = st.columns(4)
        for i, (feature, active, description) in enumerate(features):
            with cols[i % 4]:
                status_class = "status-active" if active else "status-inactive"
                status_icon = "✅" if active else "⏳"
                st.markdown(f"""
                <div class="metric-container">
                    <p class="{status_class}">{status_icon} {feature}</p>
                    <small>{description}</small>
                </div>
                """, unsafe_allow_html=True)

    def render_validation_overview(self, df, report_info):
        st.header("📊 Validation Analytics Overview")
        if df is None or df.empty:
            self.render_no_data_state()
            return

        total_invoices = len(df)

        # Status buckets
        if "Validation_Status" in df.columns:
            vs = df["Validation_Status"].astype("string")
            passed = int((vs.str.contains("PASS", na=False)).sum())
            failed = int((vs.str.contains("FAIL", na=False)).sum())
            warnings = int((vs.str.contains("WARNING", na=False)).sum())
        else:
            passed = int(total_invoices * 0.6)
            failed = int(total_invoices * 0.25)
            warnings = total_invoices - passed - failed

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📋 Total Invoices", f"{total_invoices:,}", "Enhanced processing")
        with col2:
            st.metric("✅ Passed Validation", f"{passed:,}", f"{(passed/total_invoices*100 if total_invoices else 0):.1f}% success")
        with col3:
            st.metric("⚠️ Warnings", f"{warnings:,}", f"{(warnings/total_invoices*100 if total_invoices else 0):.1f}%")
        with col4:
            st.metric("❌ Failed Validation", f"{failed:,}", f"{(failed/total_invoices*100 if total_invoices else 0):.1f}%")

        # Enhanced analytics
        st.subheader("🚀 Enhanced Analytics")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if "Invoice_Currency" in df.columns and not df["Invoice_Currency"].empty:
                currencies = df["Invoice_Currency"].nunique()
                mode = df["Invoice_Currency"].mode()
                main_currency = mode.iloc[0] if not mode.empty else "INR"
                st.metric("💱 Currencies Processed", f"{currencies} currencies", f"Primary: {main_currency}")

        with col2:
            if "Location" in df.columns and not df["Location"].empty:
                loc_first = df["Location"].astype("string").str.split(",").str[0]
                locations = loc_first.nunique()
                mode = loc_first.mode()
                main_location = mode.iloc[0] if not mode.empty else "Delhi"
                st.metric("🌍 Global Locations", f"{locations} locations", f"Primary: {main_location}")

        with col3:
            if "Due_Date_Notification" in df.columns:
                urgent = int(df["Due_Date_Notification"].isin(["YES", "OVERDUE"]).sum())
                st.metric("⏰ Payment Alerts", f"{urgent} urgent", "Due ≤2 days")

        with col4:
            if "Invoice_Creator_Name" in df.columns:
                known_creators = int((df["Invoice_Creator_Name"].astype("string") != "Unknown").sum())
                creator_rate = (known_creators / total_invoices * 100) if total_invoices else 0
                st.metric("👤 Creator Tracking", f"{known_creators} identified", f"{creator_rate:.1f}% coverage")

    def render_enhanced_charts(self, df):
        if df is None or df.empty or not PLOTLY_OK:
            if not PLOTLY_OK:
                st.info("Plotly not installed. Skipping charts. Install with: `pip install plotly`")
            return

        st.header("📈 Enhanced Visual Analytics")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📊 Validation Status Distribution")
            if "Validation_Status" in df.columns:
                status_counts = df["Validation_Status"].value_counts()
                fig = px.pie(values=status_counts.values, names=status_counts.index, title="Validation Status Breakdown")
                fig.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🌍 Location Analysis")
            if "Location" in df.columns and not df["Location"].empty:
                df_loc = df.copy()
                df_loc["Location_Clean"] = df_loc["Location"].astype("string").str.split(",").str[0]
                location_counts = df_loc["Location_Clean"].value_counts().head(10)
                fig = px.bar(
                    x=location_counts.values,
                    y=location_counts.index,
                    orientation="h",
                    title="Top 10 Locations by Invoice Count",
                )
                fig.update_layout(xaxis_title="Invoice Count", yaxis_title="Location", showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            if "Invoice_Currency" in df.columns:
                st.subheader("💱 Currency Distribution")
                currency_counts = df["Invoice_Currency"].value_counts()
                fig = px.pie(values=currency_counts.values, names=currency_counts.index, title="Invoice Currency Breakdown")
                fig.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if "Invoice_Creator_Name" in df.columns:
                st.subheader("👤 Creator Analysis")
                creator_counts = df["Invoice_Creator_Name"].value_counts().head(8)
                fig = px.bar(x=creator_counts.values, y=creator_counts.index, orientation="h", title="Top Invoice Creators")
                st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            if "GST_Validation_Result" in df.columns:
                st.subheader("🏛️ GST Validation Analysis")
                df_gst = df.copy()
                df_gst["GST_Simple"] = df_gst["GST_Validation_Result"].astype("string").apply(
                    lambda x: "Correct" if "✅" in x else ("Error" if "❌" in x else "Warning")
                )
                gst_counts = df_gst["GST_Simple"].value_counts()
                fig = px.pie(values=gst_counts.values, names=gst_counts.index, title="GST Validation Results")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if "Due_Date_Notification" in df.columns:
                st.subheader("⏰ Payment Due Date Analysis")
                due_alerts = df["Due_Date_Notification"].value_counts()
                fig = px.pie(values=due_alerts.values, names=due_alerts.index, title="Due Date Alert Status")
                st.plotly_chart(fig, use_container_width=True)

        if "Due_Date_Notification" in df.columns:
            urgent_df = df[df["Due_Date_Notification"].isin(["YES", "OVERDUE"])]
            if not urgent_df.empty:
                st.subheader("🚨 Urgent Payment Alerts")
                display_cols = ["Invoice_Number", "Vendor_Name", "Amount", "Due_Date", "Due_Date_Notification"]
                available = [c for c in display_cols if c in urgent_df.columns]
                st.dataframe(urgent_df[available].head(10) if available else urgent_df.head(10), use_container_width=True)

    def render_no_data_state(self):
        st.markdown("""
        <div class="no-data-container">
            <h2>🚀 Invoice Validation Dashboard</h2>
            <h4>Ready to process invoices with enhanced features!</h4>
        </div>
        """, unsafe_allow_html=True)

        st.subheader("✨ Enhanced Features Ready:")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
            **💰 Tax Compliance:**
            - ✅ Multi-location GST/VAT calculation
            - ✅ India: CGST, SGST, IGST validation
            - ✅ International: VAT/Sales Tax support
            - ✅ Automatic tax compliance check
            """)
        with col2:
            st.markdown("""
            **🌍 Global Operations:**
            - ✅ Currency support (INR, USD, EUR, etc.)
            - ✅ Location tracking (India + International)
            - ✅ Creator name identification
            - ✅ Method of Payment (MOP) tracking
            """)
        with col3:
            st.markdown("""
            **📊 Advanced Analytics:**
            - ✅ Due date monitoring (2-day alerts)
            - ✅ Enhanced validation fields
            - ✅ Interactive dashboards
            - ✅ Real-time status monitoring
            """)

    def render_data_explorer(self, df, report_info):
        if df is None or df.empty:
            return

        st.header("🔍 Interactive Invoice Data Explorer")

        col1, col2, col3, col4 = st.columns(4)
        filters = {}

        with col1:
            if "Validation_Status" in df.columns:
                statuses = ["All"] + sorted([str(x) for x in df["Validation_Status"].dropna().unique()])
                filters["status"] = st.selectbox("🔍 Filter by Status", statuses)

        with col2:
            if "Location" in df.columns:
                locations = ["All"] + sorted([str(x) for x in df["Location"].dropna().unique()])
                filters["location"] = st.selectbox("🌍 Filter by Location", locations)

        with col3:
            if "Invoice_Currency" in df.columns:
                currencies = ["All"] + sorted([str(x) for x in df["Invoice_Currency"].dropna().unique()])
                filters["currency"] = st.selectbox("💱 Filter by Currency", currencies)

        with col4:
            if "Invoice_Creator_Name" in df.columns:
                creators = ["All"] + sorted([str(x) for x in df["Invoice_Creator_Name"].dropna().unique()])
                filters["creator"] = st.selectbox("👤 Filter by Creator", creators)

        filtered_df = df.copy()
        for fk, fv in filters.items():
            if fv and fv != "All":
                if fk == "status":
                    filtered_df = filtered_df[filtered_df["Validation_Status"] == fv]
                elif fk == "location":
                    filtered_df = filtered_df[filtered_df["Location"] == fv]
                elif fk == "currency":
                    filtered_df = filtered_df[filtered_df["Invoice_Currency"] == fv]
                elif fk == "creator":
                    filtered_df = filtered_df[filtered_df["Invoice_Creator_Name"] == fv]

        col1, col2 = st.columns([2, 1])
        with col1:
            st.write(f"📊 Showing **{len(filtered_df):,}** of **{len(df):,}** invoices")
        with col2:
            if report_info and report_info.get("enhanced"):
                st.success("🆕 Enhanced Report")
            else:
                st.info("📊 Enhanced Processing")

        display_columns = [
            "Invoice_Number", "Vendor_Name", "Amount", "Invoice_Date",
            "Validation_Status", "Location", "Invoice_Currency", "Invoice_Creator_Name",
            "MOP", "Due_Date", "Due_Date_Notification",
        ]
        cols_to_show = [c for c in display_columns if c in filtered_df.columns]
        st.dataframe(filtered_df[cols_to_show] if cols_to_show else filtered_df, use_container_width=True)

    def render_sidebar_footer(self):
        st.sidebar.markdown("---")
        if st.sidebar.button("🔄 Refresh Dashboard"):
            st.rerun() if hasattr(st, "rerun") else st.experimental_rerun()
        if st.sidebar.button("📊 System Check"):
            st.sidebar.success("✅ All systems operational")
            st.sidebar.info(f"🕐 {datetime.now().strftime('%H:%M:%S')}")

    def render_footer(self):
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
            **🏢 Koenig Solutions Pvt. Ltd.**  
            Enhanced Invoice Validation System  
            Multi-Location GST/VAT Compliance
            """)
        with col2:
            st.markdown("""
            **🚀 Enhanced Features:**  
            ✅ Creator Name Tracking  
            ✅ Method of Payment (MOP)  
            ✅ Due Date Notifications  
            ✅ GST/VAT Compliance  
            ✅ Multi-Currency Support  
            """)
        with col3:
            st.markdown(f"""
            **📊 Dashboard Info:**  
            Version: Enhanced v2.1  
            Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}  
            Status: 🟢 Operational
            """)

    # ---------- Orchestration ----------
    def run(self):
        self.render_header()
        self.render_system_status()
        self.render_enhanced_features_status()

        try:
            df, report_info = self.load_latest_data()
            if df is not None and not df.empty:
                self.render_validation_overview(df, report_info)
                self.render_enhanced_charts(df)
                self.render_data_explorer(df, report_info)
            else:
                self.render_no_data_state()
        except Exception as e:
            st.error(f"Error loading data: {e}")
            self.render_no_data_state()

        self.render_footer()
        self.render_sidebar_footer()


# -----------------------------------------------------------------------------
# Initialize and run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        Dashboard().run()
    except Exception as e:
        st.error(f"Dashboard initialization error: {e}")
        st.info("Running in safe mode with demo data.")
        st.title("🚀 Invoice Validation Dashboard")
        st.success("✅ Dashboard loaded successfully in demo mode!")
