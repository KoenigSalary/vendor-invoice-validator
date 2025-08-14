#!/usr/bin/env python3
"""
main.py — Unified Orchestrator (imports all scripts) — PATCHED
- Fix: guard getattr() for subparser-only args when defaulting to "all"
- Same behavior otherwise.
"""
from __future__ import annotations
import os, sys, argparse, logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Callable, Iterable, Optional
from dotenv import load_dotenv

def _safe_import(name: str):
    try:
        module = __import__(name)
        return module, None
    except Exception as e:
        return None, e

rms_login, _imp_err_login = _safe_import("rms_login")
rms_salary_download, _imp_err_sal = _safe_import("rms_salary_download")
rms_tds_download, _imp_err_tds = _safe_import("rms_tds_download")
rms_bank_soa_download, _imp_err_bank = _safe_import("rms_bank_soa_download")
salary_reconciliation_agent, _imp_err_reco = _safe_import("salary_reconciliation_agent")
auto_email, _imp_err_mail = _safe_import("auto_email")

IST = ZoneInfo("Asia/Kolkata")

def setup_logging(level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); logger.addHandler(sh)
    fh = RotatingFileHandler("logs/agent.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8"); fh.setFormatter(fmt); logger.addHandler(fh)

def now_ist() -> datetime: return datetime.now(tz=IST)
def previous_month(d: date) -> tuple[int, str, int]:
    first = d.replace(day=1); prev_last = first - timedelta(days=1)
    return prev_last.month, prev_last.strftime("%B"), prev_last.year

def resolve_func(mod: Any, candidates: Iterable[str]) -> Optional[Callable]:
    if not mod: return None
    for name in candidates:
        fn = getattr(mod, name, None)
        if callable(fn): return fn
    return None

def require_env(keys: list[str]) -> dict:
    load_dotenv()
    vals = {k: os.getenv(k) for k in keys}
    if not vals.get("RMS_USERNAME"): vals["RMS_USERNAME"] = os.getenv("RMS_USER")
    if not vals.get("RMS_PASSWORD"): vals["RMS_PASSWORD"] = os.getenv("RMS_PASS")
    if not vals.get("RMS_USERNAME") or not vals.get("RMS_PASSWORD"):
        logging.warning("RMS_USERNAME/RMS_PASSWORD not set (or RMS_USER/RMS_PASS). If downloads require login, set them in .env.")
    return vals

def action_download(salary_month_name: str | None, salary_year: int | None) -> None:
    # Ensure compatibility modules exist
    ensure_compatibility()
    
    t = now_ist().date()
    if not (salary_month_name and salary_year):
        _, m_name, y = previous_month(t); salary_month_name = salary_month_name or m_name; salary_year = salary_year or y
    logging.info(f"[DOWNLOAD] Target period: {salary_month_name} {salary_year}")
    env = require_env(["RMS_USERNAME", "RMS_PASSWORD", "DOWNLOAD_DIR", "CHROMEDRIVER_PATH"])

    make_driver = resolve_func(rms_login, ["make_driver", "get_driver", "init_driver"])
    login_rms = resolve_func(rms_login, ["login_rms", "login", "do_login"])

    driver = None
    if make_driver:
        logging.info("Creating Selenium driver via rms_login…")
        try:
            if "download_dir" in getattr(make_driver, "__code__", type("c", (), {"co_varnames":()})()).co_varnames:
                driver = make_driver(download_dir=env.get("DOWNLOAD_DIR"), driver_path=env.get("CHROMEDRIVER_PATH"))
            else:
                driver = make_driver()
        except Exception as e:
            logging.warning(f"make_driver failed: {e}; continuing without explicit driver.")
            driver = None

    try:
        if login_rms and driver and env.get("RMS_USERNAME") and env.get("RMS_PASSWORD"):
            try:
                ok = login_rms(driver, env["RMS_USERNAME"], env["RMS_PASSWORD"])
                if not ok: logging.warning("login_rms returned falsy; continuing but downloads may fail.")
            except Exception as e:
                logging.warning(f"login_rms raised: {e}; continuing.")

        # Use the unified downloader if available
        if rms_bank_soa_download:
            try:
                logging.info("Using unified rms_downloader...")
                
                # Try to call individual functions with the shared driver
                if hasattr(rms_bank_soa_download, 'export_salary_sheet'):
                    logging.info("Downloading Salary sheet…")
                    rms_bank_soa_download.export_salary_sheet(driver, salary_month_name, int(salary_year))
                
                if hasattr(rms_bank_soa_download, 'export_tds'):
                    logging.info("Downloading TDS…") 
                    rms_bank_soa_download.export_tds(driver, salary_month_name, int(salary_year))
                
                if hasattr(rms_bank_soa_download, 'export_bank_soa_for_salary_month'):
                    logging.info("Downloading Bank SOA…")
                    rms_bank_soa_download.export_bank_soa_for_salary_month(driver, salary_month_name, int(salary_year))
                
                logging.info("✅ Downloads completed via unified downloader")
                return
                
            except Exception as e:
                logging.error(f"Unified downloader failed: {e}")

                # Fallback to individual downloads
                sal_fn = resolve_func(rms_salary_download, ["download_salary", "export_salary", "export_salary_sheet"])
                if sal_fn:
                    logging.info("Downloading Salary sheet…")
                    safe_call_with_driver(sal_fn, driver, salary_month_name, int(salary_year))
                else:
                    logging.warning("No salary download function found in rms_salary_download.py")

                tds_fn = resolve_func(rms_tds_download, ["download_tds", "export_tds"])
                if tds_fn:
                    logging.info("Downloading TDS paid…")
                    safe_call_with_driver(tds_fn, driver, salary_month_name, int(salary_year))
                else:
                    logging.warning("No TDS download function found in rms_tds_download.py")

                bank_fn = get_bank_soa_function()
                if bank_fn:
                    logging.info("Downloading Bank SOA… (Kotak + Deutsche expected)")
                    safe_call_with_driver(bank_fn, driver, salary_month_name, int(salary_year))
                else:
                    logging.warning("No Bank SOA function found")
        else:
            # Individual module approach
            sal_fn = resolve_func(rms_salary_download, ["download_salary", "export_salary", "export_salary_sheet"])
            if sal_fn:
                logging.info("Downloading Salary sheet…")
                safe_call_with_driver(sal_fn, driver, salary_month_name, int(salary_year))
            else:
                logging.warning("No salary download function found in rms_salary_download.py")

            tds_fn = resolve_func(rms_tds_download, ["download_tds", "export_tds"])
            if tds_fn:
                logging.info("Downloading TDS paid…")
                safe_call_with_driver(tds_fn, driver, salary_month_name, int(salary_year))
            else:
                logging.warning("No TDS download function found in rms_tds_download.py")

            bank_fn = get_bank_soa_function()
            if bank_fn:
                logging.info("Downloading Bank SOA… (Kotak + Deutsche expected)")
                safe_call_with_driver(bank_fn, driver, salary_month_name, int(salary_year))
            else:
                logging.warning("No Bank SOA function found")
                
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass

    logging.info("Download step completed.")

def action_all(month_name: str | None, year: int | None, skip_download: bool = False) -> None:
    """Run the complete workflow: download → reconcile → email"""
    logging.info(f"Starting complete workflow for {month_name or 'previous month'} {year or 'auto-detect year'}")
    
    try:
        # Step 1: Download (unless skipped)
        if not skip_download:
            logging.info("Step 1: Downloading data...")
            action_download(month_name, year)
        else:
            logging.info("Step 1: Download skipped as requested")
        
        # Step 2: Reconciliation
        logging.info("Step 2: Running reconciliation...")
        report_path = action_reconcile()
        if report_path:
            logging.info(f"✅ Reconciliation completed: {report_path}")
        
        # Step 3: Email
        logging.info("Step 3: Sending email...")
        action_email()
        
        logging.info("✅ Complete workflow finished successfully!")
        
    except Exception as e:
        logging.error(f"❌ Workflow failed at some step: {str(e)}")
        raise e

def action_reconcile() -> str:
    reco_fn = resolve_func(salary_reconciliation_agent, ["perform_reconciliation","run_reconciliation","main"])
    if not reco_fn: raise RuntimeError("No reconciliation entrypoint found in salary_reconciliation_agent.py")
    logging.info("Starting reconciliation…")
    result = reco_fn()
    month = now_ist().strftime("%B"); year = now_ist().year
    candidates = [os.path.join("output", f"Salary_Reconciliation_Report_{month}_{year}.xlsx"),
                  os.path.join("SalaryReports", now_ist().strftime("%Y-%m-%d"), "reconciliation_result.xlsx")]
    out_path = next((p for p in candidates if os.path.exists(p)), "")
    if out_path: logging.info(f"Reconciliation report: {out_path}")
    else: logging.warning("Reconciliation finished, but report path not found in common locations.")
    return out_path or (result if isinstance(result, str) else "")

def action_email() -> None:
    mail_fn = resolve_func(auto_email, ["send_email","main","run"])
    if not mail_fn: raise RuntimeError("No email entrypoint found in auto_email.py")
    logging.info("Sending reconciliation email…"); mail_fn(); logging.info("Email step completed.")

def action_download(salary_month_name: str | None, salary_year: int | None) -> None:
    # Ensure compatibility modules exist
    ensure_compatibility()
    
    t = now_ist().date()
    if not (salary_month_name and salary_year):
        _, m_name, y = previous_month(t); salary_month_name = salary_month_name or m_name; salary_year = salary_year or y
    logging.info(f"[DOWNLOAD] Target period: {salary_month_name} {salary_year}")
    env = require_env(["RMS_USERNAME", "RMS_PASSWORD", "DOWNLOAD_DIR", "CHROMEDRIVER_PATH"])

    # Use the unified downloader directly
    if rms_bank_soa_download:
        try:
            logging.info("Using unified rms_bank_soa_download...")
            
            # Set environment variables for the unified downloader
            os.environ['SALARY_MONTH'] = salary_month_name
            os.environ['SALARY_YEAR'] = str(salary_year)
            
            # Call the main function from rms_bank_soa_download
            result = rms_bank_soa_download.main()
            
            if result is None:  # main() completed successfully
                logging.info("✅ All downloads completed via unified downloader")
            else:
                logging.warning("⚠️ Unified downloader completed with warnings")
                
        except Exception as e:
            logging.error(f"❌ Unified downloader failed: {e}")
    else:
        logging.error("❌ rms_bank_soa_download module not available")

    logging.info("Download step completed.")

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Salary Reconciliation Agent Orchestrator (imports all scripts)")
    sub = p.add_subparsers(dest="command", required=False)
    p_dl = sub.add_parser("download", help="Download Salary, TDS, Bank SOA for the given/previous month")
    p_dl.add_argument("--month-name", help="Month name, e.g., July"); p_dl.add_argument("--year", type=int, help="Four-digit year, e.g., 2025")
    sub.add_parser("reconcile", help="Run reconciliation (salary_reconciliation_agent.py)")
    sub.add_parser("email", help="Send the final reconciliation email (auto_email.py)")
    p_all = sub.add_parser("all", help="Run download → reconcile → email")
    p_all.add_argument("--month-name", help="Month name, e.g., July"); p_all.add_argument("--year", type=int, help="Four-digit year, e.g., 2025")
    p_all.add_argument("--skip-download", action="store_true", help="Skip download step")
    p.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    return p.parse_args(argv)

def main(argv: list[str]) -> int:
    args = parse_args(argv); setup_logging(args.log_level)
    cmd = getattr(args, "command", None) or "all"
    logging.info(f"Command: {cmd}")
    try:
        if cmd == "download":
            month_name = getattr(args, "month_name", None); year = getattr(args, "year", None)
            action_download(month_name, year)
        elif cmd == "reconcile":
            action_reconcile()
        elif cmd == "email":
            action_email()
        elif cmd == "all":
            month_name = getattr(args, "month_name", None); year = getattr(args, "year", None)
            skip_download = getattr(args, "skip_download", False)
            action_all(month_name, year, skip_download)
        else:
            logging.error(f"Unknown command: {cmd}"); return 2
        return 0
    except Exception:
        logging.exception("Fatal error"); return 1

# =========================
# COMPATIBILITY FUNCTIONS FOR MAIN.PY
# =========================

def create_missing_modules():
    """Create missing module files with compatibility functions"""
    
    # Create rms_salary_download.py if it doesn't exist
    if not os.path.exists("rms_salary_download.py"):
        with open("rms_salary_download.py", "w") as f:
            f.write('''#!/usr/bin/env python3
from rms_downloader import export_salary_sheet

def download_salary(driver, month_name, year):
    return export_salary_sheet(driver, month_name, year)

def export_salary(driver, month_name, year):
    return export_salary_sheet(driver, month_name, year)

__all__ = ['download_salary', 'export_salary', 'export_salary_sheet']
''')
    
    # Create rms_tds_download.py if it doesn't exist  
    if not os.path.exists("rms_tds_download.py"):
        with open("rms_tds_download.py", "w") as f:
            f.write('''#!/usr/bin/env python3
from rms_downloader import export_tds

def download_tds(driver, month_name, year):
    return export_tds(driver, month_name, year)

__all__ = ['download_tds', 'export_tds']
''')

def ensure_compatibility():
    """Ensure all required modules and functions exist"""
    try:
        create_missing_modules()
        print("[COMPAT] Missing modules created successfully")
    except Exception as e:
        print(f"[COMPAT] Warning: Could not create missing modules: {e}")

def get_bank_soa_function():
    """Get the bank SOA function with proper parameter handling"""
    bank_fn = resolve_func(rms_bank_soa_download, 
        ["download_bank_soa", "export_bank_soa_for_salary_month", "download_salary_bank_soa"])
    
    if bank_fn and hasattr(bank_fn, '__code__'):
        # Create a wrapper that handles the parameter correctly
        def bank_soa_wrapper(driver_or_month, month_name=None, year=None):
            if month_name is None:
                # Called without driver (old style)
                return bank_fn(driver_or_month, year)
            else:
                # Called with driver (new style)
                return bank_fn(driver_or_month, month_name, year)
        return bank_soa_wrapper
    
    return bank_fn

def safe_call_with_driver(func, driver, month_name, year):
    """Safely call a function with driver, handling parameter variations"""
    if not func:
        return None
        
    try:
        # Check if function expects driver parameter
        if hasattr(func, '__code__'):
            param_count = func.__code__.co_argcount
            param_names = func.__code__.co_varnames[:param_count]
            
            if 'driver' in param_names or param_count >= 3:
                # Function expects driver parameter
                return func(driver, month_name, year)
            else:
                # Function doesn't expect driver parameter
                return func(month_name, year)
        else:
            # Fallback: try with driver first
            try:
                return func(driver, month_name, year)
            except TypeError:
                return func(month_name, year)
                
    except Exception as e:
        logging.warning(f"Error calling {func.__name__}: {e}")
        return None

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
