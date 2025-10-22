import os
import pandas as pd
import chardet

def read_file(filepath: str):
    """
    Smart file reader that handles XLS, XLSX, CSV, TSV, and TXT automatically.
    If an unsupported format is provided, returns an empty DataFrame and logs a warning.
    """
    ext = os.path.splitext(filepath)[1].lower()

    try:
        if ext in [".xls", ".xlsx"]:
            return pd.read_excel(filepath)
        elif ext in [".csv"]:
            # Auto detect encoding for CSV
            with open(filepath, "rb") as f:
                encoding = chardet.detect(f.read())["encoding"]
            return pd.read_csv(filepath, encoding=encoding)
        elif ext in [".tsv", ".txt"]:
            return pd.read_csv(filepath, sep="\t")
        else:
            print(f"[WARN] Unsupported file format: {ext}. Skipping {filepath}.")
            return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] Could not read file {filepath}: {e}")
        return pd.DataFrame()


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize dataframe columns (strip spaces, unify case, remove NaNs).
    """
    if df.empty:
        return df

    df.columns = [str(col).strip().lower() for col in df.columns]
    df = df.dropna(how="all")
    df = df.fillna("")
    return df
