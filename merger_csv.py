#!/usr/bin/env python3
import os
import pandas as pd
import argparse
from fnmatch import fnmatch

def merge_and_clean_files(input_dir, output_file, file_pattern=None, file_type='excel', custom_transforms=None, skip_cleaning=False):
    """
    Dynamic file merger with user-defined parameters.
    - input_dir: folder containing files
    - output_file: path for output
    - file_pattern: pattern to match files (e.g., 'sales_*.xlsx', 'report_2024*')
    - file_type: 'csv', 'excel', or 'both'
    - custom_transforms: list of functions to apply
    - skip_cleaning: skip default cleaning if True
    """
    dfs = []
    
    # Get all files in directory
    all_files = os.listdir(input_dir)
    
    # Filter files by pattern if specified
    if file_pattern:
        all_files = [f for f in all_files if fnmatch(f.lower(), file_pattern.lower())]
        print(f"Files matching pattern '{file_pattern}': {len(all_files)}")
    
    # Load CSV files
    if file_type in ['csv', 'both']:
        csv_files = [f for f in all_files if f.lower().endswith('.csv')]
        for fname in csv_files:
            path = os.path.join(input_dir, fname)
            print(f"Loading CSV: {fname}")
            dfs.append(pd.read_csv(path))
    
    # Load Excel files
    if file_type in ['excel', 'both']:
        excel_files = [f for f in all_files if f.lower().endswith(('.xlsx', '.xls'))]
        for fname in excel_files:
            path = os.path.join(input_dir, fname)
            print(f"Loading Excel: {fname}")
            excel_data = pd.read_excel(path, sheet_name=None)
            for sheet_name, sheet_df in excel_data.items():
                if not sheet_df.empty:
                    print(f"  Sheet: {sheet_name} ({sheet_df.shape[0]} rows)")
                    dfs.append(sheet_df)
    
    if not dfs:
        raise FileNotFoundError(f"No matching files found in {input_dir}")
    
    # Merge dataframes
    df = pd.concat(dfs, ignore_index=True)
    print(f"Combined shape: {df.shape}")
    
    # Apply cleaning unless skipped
    if not skip_cleaning:
        df = clean_data(df)
    
    # Apply custom transformations
    if custom_transforms:
        for transform in custom_transforms:
            print(f"Applying custom transform: {transform.__name__}")
            df = transform(df)
            print(f"After {transform.__name__}: {df.shape}")
    
    # Save result
    if output_file.lower().endswith('.xlsx'):
        df.to_excel(output_file, index=False)
    else:
        df.to_csv(output_file, index=False)
    print(f"Saved to {output_file}, final rows: {df.shape[0]}")
    return df

def clean_data(df):
    """Comprehensive data cleaning"""
    print("Starting data cleaning...")
    
    # Remove completely empty rows/columns
    df = df.dropna(how='all').dropna(axis=1, how='all')
    print(f"After removing empty rows/cols: {df.shape}")
    
    # Remove duplicates
    df = df.drop_duplicates()
    print(f"After removing duplicates: {df.shape}")
    
    # Remove rows with >70% missing data
    thresh = int(0.3 * len(df.columns))
    df = df.dropna(thresh=thresh)
    print(f"After removing incomplete rows: {df.shape}")
    
    # Clean text columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(['', 'nan', 'None', 'null'], pd.NA)
    
    return df

def interactive_merge():
    """Interactive mode for user input"""
    print("=== Dynamic File Merger ===")
    
    input_dir = input("Enter input directory (default: current): ").strip() or "."
    
    # Show available files
    files = [f for f in os.listdir(input_dir) if f.lower().endswith(('.xlsx', '.xls', '.csv'))]
    print(f"\nFound {len(files)} files:")
    for i, f in enumerate(files[:10], 1):
        print(f"  {i}. {f}")
    if len(files) > 10:
        print(f"  ... and {len(files)-10} more")
    
    # Get file pattern
    pattern = input("\nFile pattern (e.g., 'sales_*.xlsx' or press Enter for all): ").strip()
    
    # Get output file
    output = input("Output file (default: merged_output.xlsx): ").strip() or "merged_output.xlsx"
    
    # Get file type
    file_type = input("File type [excel/csv/both] (default: excel): ").strip() or "excel"
    
    return input_dir, output, pattern or None, file_type

# Example custom transforms
def filter_recent_data(df):
    """Example: Keep only recent data if date column exists"""
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    if date_cols:
        df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors='coerce')
        df = df.dropna(subset=[date_cols[0]])
        cutoff = pd.Timestamp.now() - pd.DateOffset(years=1)
        df = df[df[date_cols[0]] >= cutoff]
    return df

def remove_test_data(df):
    """Example: Remove rows containing 'test' in any column"""
    mask = df.astype(str).apply(lambda x: x.str.contains('test', case=False, na=False)).any(axis=1)
    return df[~mask]

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        # Interactive mode
        input_dir, output_file, pattern, file_type = interactive_merge()
        merge_and_clean_files(input_dir, output_file, pattern, file_type)
    else:
        # Direct usage examples
        INPUT_DIR = "."
        
        # Example 1: Merge all Excel files
        # merge_and_clean_files(INPUT_DIR, "all_merged.xlsx")
        
        # Example 2: Merge specific pattern
        # merge_and_clean_files(INPUT_DIR, "sales_merged.xlsx", "sales_*.xlsx")
        
        # Basic merge with cleaning only
        merge_and_clean_files(
            input_dir=".",
            output_file="merged_cleaned.xlsx",
            file_pattern=None,  # All files
            custom_transforms=None  # No custom transforms
        )
