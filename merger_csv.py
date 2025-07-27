#!/usr/bin/env python3
import os
import pandas as pd

def merge_and_clean_csvs(input_dir, output_file,
                         dedupe_subset=None,
                         dropna_thresh=None,
                         filters=None):
    """
    Merge multiple CSVs, clean data, and write to output.
    - input_dir: folder containing .csv files
    - output_file: path for cleaned CSV
    - dedupe_subset: list of columns to consider for duplicates (None = all columns)
    - dropna_thresh: int or dict for dropna
    - filters: list of lambdas to filter rows, e.g. [lambda df: df['age'] > 18]
    """
    # 1. Load all CSVs
    csv_files = [f for f in os.listdir(input_dir)
                 if f.lower().endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {input_dir}")

    dfs = []
    for fname in csv_files:
        path = os.path.join(input_dir, fname)
        print(f"Loading {path} …")
        dfs.append(pd.read_csv(path))

    # 2. Merge
    df = pd.concat(dfs, ignore_index=True)
    print(f"Combined shape: {df.shape}")

    # 3. Drop duplicates
    df = df.drop_duplicates(subset=dedupe_subset)
    print(f"After dedupe: {df.shape}")

    # 4. Drop NULL / NaN rows
    if dropna_thresh is not None:
        df = df.dropna(thresh=dropna_thresh)
        print(f"After dropna: {df.shape}")

    # 5. Custom filters
    if filters:
        for flt in filters:
            before = df.shape[0]
            df = df[flt(df)]
            after = df.shape[0]
            print(f"Applied filter: kept {after}/{before}")

    # 6. Save result
    df.to_csv(output_file, index=False)
    print(f"Saved cleaned CSV to {output_file}, rows: {df.shape[0]}")

if __name__ == "__main__":
    INPUT_DIR = "."
    OUTPUT_FILE = "merged_all.csv"

    # Example: drop duplicates by all columns, drop rows with >50% nulls,
    # then filter for col 'age'>18 and 'salary'>1000
    my_filters = [
        # lambda df: df['age'] > 18,
        # lambda df: df['salary'] >= 1000
    ]

    merge_and_clean_csvs(
        input_dir=INPUT_DIR,
        output_file=OUTPUT_FILE,
        dedupe_subset=None,
        dropna_thresh=int(0.5 * len(pd.read_csv(os.path.join(INPUT_DIR, os.listdir(INPUT_DIR)[0])))),
        filters=my_filters
    )
