#!/usr/bin/env python3
"""
Verify DST correction results from the processed parquet file.

# /// script
# dependencies = ["polars"]
# ///
"""

import polars as pl

def main():
    df = pl.read_parquet('output/processed_sap_flux_demo.parquet')
    
    print('=== DST Correction Verification ===')
    print(f'Total rows: {len(df)}')
    print(f'Columns: {df.columns}')
    print()
    
    print('Sample data showing original vs corrected timestamps:')
    sample = df.select([
        'timestamp_original_local', 
        'timestamp_utc_corrected', 
        'original_timezone_offset', 
        'chunk_id'
    ]).head(10)
    print(sample)
    print()
    
    print('Timezone offset distribution:')
    print(df['original_timezone_offset'].value_counts())
    print()
    
    print('Chunk distribution:')
    print(df['chunk_id'].value_counts())

if __name__ == '__main__':
    main()