#!/usr/bin/env python3
"""
Comprehensive test cases for DST algorithm validation.

# /// script
# dependencies = ["polars"]
# ///
"""

import polars as pl
from datetime import datetime

def main():
    print("=== DST Algorithm Test Cases ===")
    print()
    
    # Test the DST transition table dates from the README
    dst_transitions = [
        # 2021
        ("2021-03-14", "start", "Spring forward"),
        ("2021-11-07", "end", "Fall back"),
        # 2022  
        ("2022-03-13", "start", "Spring forward"),
        ("2022-11-06", "end", "Fall back"),
        # 2023
        ("2023-03-12", "start", "Spring forward"), 
        ("2023-11-05", "end", "Fall back"),
        # 2024
        ("2024-03-10", "start", "Spring forward"),
        ("2024-11-03", "end", "Fall back"),
        # 2025
        ("2025-03-09", "start", "Spring forward"),
        ("2025-11-02", "end", "Fall back"),
    ]
    
    print("1. DST TRANSITION DATE VERIFICATION")
    print("=" * 50)
    
    # Check if our implementation correctly identifies timezone for critical dates
    test_dates = [
        # Before 2021 spring transition (should be EST)
        ("2021-03-13 01:00:00", "EST", "Day before spring 2021"),
        # After 2021 spring transition (should be EDT)
        ("2021-03-15 01:00:00", "EDT", "Day after spring 2021"),
        # Before 2021 fall transition (should be EDT)
        ("2021-11-06 01:00:00", "EDT", "Day before fall 2021"),
        # After 2021 fall transition (should be EST)
        ("2021-11-08 01:00:00", "EST", "Day after fall 2021"),
        
        # 2024 tests
        ("2024-03-09 01:00:00", "EST", "Day before spring 2024"),
        ("2024-03-11 01:00:00", "EDT", "Day after spring 2024"),
        ("2024-11-02 01:00:00", "EDT", "Day before fall 2024"),
        ("2024-11-04 01:00:00", "EST", "Day after fall 2024"),
    ]
    
    print("🔍 Testing timezone determination logic...")
    print("   (This simulates what our algorithm should determine for these timestamps)")
    
    for timestamp_str, expected_tz, description in test_dates:
        # Parse the timestamp
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        
        # Simulate our algorithm's logic
        is_dst = False
        for transition_date, action, _ in dst_transitions:
            transition_dt = datetime.strptime(transition_date, "%Y-%m-%d")
            if dt.date() >= transition_dt.date():
                if action == "start":
                    is_dst = True
                elif action == "end":
                    is_dst = False
        
        actual_tz = "EDT" if is_dst else "EST"
        status = "✅" if actual_tz == expected_tz else "❌"
        
        print(f"   {status} {timestamp_str}: Expected {expected_tz}, algorithm gives {actual_tz}")
        print(f"      {description}")
        
        if actual_tz != expected_tz:
            print(f"      ⚠️  ALGORITHM ERROR DETECTED!")
    
    print("\n2. EDGE CASE TESTING")
    print("=" * 50)
    
    # Test edge cases around DST transitions
    edge_cases = [
        # Spring transition edge cases (2024-03-10 at 2:00 AM becomes 3:00 AM)
        ("2024-03-10 01:59:00", "EST", "Just before spring forward"),
        ("2024-03-10 03:01:00", "EDT", "Just after spring forward"),
        
        # Fall transition edge cases (2024-11-03 at 2:00 AM becomes 1:00 AM)
        ("2024-11-03 01:59:00", "EDT", "Before fall back"),
        ("2024-11-03 02:01:00", "EST", "After fall back"),
    ]
    
    print("🔍 Testing edge cases around DST transitions...")
    
    for timestamp_str, expected_tz, description in edge_cases:
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        
        # Our algorithm logic (simplified)
        is_dst = False
        for transition_date, action, _ in dst_transitions:
            transition_dt = datetime.strptime(transition_date, "%Y-%m-%d")
            if dt.date() >= transition_dt.date():
                if action == "start":
                    is_dst = True
                elif action == "end":
                    is_dst = False
        
        actual_tz = "EDT" if is_dst else "EST"
        status = "✅" if actual_tz == expected_tz else "❌"
        
        print(f"   {status} {timestamp_str}: Expected {expected_tz}, got {actual_tz}")
        print(f"      {description}")
    
    print("\n3. CHUNK BOUNDARY ANALYSIS")
    print("=" * 50)
    
    # Load actual data to check for problematic chunk boundaries
    try:
        df = pl.read_parquet('output/processed_sap_flux_demo.parquet')
        
        chunk_boundaries = df.select(['chunk_id', 'timestamp_original_local', 'timestamp_utc_corrected']).group_by('chunk_id').agg([
            pl.col('timestamp_original_local').min().alias('start_local'),
            pl.col('timestamp_original_local').max().alias('end_local'),
            pl.col('timestamp_utc_corrected').min().alias('start_utc'),
            pl.col('timestamp_utc_corrected').max().alias('end_utc')
        ]).sort('start_local')
        
        print("🔍 Analyzing chunk boundaries for DST transition issues...")
        
        for row in chunk_boundaries.iter_rows(named=True):
            chunk_id = row['chunk_id']
            start_date = row['start_local'].strftime('%Y-%m-%d')
            end_date = row['end_local'].strftime('%Y-%m-%d')
            
            print(f"\n   Chunk: {chunk_id}")
            print(f"   Local time range: {row['start_local']} to {row['end_local']}")
            print(f"   UTC time range: {row['start_utc']} to {row['end_utc']}")
            
            # Check if chunk spans any DST transitions
            spans_transition = False
            for transition_date, action, desc in dst_transitions:
                if start_date <= transition_date <= end_date:
                    print(f"   ⚠️  SPANS DST TRANSITION: {transition_date} ({desc})")
                    spans_transition = True
            
            if not spans_transition:
                print(f"   ✅ No DST transitions spanned")
                
    except FileNotFoundError:
        print("   ⚠️  Output file not found - run main pipeline first")
    
    print("\n4. ALGORITHM CORRECTNESS ISSUES IDENTIFIED")
    print("=" * 50)
    
    print("❌ CRITICAL ISSUE: Timezone determination logic is incorrect!")
    print()
    print("   Current algorithm uses first timestamp of chunk to determine timezone,")
    print("   but our analysis shows this doesn't correctly handle DST transitions.")
    print()
    print("   Example problems:")
    print("   - Algorithm treats dates >= transition date as being in new timezone")
    print("   - But DST transitions happen at specific TIMES, not just dates")
    print("   - Spring forward: 2:00 AM becomes 3:00 AM")
    print("   - Fall back: 2:00 AM becomes 1:00 AM")
    print()
    print("   The algorithm needs to:")
    print("   1. Use DATETIME comparison, not just date comparison")
    print("   2. Account for the specific transition times (2:00 AM)")
    print("   3. Handle the 'missing hour' and 'repeated hour' properly")
    
    print("\n5. RECOMMENDED FIXES")
    print("=" * 50)
    
    print("1. ✅ Fix timezone determination to use proper datetime comparison")
    print("2. ✅ Account for 2:00 AM transition times")
    print("3. ✅ Add validation for missing/duplicate hours")
    print("4. ✅ Implement proper chunk boundary validation")
    print("5. ✅ Add comprehensive test cases")

if __name__ == '__main__':
    main()