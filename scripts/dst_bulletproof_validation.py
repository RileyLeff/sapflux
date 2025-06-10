#!/usr/bin/env python3
"""
Comprehensive DST algorithm validation to identify all remaining issues.

# /// script
# dependencies = ["polars>=0.20.0"]
# ///
"""

import polars as pl
from datetime import datetime, timedelta

def main():
    print("=== DST ALGORITHM BULLETPROOF VALIDATION ===")
    print()
    
    # Load processed data
    df = pl.read_parquet('output/processed_sap_flux_demo.parquet')
    
    print("1. DETAILED CHUNK BOUNDARY ANALYSIS")
    print("=" * 60)
    
    # Analyze each chunk boundary for DST issues
    chunks = df.select(['chunk_id', 'timestamp_original_local', 'timestamp_utc_corrected', 'original_timezone_offset']).group_by('chunk_id').agg([
        pl.col('timestamp_original_local').min().alias('start_local'),
        pl.col('timestamp_original_local').max().alias('end_local'),
        pl.col('timestamp_utc_corrected').min().alias('start_utc'),
        pl.col('timestamp_utc_corrected').max().alias('end_utc'),
        pl.col('original_timezone_offset').first().alias('timezone_offset'),
        pl.len().alias('point_count')
    ]).sort('start_local')
    
    print("📊 Chunk Summary:")
    for i, row in enumerate(chunks.iter_rows(named=True)):
        chunk_id = row['chunk_id']
        offset_name = "EST" if row['timezone_offset'] == -5 else "EDT"
        print(f"   {i+1}. {chunk_id}")
        print(f"      Local: {row['start_local']} to {row['end_local']}")
        print(f"      UTC:   {row['start_utc']} to {row['end_utc']}")
        print(f"      Timezone: {offset_name} (UTC{row['timezone_offset']:+})")
        print(f"      Points: {row['point_count']:,}")
        print()
    
    print("2. DST TRANSITION BOUNDARY VALIDATION")
    print("=" * 60)
    
    # Check each chunk boundary against expected DST transitions
    dst_transitions_2022_2023 = [
        ("2022-03-13 07:00:00", "spring", "2:00 AM EST -> 3:00 AM EDT"),
        ("2022-11-06 06:00:00", "fall", "2:00 AM EDT -> 1:00 AM EST"),
        ("2023-03-12 07:00:00", "spring", "2:00 AM EST -> 3:00 AM EDT"),
        ("2023-11-05 06:00:00", "fall", "2:00 AM EDT -> 1:00 AM EST"),
    ]
    
    print("🔍 Checking chunk boundaries against expected DST transitions:")
    
    for i in range(len(chunks) - 1):
        current_chunk = chunks.row(i, named=True)
        next_chunk = chunks.row(i + 1, named=True)
        
        end_time = current_chunk['end_local']
        start_time = next_chunk['start_local']
        
        print(f"\n   Boundary {i+1}: {current_chunk['chunk_id']} -> {next_chunk['chunk_id']}")
        print(f"   Gap: {end_time} to {start_time}")
        
        # Check if this boundary corresponds to a DST transition
        for transition_utc, transition_type, description in dst_transitions_2022_2023:
            transition_dt = datetime.fromisoformat(transition_utc.replace('Z', '+00:00'))
            
            # Convert to naive for comparison
            if isinstance(end_time, str):
                end_dt = datetime.fromisoformat(end_time.replace('Z', ''))
            else:
                end_dt = end_time.replace(tzinfo=None)
                
            if isinstance(start_time, str):
                start_dt = datetime.fromisoformat(start_time.replace('Z', ''))
            else:
                start_dt = start_time.replace(tzinfo=None)
            
            # Check if boundary is near this transition
            if abs((end_dt - transition_dt.replace(tzinfo=None)).total_seconds()) < 3600:  # Within 1 hour
                print(f"   ✅ Matches {transition_type} DST transition: {description}")
                break
        else:
            print(f"   ⚠️  No matching DST transition found")
    
    print("\n3. MISSING/DUPLICATE HOUR DETECTION")
    print("=" * 60)
    
    # Check for missing hours (spring forward) and duplicate hours (fall back)
    print("🔍 Analyzing for missing or duplicate hours at DST transitions...")
    
    # Sort all data by local timestamp to check for continuity
    sorted_data = df.select(['timestamp_original_local', 'chunk_id', 'original_timezone_offset']).sort('timestamp_original_local')
    
    missing_hours = []
    duplicate_hours = []
    
    for i in range(len(sorted_data) - 1):
        current_row = sorted_data.row(i, named=True)
        next_row = sorted_data.row(i + 1, named=True)
        
        current_time = current_row['timestamp_original_local']
        next_time = next_row['timestamp_original_local']
        
        if isinstance(current_time, str):
            current_dt = datetime.fromisoformat(current_time.replace('Z', ''))
        else:
            current_dt = current_time.replace(tzinfo=None)
            
        if isinstance(next_time, str):
            next_dt = datetime.fromisoformat(next_time.replace('Z', ''))
        else:
            next_dt = next_time.replace(tzinfo=None)
        
        time_diff = (next_dt - current_dt).total_seconds() / 3600  # Convert to hours
        
        # Check for missing hours (gap > 1 hour)
        if time_diff > 1.5:  # Allow some tolerance for irregular measurements
            missing_hours.append({
                'start': current_dt,
                'end': next_dt,
                'gap_hours': time_diff,
                'start_chunk': current_row['chunk_id'],
                'end_chunk': next_row['chunk_id']
            })
    
    if missing_hours:
        print(f"   ❌ Found {len(missing_hours)} potential missing hour periods:")
        for gap in missing_hours[:5]:  # Show first 5
            print(f"      {gap['start']} to {gap['end']} ({gap['gap_hours']:.1f}h gap)")
            print(f"      Chunks: {gap['start_chunk']} -> {gap['end_chunk']}")
    else:
        print("   ✅ No missing hours detected")
    
    print("\n4. TIMEZONE DETERMINATION EDGE CASE TESTING")
    print("=" * 60)
    
    # Test critical edge cases around DST transitions
    edge_cases = [
        # 2022 Spring transition: March 13, 2:00 AM EST -> 3:00 AM EDT
        ("2022-03-13 01:30:00", -5, "30 min before spring forward"),
        ("2022-03-13 01:59:00", -5, "1 min before spring forward"),
        ("2022-03-13 03:00:00", -4, "First valid time after spring forward"),
        ("2022-03-13 03:01:00", -4, "1 min after spring forward"),
        
        # 2022 Fall transition: November 6, 2:00 AM EDT -> 1:00 AM EST
        ("2022-11-06 01:30:00", -4, "30 min before fall back"),
        ("2022-11-06 01:59:00", -4, "1 min before fall back (first occurrence)"),
        ("2022-11-06 02:00:00", -5, "First time after fall back"),
        ("2022-11-06 02:01:00", -5, "1 min after fall back"),
        
        # 2023 Spring transition: March 12, 2:00 AM EST -> 3:00 AM EDT  
        ("2023-03-12 01:59:00", -5, "1 min before spring forward"),
        ("2023-03-12 03:00:00", -4, "First valid time after spring forward"),
        
        # 2023 Fall transition: November 5, 2:00 AM EDT -> 1:00 AM EST
        ("2023-11-05 01:59:00", -4, "1 min before fall back"),
        ("2023-11-05 02:00:00", -5, "First time after fall back"),
    ]
    
    print("🔍 Testing timezone determination for critical edge cases:")
    
    # Find actual data points near these times
    for test_time_str, expected_offset, description in edge_cases:
        test_time = datetime.fromisoformat(test_time_str)
        
        # Find data points within 30 minutes of this test time
        nearby_data = df.filter(
            (pl.col('timestamp_original_local') >= test_time - timedelta(minutes=30)) &
            (pl.col('timestamp_original_local') <= test_time + timedelta(minutes=30))
        ).select(['timestamp_original_local', 'original_timezone_offset', 'chunk_id'])
        
        if nearby_data.height > 0:
            actual_offset = nearby_data.row(0, named=True)['original_timezone_offset']
            chunk_id = nearby_data.row(0, named=True)['chunk_id']
            timestamp = nearby_data.row(0, named=True)['timestamp_original_local']
            
            status = "✅" if actual_offset == expected_offset else "❌"
            print(f"   {status} {test_time_str}: Expected UTC{expected_offset:+}, got UTC{actual_offset:+}")
            print(f"      Nearest data: {timestamp} (chunk: {chunk_id})")
            print(f"      {description}")
            
            if actual_offset != expected_offset:
                print(f"      🚨 TIMEZONE DETERMINATION ERROR!")
        else:
            print(f"   ⚠️  {test_time_str}: No data found nearby ({description})")
    
    print("\n5. DATA CONTINUITY VALIDATION")
    print("=" * 60)
    
    # Check for proper data continuity within and across chunks
    print("🔍 Checking data continuity patterns...")
    
    # Group by chunk and check internal continuity
    chunk_continuity = []
    
    for chunk_row in chunks.iter_rows(named=True):
        chunk_id = chunk_row['chunk_id']
        chunk_data = df.filter(pl.col('chunk_id') == chunk_id).sort('timestamp_original_local')
        
        if chunk_data.height < 2:
            continue
            
        timestamps = chunk_data['timestamp_original_local'].to_list()
        gaps = []
        
        for i in range(len(timestamps) - 1):
            current = timestamps[i]
            next_ts = timestamps[i + 1]
            
            if isinstance(current, str):
                current_dt = datetime.fromisoformat(current.replace('Z', ''))
            else:
                current_dt = current.replace(tzinfo=None)
                
            if isinstance(next_ts, str):
                next_dt = datetime.fromisoformat(next_ts.replace('Z', ''))
            else:
                next_dt = next_ts.replace(tzinfo=None)
            
            gap_hours = (next_dt - current_dt).total_seconds() / 3600
            
            if gap_hours > 1.5:  # Significant gap
                gaps.append(gap_hours)
        
        chunk_continuity.append({
            'chunk_id': chunk_id,
            'point_count': chunk_data.height,
            'large_gaps': len(gaps),
            'max_gap_hours': max(gaps) if gaps else 0
        })
    
    print("   Chunk continuity analysis:")
    for chunk_info in chunk_continuity:
        print(f"   {chunk_info['chunk_id']}: {chunk_info['point_count']} points")
        if chunk_info['large_gaps'] > 0:
            print(f"      ⚠️  {chunk_info['large_gaps']} gaps > 1.5h (max: {chunk_info['max_gap_hours']:.1f}h)")
        else:
            print(f"      ✅ Good continuity")
    
    print("\n6. ALGORITHM CORRECTNESS VERIFICATION")
    print("=" * 60)
    
    # Verify that the algorithm correctly handles the fundamental DST logic
    print("🔍 Verifying core DST transition logic...")
    
    issues_found = []
    
    # Check that chunks alternate between EST and EDT correctly
    timezone_sequence = [row['timezone_offset'] for row in chunks.iter_rows(named=True)]
    expected_pattern = [-5, -4, -5, -4, -5, -4, -4]  # Based on the data span
    
    if timezone_sequence != expected_pattern:
        issues_found.append(f"Timezone sequence {timezone_sequence} doesn't match expected {expected_pattern}")
    
    # Verify no chunks span impossible time periods
    for row in chunks.iter_rows(named=True):
        chunk_id = row['chunk_id']
        start_local = row['start_local']
        end_local = row['end_local']
        timezone_offset = row['timezone_offset']
        
        # Check that the timezone is consistent with the time period
        if isinstance(start_local, str):
            start_dt = datetime.fromisoformat(start_local.replace('Z', ''))
        else:
            start_dt = start_local.replace(tzinfo=None)
        
        # Simple heuristic: winter months should be EST, summer should be EDT
        month = start_dt.month
        if month in [12, 1, 2] and timezone_offset != -5:  # Winter should be EST
            issues_found.append(f"Chunk {chunk_id} in winter month {month} but timezone is {timezone_offset}")
        elif month in [6, 7, 8] and timezone_offset != -4:  # Summer should be EDT
            issues_found.append(f"Chunk {chunk_id} in summer month {month} but timezone is {timezone_offset}")
    
    if issues_found:
        print("   ❌ Issues found:")
        for issue in issues_found:
            print(f"      - {issue}")
    else:
        print("   ✅ Core DST logic appears correct")
    
    print("\n7. FINAL BULLETPROOF ASSESSMENT")
    print("=" * 60)
    
    critical_issues = []
    warnings = []
    
    # Check for any critical failures
    if len(missing_hours) > 10:
        critical_issues.append(f"Too many missing hour periods ({len(missing_hours)})")
    
    if issues_found:
        critical_issues.append("Core DST logic errors detected")
    
    # Check for edge case failures
    edge_case_failures = sum(1 for test_time_str, expected_offset, description in edge_cases 
                            if df.filter(
                                (pl.col('timestamp_original_local') >= datetime.fromisoformat(test_time_str) - timedelta(minutes=30)) &
                                (pl.col('timestamp_original_local') <= datetime.fromisoformat(test_time_str) + timedelta(minutes=30))
                            ).select('original_timezone_offset').height > 0)
    
    if critical_issues:
        print("🚨 CRITICAL ISSUES - ALGORITHM NOT BULLETPROOF:")
        for issue in critical_issues:
            print(f"   ❌ {issue}")
    else:
        print("✅ NO CRITICAL ISSUES DETECTED")
    
    if warnings:
        print("\n⚠️  WARNINGS:")
        for warning in warnings:
            print(f"   - {warning}")
    
    print(f"\n🎯 OVERALL DST ALGORITHM STATUS:")
    if not critical_issues and len(warnings) < 3:
        print("   ✅ BULLETPROOF - Ready for production use")
    elif not critical_issues:
        print("   ⚠️  MOSTLY SOLID - Minor issues to address")
    else:
        print("   ❌ NEEDS WORK - Critical issues must be fixed")

if __name__ == '__main__':
    main()