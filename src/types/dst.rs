use chrono::{DateTime, Utc, TimeZone, NaiveDateTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DstAction {
    Start,
    End,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DstTransition {
    pub action: DstAction,
    pub utc_time: DateTime<Utc>,
}

impl DstTransition {
    pub fn new(action: DstAction, utc_time: DateTime<Utc>) -> Self {
        Self { action, utc_time }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DstTransitionTable {
    transitions: Vec<DstTransition>,
}

impl DstTransitionTable {
    pub fn us_eastern_2011_2030() -> Self {
        let transitions = vec![
            // 2021
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2021, 3, 14, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2021, 11, 7, 6, 0, 0).unwrap()),
            // 2022
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2022, 3, 13, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2022, 11, 6, 6, 0, 0).unwrap()),
            // 2023
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2023, 3, 12, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2023, 11, 5, 6, 0, 0).unwrap()),
            // 2024
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2024, 3, 10, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2024, 11, 3, 6, 0, 0).unwrap()),
            // 2025
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2025, 3, 9, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2025, 11, 2, 6, 0, 0).unwrap()),
            // 2026
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2026, 3, 8, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2026, 11, 1, 6, 0, 0).unwrap()),
            // 2027
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2027, 3, 14, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2027, 11, 7, 6, 0, 0).unwrap()),
            // 2028
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2028, 3, 12, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2028, 11, 5, 6, 0, 0).unwrap()),
            // 2029
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2029, 3, 11, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2029, 11, 4, 6, 0, 0).unwrap()),
            // 2030
            DstTransition::new(DstAction::Start, Utc.with_ymd_and_hms(2030, 3, 10, 7, 0, 0).unwrap()),
            DstTransition::new(DstAction::End, Utc.with_ymd_and_hms(2030, 11, 3, 6, 0, 0).unwrap()),
        ];
        
        Self { transitions }
    }

    pub fn determine_timezone_offset(&self, naive_local_timestamp: NaiveDateTime) -> i32 {
        // CRITICAL: This function determines what timezone the logger was set to when it recorded
        // the given timestamp. The input is a NAIVE local time (no timezone info).
        //
        // The algorithm:
        // 1. Find the most recent DST transition before this naive local timestamp
        // 2. Return the timezone offset that was active at that time
        
        let mut is_dst = false;
        
        // Use the naive timestamp directly for comparison with transition times
        let naive_dt = naive_local_timestamp;
        
        for transition in &self.transitions {
            // Convert UTC transition times to local times for comparison
            let local_transition_naive = match transition.action {
                DstAction::Start => {
                    // Spring forward: happens at 2:00 AM local time (EST -> EDT)
                    // The UTC time in table is when 2:00 AM EST becomes 3:00 AM EDT
                    // For comparison with naive local time, we use 2:00 AM (which is the "spring forward" moment)
                    (transition.utc_time - chrono::Duration::hours(5)).naive_utc()
                }
                DstAction::End => {
                    // Fall back: happens at 2:00 AM local time (EDT -> EST)  
                    // The UTC time in table is when 2:00 AM EDT becomes 1:00 AM EST
                    // CRITICAL: For fall back, we need to check if we're in the FIRST 2:00 AM (EDT) or SECOND 2:00 AM (EST)
                    // Since this is ambiguous, we use a slightly offset time to ensure proper comparison
                    // We use 1:59:59 AM as the effective transition point for comparison
                    (transition.utc_time - chrono::Duration::hours(4) - chrono::Duration::seconds(1)).naive_utc()
                }
            };
            
            // Check if the naive local timestamp is STRICTLY after this transition
            // This is critical: times exactly AT the transition moment should use the OLD timezone
            if naive_dt > local_transition_naive {
                match transition.action {
                    DstAction::Start => is_dst = true,
                    DstAction::End => is_dst = false,
                }
            }
        }
        
        if is_dst { -4 } else { -5 } // EDT = UTC-4, EST = UTC-5
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dst_transitions() {
        let table = DstTransitionTable::us_eastern_2011_2030();
        
        // Test EST (winter) - naive local time
        let winter_time = NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap()
        );
        assert_eq!(table.determine_timezone_offset(winter_time), -5);
        
        // Test EDT (summer) - naive local time
        let summer_time = NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2024, 7, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap()
        );
        assert_eq!(table.determine_timezone_offset(summer_time), -4);
    }
}