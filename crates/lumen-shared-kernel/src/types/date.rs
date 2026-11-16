//! Strict canonical `YYYY-MM-DD` date check.

/// Why a string is not a canonical `YYYY-MM-DD` date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateError {
    /// Not exactly 10 ASCII bytes shaped `DDDD-DD-DD` (digits and two `-`).
    WrongShape,
    /// Month is not 01..=12, or the year is 0000.
    MonthOutOfRange,
    /// Day is 00 or past the last day of that month.
    DayOutOfRange,
}

/// Checks that `s` is a canonical `YYYY-MM-DD` date (year 0001..=9999).
pub fn parse_date(s: &str) -> Result<(), DateError> {
    let b = s.as_bytes();
    let shaped = b.len() == 10
        && b.iter().enumerate().all(|(i, &c)| match i {
            4 | 7 => c == b'-',
            _ => c.is_ascii_digit(),
        });
    if !shaped {
        return Err(DateError::WrongShape);
    }
    let digits = |from: usize, to: usize| {
        b[from..to]
            .iter()
            .fold(0u32, |acc, &c| acc * 10 + u32::from(c - b'0'))
    };
    let year = digits(0, 4);
    let month = digits(5, 7);
    let day = digits(8, 10);
    if year == 0 || !(1..=12).contains(&month) {
        return Err(DateError::MonthOutOfRange);
    }
    if day == 0 || day > days_in_month(year, month) {
        return Err(DateError::DayOutOfRange);
    }
    Ok(())
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            let leap =
                (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400);
            if leap {
                29
            } else {
                28
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_leap_day() {
        assert_eq!(parse_date("2024-02-29"), Ok(()));
    }

    #[test]
    fn accepts_plain_date() {
        assert_eq!(parse_date("2026-01-01"), Ok(()));
    }

    #[test]
    fn accepts_year_one() {
        assert_eq!(parse_date("0001-01-01"), Ok(()));
    }

    #[test]
    fn accepts_century_leap_year() {
        assert_eq!(parse_date("2000-02-29"), Ok(()));
    }

    #[test]
    fn accepts_last_day_of_31_day_month() {
        assert_eq!(parse_date("2026-12-31"), Ok(()));
    }

    #[test]
    fn refuses_feb_30() {
        assert_eq!(parse_date("2026-02-30"), Err(DateError::DayOutOfRange));
    }

    #[test]
    fn refuses_feb_29_in_common_year() {
        assert_eq!(parse_date("2023-02-29"), Err(DateError::DayOutOfRange));
    }

    #[test]
    fn refuses_feb_29_in_1900() {
        assert_eq!(parse_date("1900-02-29"), Err(DateError::DayOutOfRange));
    }

    #[test]
    fn refuses_apr_31() {
        assert_eq!(parse_date("2026-04-31"), Err(DateError::DayOutOfRange));
    }

    #[test]
    fn refuses_day_zero() {
        assert_eq!(parse_date("2026-01-00"), Err(DateError::DayOutOfRange));
    }

    #[test]
    fn refuses_month_13() {
        assert_eq!(parse_date("2026-13-01"), Err(DateError::MonthOutOfRange));
    }

    #[test]
    fn refuses_month_zero() {
        assert_eq!(parse_date("2026-00-01"), Err(DateError::MonthOutOfRange));
    }

    #[test]
    fn refuses_year_zero() {
        assert_eq!(parse_date("0000-01-01"), Err(DateError::MonthOutOfRange));
    }

    #[test]
    fn refuses_short_month_and_day() {
        assert_eq!(parse_date("2026-1-1"), Err(DateError::WrongShape));
    }

    #[test]
    fn refuses_no_separators() {
        assert_eq!(parse_date("20260101"), Err(DateError::WrongShape));
    }

    #[test]
    fn refuses_empty() {
        assert_eq!(parse_date(""), Err(DateError::WrongShape));
    }

    #[test]
    fn refuses_time_part() {
        assert_eq!(
            parse_date("2026-01-01T00:00:00Z"),
            Err(DateError::WrongShape)
        );
    }

    #[test]
    fn refuses_trailing_space() {
        assert_eq!(parse_date("2026-01-01 "), Err(DateError::WrongShape));
    }

    #[test]
    fn refuses_leading_space() {
        assert_eq!(parse_date(" 2026-01-01"), Err(DateError::WrongShape));
    }

    #[test]
    fn refuses_slash_separator() {
        assert_eq!(parse_date("2026/01/01"), Err(DateError::WrongShape));
    }

    #[test]
    fn refuses_non_ascii_digits_without_panic() {
        assert_eq!(parse_date("２０２６-01-01"), Err(DateError::WrongShape));
    }
}
