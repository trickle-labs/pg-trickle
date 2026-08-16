use crate::error::PgTrickleError;

pub(crate) fn finite_fraction(name: &str, value: f64) -> Result<f64, PgTrickleError> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(PgTrickleError::InvalidArgument(format!(
            "{name} must be finite and between 0.0 and 1.0 (got {value})"
        )));
    }
    Ok(value)
}

pub(crate) fn checked_i32(name: &str, value: i64) -> Result<i32, PgTrickleError> {
    i32::try_from(value).map_err(|_| {
        PgTrickleError::InvalidArgument(format!(
            "{name} must be representable as a 32-bit integer (got {value})"
        ))
    })
}

pub(crate) fn nonnegative_i32(name: &str, value: i64) -> Result<i32, PgTrickleError> {
    let checked = checked_i32(name, value)?;
    if checked < 0 {
        return Err(PgTrickleError::InvalidArgument(format!(
            "{name} must be non-negative (got {value})"
        )));
    }
    Ok(checked)
}

pub(crate) fn positive_i32(name: &str, value: i64) -> Result<i32, PgTrickleError> {
    let checked = checked_i32(name, value)?;
    if checked <= 0 {
        return Err(PgTrickleError::InvalidArgument(format!(
            "{name} must be positive (got {value})"
        )));
    }
    Ok(checked)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_fraction_boundaries() {
        assert_eq!(finite_fraction("fraction", 0.0).unwrap(), 0.0);
        assert_eq!(finite_fraction("fraction", 1.0).unwrap(), 1.0);
        assert!(finite_fraction("fraction", -0.1).is_err());
        assert!(finite_fraction("fraction", 1.1).is_err());
        assert!(finite_fraction("fraction", f64::NAN).is_err());
        assert!(finite_fraction("fraction", f64::INFINITY).is_err());
    }

    #[test]
    fn checks_integer_ranges() {
        assert_eq!(checked_i32("n", i64::from(i32::MAX)).unwrap(), i32::MAX);
        assert!(checked_i32("n", i64::from(i32::MAX) + 1).is_err());
        assert!(nonnegative_i32("n", -1).is_err());
        assert!(positive_i32("n", 0).is_err());
    }
}
