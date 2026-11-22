use crate::model::Totals;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct AuditOptions {
    pub sample: usize,
    pub priority: usize,
    pub confidence: f64,
    pub max_delta_bytes: Option<u64>,
    pub error_bytes: Option<u64>,
}

impl Default for AuditOptions {
    fn default() -> Self {
        Self {
            sample: 512,
            priority: 32,
            confidence: 0.95,
            max_delta_bytes: None,
            error_bytes: None,
        }
    }
}

impl AuditOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.confidence.is_finite() && self.confidence > 0.0 && self.confidence < 1.0,
            "confidence must be strictly between 0 and 1"
        );
        ensure!(
            self.error_bytes.is_none() || self.max_delta_bytes.is_some(),
            "--error-bytes requires an explicit --max-delta-bytes assumption"
        );
        ensure!(
            i64::try_from(self.priority).is_ok(),
            "priority count exceeds the supported range"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Interval {
    pub lower_bytes: f64,
    pub upper_bytes: f64,
    pub confidence: f64,
    pub conditional: bool,
    pub target_met: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Estimate {
    pub bytes: f64,
    pub interval: Option<Interval>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Audit {
    pub population_files: usize,
    pub random_samples: usize,
    pub verified_files: usize,
    pub changed_samples: usize,
    pub size_change_fraction_upper: Option<f64>,
    pub confidence: f64,
    pub assumed_max_delta_bytes: Option<u64>,
    pub status: String,
    pub disk: Estimate,
    pub apparent: Estimate,
}

#[derive(Default)]
pub(crate) struct Samples {
    pub count: usize,
    pub changed: usize,
    pub disk_delta: i128,
    pub apparent_delta: i128,
    pub largest_delta: u128,
}

impl Samples {
    pub fn observe(&mut self, disk: i128, apparent: i128) {
        self.count += 1;
        self.changed += usize::from(disk != 0 || apparent != 0);
        self.disk_delta += disk;
        self.apparent_delta += apparent;
        self.largest_delta = self
            .largest_delta
            .max(disk.unsigned_abs())
            .max(apparent.unsigned_abs());
    }
}

pub(crate) fn calculate(
    baseline: Totals,
    recorded: Totals,
    population: usize,
    verified: usize,
    samples: &Samples,
    options: &AuditOptions,
    valid: bool,
) -> Audit {
    let all_verified = verified >= population;
    let violated = options
        .max_delta_bytes
        .is_some_and(|bound| samples.largest_delta > bound as u128);
    let status = if !valid {
        "incomplete"
    } else if all_verified {
        "file_census"
    } else if violated {
        "bound_violated"
    } else if options.max_delta_bytes.is_some() && samples.count > 0 {
        "conditional_bound"
    } else {
        "unbounded"
    };
    let estimate = |baseline: u64, recorded: u64, delta: i128| {
        let raw = if samples.count == 0 {
            baseline as f64
        } else {
            baseline as f64 + population as f64 * delta as f64 / samples.count as f64
        };
        let mut bytes = if all_verified {
            recorded as f64
        } else {
            raw.max(0.0)
        };
        let interval = if valid && all_verified {
            Some(Interval {
                lower_bytes: bytes,
                upper_bytes: bytes,
                confidence: 1.0,
                conditional: true,
                target_met: options.error_bytes.map(|_| true),
            })
        } else if valid && !violated && samples.count > 0 {
            options.max_delta_bytes.map(|bound| {
                // Fixed-size sampling without replacement: Hoeffding is conservative.
                let radius = population as f64
                    * bound as f64
                    * (2.0 * (2.0 / (1.0 - options.confidence)).ln() / samples.count as f64).sqrt();
                let residual = population.saturating_sub(verified) as f64 * bound as f64;
                let worst_lower = (recorded as f64 - residual).max(0.0);
                let worst_upper = recorded as f64 + residual;
                let mut lower = (raw - radius).max(worst_lower);
                let mut upper = (raw + radius).min(worst_upper).max(0.0);
                if lower > upper {
                    lower = worst_lower;
                    upper = worst_upper;
                }
                bytes = bytes.clamp(lower, upper);
                Interval {
                    lower_bytes: lower,
                    upper_bytes: upper,
                    confidence: options.confidence,
                    conditional: true,
                    target_met: options
                        .error_bytes
                        .map(|target| (bytes - lower).max(upper - bytes) <= target as f64),
                }
            })
        } else {
            None
        };
        Estimate { bytes, interval }
    };
    let fraction = if !valid || samples.count == 0 {
        None
    } else if samples.count == population {
        Some(samples.changed as f64 / population as f64)
    } else if samples.changed == 0 {
        Some(-((1.0 - options.confidence).ln() / samples.count as f64).exp_m1())
    } else {
        Some(
            (samples.changed as f64 / samples.count as f64
                + ((1.0 / (1.0 - options.confidence)).ln() / (2.0 * samples.count as f64)).sqrt())
            .min(1.0),
        )
    };
    Audit {
        population_files: population,
        random_samples: samples.count,
        verified_files: verified.min(population),
        changed_samples: samples.changed,
        size_change_fraction_upper: fraction,
        confidence: options.confidence,
        assumed_max_delta_bytes: options.max_delta_bytes,
        status: status.to_owned(),
        disk: estimate(baseline.disk_bytes, recorded.disk_bytes, samples.disk_delta),
        apparent: estimate(
            baseline.apparent_bytes,
            recorded.apparent_bytes,
            samples.apparent_delta,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_observed_changes_do_not_imply_zero_byte_uncertainty() {
        let samples = Samples {
            count: 1000,
            ..Samples::default()
        };
        let audit = calculate(
            Totals::default(),
            Totals::default(),
            1_000_000,
            1000,
            &samples,
            &AuditOptions::default(),
            true,
        );
        assert!(audit.disk.interval.is_none());
        let upper = audit.size_change_fraction_upper.unwrap();
        assert!(upper > 0.0029 && upper < 0.0031);
    }

    #[test]
    fn fixed_size_bounds_cover_a_bounded_finite_population() {
        let population = [-10, -4, 0, 2, 8, 10];
        let baseline = Totals {
            disk_bytes: 1000,
            apparent_bytes: 1000,
            ..Totals::default()
        };
        let truth = 1000.0 + population.iter().sum::<i128>() as f64;
        let options = AuditOptions {
            max_delta_bytes: Some(10),
            ..AuditOptions::default()
        };
        let mut covered = 0;
        let mut total = 0;
        for a in 0..6 {
            for b in a + 1..6 {
                for c in b + 1..6 {
                    let mut samples = Samples::default();
                    for i in [a, b, c] {
                        samples.observe(population[i], population[i]);
                    }
                    let recorded = Totals {
                        disk_bytes: (1000 + samples.disk_delta) as u64,
                        apparent_bytes: (1000 + samples.apparent_delta) as u64,
                        ..Totals::default()
                    };
                    let audit = calculate(baseline, recorded, 6, 3, &samples, &options, true);
                    let interval = audit.disk.interval.unwrap();
                    covered +=
                        usize::from(interval.lower_bytes <= truth && truth <= interval.upper_bytes);
                    total += 1;
                }
            }
        }
        assert!(covered as f64 / total as f64 >= options.confidence);
    }

    #[test]
    fn incomplete_metadata_invalidates_even_a_complete_file_sample() {
        let audit = calculate(
            Totals::default(),
            Totals::default(),
            1,
            1,
            &Samples {
                count: 1,
                ..Samples::default()
            },
            &AuditOptions::default(),
            false,
        );
        assert_eq!(audit.status, "incomplete");
        assert!(audit.disk.interval.is_none());
        assert!(audit.size_change_fraction_upper.is_none());
    }
}
