//! One valid Prometheus text exposition out of lines several writers append.
//!
//! The text format allows one `# HELP` and one `# TYPE` line per metric
//! family and requires a family's samples to be one contiguous group. Two
//! indexers on one host may each declare the same family (CLAMM and Arch Lend
//! both report `apa_events_unpriced` and `apa_state_point_age_seconds`), and a
//! parser rejects the second `# TYPE`. [`group_families`] regroups the scrape:
//! families in order of first appearance, each with its first `# HELP` and
//! first `# TYPE`, then every sample of it in the order written.

use std::collections::HashMap;

#[derive(Default)]
struct Family<'a> {
    help: Option<&'a str>,
    kind: Option<&'a str>,
    samples: Vec<&'a str>,
}

enum Line<'a> {
    Type(&'a str),
    Help(&'a str),
    Sample(&'a str),
}

/// Regroup `text` by metric family (module doc). Other comment lines and
/// blank lines are dropped; sample lines are kept verbatim.
pub fn group_families(text: &str) -> String {
    let mut lines: Vec<(&str, Line)> = Vec::new();
    for line in text.lines() {
        let line = line.trim_end();
        if line.trim().is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = split_first_word(rest);
            lines.push((name, Line::Type(kind)));
        } else if let Some(rest) = line.strip_prefix("# HELP ") {
            lines.push((split_first_word(rest).0, Line::Help(line)));
        } else if !line.starts_with('#') {
            lines.push((sample_name(line), Line::Sample(line)));
        }
    }
    // Every declaration is known before a sample is placed, so a histogram's
    // `_bucket`/`_sum`/`_count` join their family wherever it was declared.
    let mut kinds: HashMap<&str, &str> = HashMap::new();
    for (name, line) in &lines {
        if let Line::Type(kind) = line {
            kinds.entry(name).or_insert(kind);
        }
    }
    let mut order: Vec<&str> = Vec::new();
    let mut families: HashMap<&str, Family> = HashMap::new();
    for (name, line) in lines {
        let name = match line {
            Line::Sample(_) => owner(&kinds, name),
            _ => name,
        };
        let f = families.entry(name).or_insert_with(|| {
            order.push(name);
            Family::default()
        });
        match line {
            Line::Type(kind) => {
                f.kind.get_or_insert(kind);
            }
            Line::Help(help) => {
                f.help.get_or_insert(help);
            }
            Line::Sample(sample) => f.samples.push(sample),
        }
    }
    let mut out = String::with_capacity(text.len());
    for name in order {
        let f = &families[name];
        if let Some(help) = f.help {
            out.push_str(help);
            out.push('\n');
        }
        if let Some(kind) = f.kind {
            out.push_str(&format!("# TYPE {name} {kind}\n"));
        }
        for sample in &f.samples {
            out.push_str(sample);
            out.push('\n');
        }
    }
    out
}

fn split_first_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.split_once(char::is_whitespace) {
        Some((head, rest)) => (head, rest.trim()),
        None => (s, ""),
    }
}

/// A sample line's metric name: everything before its labels or value.
fn sample_name(line: &str) -> &str {
    let end = line
        .find(|c: char| c == '{' || c.is_whitespace())
        .unwrap_or(line.len());
    &line[..end]
}

/// The family a sample belongs to: its own name, or for a declared
/// histogram's or summary's suffixed series, the declared base.
fn owner<'a>(kinds: &HashMap<&str, &str>, name: &'a str) -> &'a str {
    if kinds.contains_key(name) {
        return name;
    }
    for suffix in ["_bucket", "_sum", "_count"] {
        if let Some(base) = name.strip_suffix(suffix) {
            if matches!(kinds.get(base), Some(&"histogram" | &"summary")) {
                return base;
            }
        }
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_family_two_indexers_declare_is_typed_once_and_grouped() {
        let clamm = "# TYPE apa_events_unpriced gauge\n\
                     apa_events_unpriced{kind=\"swap\"} 3\n\
                     # TYPE apa_state_point_age_seconds gauge\n\
                     apa_state_point_age_seconds{kind=\"pool\"} 12.5\n";
        let lend = "# TYPE apa_events_unpriced gauge\n\
                    apa_events_unpriced{kind=\"lending\"} 0\n\
                    # TYPE apa_state_point_age_seconds gauge\n\
                    apa_state_point_age_seconds{kind=\"market\"} 4\n\
                    # TYPE apa_lending_events_total counter\n\
                    apa_lending_events_total{kind=\"deposit\"} 9\n";
        assert_eq!(
            group_families(&format!("{clamm}{lend}")),
            "# TYPE apa_events_unpriced gauge\n\
             apa_events_unpriced{kind=\"swap\"} 3\n\
             apa_events_unpriced{kind=\"lending\"} 0\n\
             # TYPE apa_state_point_age_seconds gauge\n\
             apa_state_point_age_seconds{kind=\"pool\"} 12.5\n\
             apa_state_point_age_seconds{kind=\"market\"} 4\n\
             # TYPE apa_lending_events_total counter\n\
             apa_lending_events_total{kind=\"deposit\"} 9\n"
        );
    }

    #[test]
    fn help_first_then_type_and_suffixed_series_join_their_histogram() {
        let text = "# HELP lat_seconds Latency.\n\
                    # TYPE lat_seconds histogram\n\
                    lat_seconds_bucket{le=\"1\"} 2\n\
                    other_total 1\n\
                    # HELP lat_seconds A second help.\n\
                    lat_seconds_sum 1.5\n\
                    lat_seconds_count 2\n\
                    # a comment\n\
                    \n\
                    untyped_metric 7\n";
        assert_eq!(
            group_families(text),
            "# HELP lat_seconds Latency.\n\
             # TYPE lat_seconds histogram\n\
             lat_seconds_bucket{le=\"1\"} 2\n\
             lat_seconds_sum 1.5\n\
             lat_seconds_count 2\n\
             other_total 1\n\
             untyped_metric 7\n"
        );
    }

    #[test]
    fn a_type_declared_after_its_first_sample_still_heads_the_group() {
        let text = "x_total 1\n# TYPE x_total counter\nx_total{a=\"b\"} 2\n";
        assert_eq!(
            group_families(text),
            "# TYPE x_total counter\nx_total 1\nx_total{a=\"b\"} 2\n"
        );
    }

    #[test]
    fn a_counter_named_with_a_histogram_suffix_stays_its_own_family() {
        let text = "# TYPE jobs gauge\njobs 1\n# TYPE jobs_count counter\njobs_count 4\n";
        assert_eq!(group_families(text), text);
    }
}
