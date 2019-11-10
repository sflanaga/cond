use std::time::Duration;
use anyhow::{Context, Result};

pub fn dur_from_str(s: &str) -> Result<Duration> {
    let mut _tmp = String::new();
    let mut tot_secs = 0u64;
    for c in s.chars() {
        if c >= '0' && c <= '9' { _tmp.push(c); }
        else {
            tot_secs += match c {
                's' => _tmp.parse::<u64>()?,
                'm' => _tmp.parse::<u64>()? * 60,
                'h' => _tmp.parse::<u64>()? * 3600,
                'd' => _tmp.parse::<u64>()? * 24 * 3600,
                'w' => _tmp.parse::<u64>()? * 24 * 3600 * 7,
                'y' => _tmp.parse::<u64>()? * 24 * 3600 * 365,
                _ => panic!("char {} not understood", c),
            };
            _tmp.clear();
        }
    }
    Ok(Duration::from_secs(tot_secs))
}


fn mem_metric<'a>(v: usize) -> (f64, &'a str) {
    const METRIC: [&str; 8] = ["B ", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"];

    let mut size = 1usize << 10;
    for e in &METRIC {
        if v < size {
            return ((v as f64 / (size >> 10) as f64) as f64, e);
        }
        size <<= 10;
    }
    (v as f64, "")
}
/// keep only a few significant digits of a simple float value
fn sig_dig(v: f64, digits: usize) -> String {
    let x = format!("{}", v);
    let mut d = String::new();
    let mut count = 0;
    let mut found_pt = false;
    for c in x.chars() {
        if c != '.' {
            count += 1;
        } else {
            if count >= digits {
                break;
            }
            found_pt = true;
        }

        d.push(c);

        if count >= digits && found_pt {
            break;
        }
    }
    d
}

pub fn mem_metric_digit(v: usize, sig: usize) -> String {
    if v == 0 || v > std::usize::MAX / 2 {
        return format!("{:>width$}", "unknown", width = sig + 3);
    }
    let vt = mem_metric(v);
    format!("{:>width$} {}", sig_dig(vt.0, sig), vt.1, width = sig + 1,)
}

const GREEK_SUFFIXES: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

pub fn greek(v: f64) -> String {

    let mut number = v;
    let mut multi = 0;

    while number >= 1000.0 && multi < GREEK_SUFFIXES.len()-1 {
        multi += 1;
        number /= 1024.0;
    }

    let mut s = format!("{}", number);
    s.truncate(4);
    if s.ends_with('.') {
        s.pop();
    }
    if s.len() < 4 { s.push(' ' ); }

    return format!("{}{}", s, GREEK_SUFFIXES[multi]);
}

