use structopt::StructOpt;
use std::path::PathBuf;
use std::time::{SystemTime, Duration};
use anyhow::{Result, anyhow, Context};
use std::fs::symlink_metadata;
use lazy_static::lazy_static;
use std::cmp::max;

lazy_static! {
    pub static ref CLI: ParLsCfg = {
       get_cli()
    };
    pub static ref EXE: String = get_exe_name();
}


#[derive(StructOpt, Debug, Clone)]
#[structopt(
global_settings(& [structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands, structopt::clap::AppSettings::DeriveDisplayOrder]),
//raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
)]
/// Parallel file system lister / Usage Statistics Summary
///
/// A "recursive" listing of files and/or usage statistics summary is created.
///
/// Latency vs throughput:
/// The theory here is that parallel listing overcomes latency issues
/// by having multiple requests in play at once, and is not cpu bound.
/// This can overcome remote file system metadata throughput of a
/// NAS via many simultaneous i/o requests.
///
/// Also, of note is that each "opendir" is finished to completion so that
/// the directory is left open for minimal amount of time, but this comes at a
/// cost of significantly more memory.
pub struct ParLsCfg {
    #[structopt(name = "DIRECTORY", parse(try_from_str = dir_check))]
    /// A list of directories to walk
    pub dir: PathBuf,

    #[structopt(short = "u", long = "usage_trees")]
    /// Write disk usage summary
    pub usage_mode: bool,

    #[structopt(short = "l", name = "list_files")]
    /// Write file list
    pub list_files: bool,

    #[structopt(short = "n", name = "top_n_limit", default_value("15"))]
    /// Report top usage limit
    pub limit: usize,

    #[structopt(short = "d", long = "delimiter", default_value("|"))]
    /// Disk usage mode - do not write the files found
    pub delimiter: char,

    #[structopt(short = "t", long = "worker_threads", default_value("0"))]
    /// Number worker threads
    ///
    /// defaults to 0 which means # of cpus or at least 4
    pub no_threads: usize,

    #[structopt(short = "q", long = "queue_limit", default_value("0"))]
    /// Limit of the queue size so that we do not get too greedy with memory - 0 means no limit
    pub queue_limit: usize,

    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,

    #[structopt(short = "i", long = "ticker_interval", default_value("200"))]
    /// Interval at which stats are written - 0 means no ticker is run
    pub ticker_interval: u64,

    #[structopt(long = "progress")]
    /// Writes progress stats on every ticker interval
    pub progress: bool,

    #[structopt(long = "write_thread_status", conflicts_with("write_thread_status_on_enter_key"))]
    /// Writes thread status every ticker interval - used to debug things
    pub t_status_interval: bool,

    #[structopt(long = "t_status_on_key", conflicts_with("write_thread_status"))]
    /// Writes thread status when stdin sees a line entered by user
    pub t_status_on_key: bool,

    #[structopt(long = "file_newer_than", parse(try_from_str = parse_timespec))]
    /// Only count/sum entries newer than this age
    pub file_newer_than: Option<SystemTime>,

    #[structopt(long = "file_older_than", parse(try_from_str = parse_timespec))]
    /// Only count/sum entries older than this age
    pub file_older_than: Option<SystemTime>,

    #[structopt(long = "write_thread_cpu_time")]
    /// write cpu time consumed by each thread
    pub write_thread_cpu_time: bool,

    #[structopt(skip)]
    pub update_status: bool,
}

fn parse_timespec(str: &str) -> Result<SystemTime> {
    let dur = dur_from_str(str)?;
    let ret = SystemTime::now() - dur;
    Ok(ret)
}

fn dir_check(s: &str) -> Result<PathBuf> {
    let p = PathBuf::from(s);

    let m = symlink_metadata(&p).with_context(|| format!("path specified: {}", s))?;
    if !m.is_dir() {
        return Err(anyhow!("{} not a directory", s));
    }

    Ok(p)
}

//noinspection ALL
fn get_cli() -> ParLsCfg {
    let mut cfg = ParLsCfg::from_args();
    if cfg.no_threads == 0 {
        cfg.no_threads = max(num_cpus::get(), 4);
    }
    if !cfg.usage_mode && !cfg.list_files {
        cfg.usage_mode = true;
    }
    if cfg.t_status_on_key || cfg.t_status_interval {
        cfg.update_status = true;
    }
    cfg
}

pub fn dur_from_str(s: &str) -> Result<Duration> {
    let mut _tmp = String::new();
    let mut tot_secs = 0u64;
    for c in s.chars() {
        if c >= '0' && c <= '9' { _tmp.push(c); } else {
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

fn get_exe_name() -> String {
    std::env::args().nth(0).unwrap()
}



