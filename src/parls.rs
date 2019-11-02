use std::thread::spawn;
use std::time::{Duration, Instant};
use structopt::StructOpt;

mod worker_queue;

use worker_queue::*;
use std::path::PathBuf;
use std::fs::{metadata, read_dir, symlink_metadata, Metadata, FileType};
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(target_family = "unix")]
use std::os::unix::fs::{PermissionsExt, MetadataExt};


use lazy_static::lazy_static;
use std::thread;
use std::sync::Arc;

use anyhow::{Context, Result};

fn main() {
    if let Err(err) = parls() {
        eprintln!("ERROR in main: {}", &err);
        std::process::exit(11);
    }
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(
global_settings(& [structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands, structopt::clap::AppSettings::DeriveDisplayOrder]),
//raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
author, about
)]
///
/// Perform a sql-like group by on csv or arbitrary text files organized into lines.  You can use multiple regex entries to attempt to capture a record across multiple lines such as xml files, but this is very experiemental.
///
pub struct ParLsCfg {
    #[structopt(short = "d", long = "dirs", name = "dirs")]
    /// A list of directories to walk
    pub dirs: Vec<PathBuf>,

    #[structopt(short = "n", long = "worker_threads", default_value("0"))]
    /// Number worker threads - defaults to 0 which means # of cpus
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
}

fn worker(queue: &mut WorkerQueue<Option<PathBuf>>, out_q: &mut WorkerQueue<Option<(PathBuf, Metadata)>>) {
    // get back to work slave loop....
    loop {
        match _worker(queue, out_q) {
            Err(e) => {
                // filthy filthy error catch
                eprintln!("continue error: {}  cause: {}", e, e.root_cause());
            }
            Ok(()) => return,
        }
    }
}

fn _worker(queue: &mut WorkerQueue<Option<PathBuf>>,out_q: &mut WorkerQueue<Option<(PathBuf, Metadata)>>) -> Result<()> {
    loop {
        match queue.pop() {
            None => break,
            Some(p) => { //println!("path: {}", p.to_str().unwrap()),
                if CLI.verbose > 0 {
                    if p.to_str().is_none() { break; }
                    else {eprintln!("listing for {}", p.to_str().unwrap());}
                }
                let mut other_dirs = vec![];
                for entry in std::fs::read_dir(&p).with_context(||format!("read_dir on path {} failed", p.display()))?
                    //.map_err(|e| Err(format!("cannot read_dir for path {} due to error: {}", p.display(), e.to_string())))?
                    {
                    let entry = entry?;
                    let path = entry.path();
                    let md = symlink_metadata(&entry.path()).with_context(||format!("stat of path {} failed", path.display()))?;
                    if CLI.verbose > 2 {
                        eprintln!("raw meta: {:#?}", &md);
                    }
                    let file_type:FileType = md.file_type();
                    if !file_type.is_symlink() {
                        let path = entry.path().canonicalize().with_context(||"convert to full path error")?;
                        if file_type.is_file() {
                            let ep = path.display();
                            out_q.push(Some((path,md)))?;
                            //write_meta(&path, &md);
                        } else if file_type.is_dir() {
                            other_dirs.push(path);
                        }
                    } else {
                        if CLI.verbose > 0 { eprintln!("skipping sym link: {}", path.to_string_lossy()); }
                    }
                }
                for d in other_dirs {
                    queue.push(Some(d));
                }
            }
        }
    }
    Ok(())
}

#[cfg(target_family = "unix")]
fn write_meta(path: &PathBuf, meta: &Metadata) {
    println!("{}|{}|{:o}|{}", path.to_string_lossy(), meta.size(), meta.permissions().mode(), meta.uid());
}

#[cfg(target_family = "windows")]
fn write_meta(path: &PathBuf, meta: &Metadata) {
    println!("{}|{}", path.to_string_lossy(), meta.len());

}

lazy_static! {
    static ref CLI: ParLsCfg = {
       get_cli()
    };
}

fn get_cli() -> ParLsCfg {
    let mut cfg = ParLsCfg::from_args();
    if cfg.no_threads == 0 {
        cfg.no_threads = num_cpus::get();
    }
    cfg
}

fn file_track(out_q: &mut WorkerQueue<Option<(PathBuf, Metadata)>>, work_q: &mut WorkerQueue<Option<PathBuf>>) {
    let mut count = Arc::new(AtomicUsize::new(0));

    let sub_count = count.clone();
    let sub_out_q = out_q.clone();
    let sub_work_q = work_q.clone();
    thread::spawn(move|| {
        let mut last = 0;
        let start_f = Instant::now();

        loop {
            thread::sleep(Duration::from_millis(CLI.ticker_interval));
            let thiscount = sub_count.load(Ordering::Relaxed);

            let elapsed = start_f.elapsed();
            let sec: f64 = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
            let rate = (thiscount as f64 / sec) as usize;

            let stats_workers:QueueStats = sub_work_q.get_stats();
            let stats_io:QueueStats = sub_out_q.get_stats();
            eprint!("\rfiles: {}  rate: {}  blocked: {}  directory q len: {}  io q len: {}                 ",
                    thiscount, rate, stats_workers.curr_poppers, stats_workers.curr_q_len, stats_io.curr_q_len);
            if thiscount < last {
                break;
            }
            last = thiscount;
        }
    });

    loop {
        match out_q.pop() {
            Some((path, md)) => write_meta(&path, &md),
            None => break,
        }
        count.fetch_add(1, Ordering::Relaxed);
    }
    count.store(0, Ordering::Relaxed);
}

fn parls() -> Result<()> {
    let mut q: WorkerQueue<Option<PathBuf>> = WorkerQueue::new(CLI.no_threads,  CLI.queue_limit);
    let mut oq: WorkerQueue<Option<(PathBuf, Metadata)>> = WorkerQueue::new(1,  0);

    CLI.dirs.iter().for_each(|x| q.push(Some(x.to_path_buf())).expect("Could not evern prime the pump with initial directories"));

    let mut handles = vec![];
    for i in 0..CLI.no_threads {
        let mut q = q.clone();
        let mut oq = oq.clone();
        let h = spawn(move || worker(&mut q, &mut oq));
        handles.push(h);
    }

    let mut c_oq = oq.clone();
    let mut c_q = q.clone();
    let w_h = spawn(move||file_track(&mut c_oq, &mut c_q));

    let n_threads = CLI.no_threads;
    loop {
        let x = q.wait_for_finish_timeout(Duration::from_millis(250))?;
        if x != -1 {break;}
        if CLI.verbose > 0 { q.status() };
    }
    if CLI.verbose > 0 { q.print_max_queue();}
    if CLI.verbose > 0 { eprintln!("finished so sends the Nones and join"); }
    for _ in 0..CLI.no_threads { q.push(None); }
    for h in handles {
        h.join();
    }
    if CLI.verbose > 0 {eprintln!("waiting on out finish");}
    oq.wait_for_finish()?;
    if CLI.verbose > 0 {eprintln!("push none of out queue");}
    oq.push(None)?;
    if CLI.verbose > 0 {eprintln!("joining out thread");}
    w_h.join();


    Ok(())
}



