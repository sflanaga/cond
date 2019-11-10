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
#[cfg(target_family = "windows")]
use std::os::windows::fs::{MetadataExt};


use lazy_static::lazy_static;
use std::thread;
use std::sync::Arc;

use users::{get_user_by_uid, get_current_uid};

use anyhow::{Context, Result};
use std::time::SystemTime;
use std::collections::{LinkedList, BTreeMap, BinaryHeap};

mod util;

use util::{mem_metric_digit, dur_from_str, greek};
use std::borrow::Borrow;


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
    #[structopt(name = "DIRECTORY")]
    /// A list of directories to walk
    pub dir: PathBuf,

    #[structopt(short = "l", name = "top_n_limit", default_value("15"))]
    /// Report top usage limit
    pub limit: usize,

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

    #[structopt(short = "u", long = "usage_trees")]
    /// Disk usage mode - do not write the files found
    pub usage_mode: bool,

    #[structopt(long = "file_newer_than", parse(try_from_str = parse_timespec))]
    /// Only count/sum entries newer than this age
    pub file_newer_than: Option<SystemTime>,

    #[structopt(long = "file_older_than", parse(try_from_str = parse_timespec))]
    /// Only count/sum entries older than this age
    pub file_older_than: Option<SystemTime>,
}

fn parse_timespec(str: &str) -> Result<SystemTime> {
    let dur = dur_from_str(str)?;
    let ret = SystemTime::now() - dur;
    Ok(ret)
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


fn worker(queue: &mut WorkerQueue<Option<PathBuf>>, out_q: &mut WorkerQueue<Option<Vec<(PathBuf, Metadata)>>>) {
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

fn _worker(queue: &mut WorkerQueue<Option<PathBuf>>, out_q: &mut WorkerQueue<Option<Vec<(PathBuf, Metadata)>>>) -> Result<()> {
    loop {
        match queue.pop() {
            None => break,
            Some(p) => { //println!("path: {}", p.to_str().unwrap()),
                if CLI.verbose > 0 {
                    if p.to_str().is_none() { break; } else { eprintln!("listing for {}", p.to_str().unwrap()); }
                }
                let mut other_dirs = vec![];
                let mut metalist = vec![];
                for entry in std::fs::read_dir(&p).with_context(|| format!("read_dir on path {} failed", p.display()))?
                    //.map_err(|e| Err(format!("cannot read_dir for path {} due to error: {}", p.display(), e.to_string())))?
                    {
                        let entry = entry?;
                        let path = entry.path();
                        let md = symlink_metadata(&entry.path()).with_context(|| format!("stat of path {} failed", path.display()))?;
                        if CLI.verbose > 2 {
                            eprintln!("raw meta: {:#?}", &md);
                        }
                        let file_type: FileType = md.file_type();
                        if !file_type.is_symlink() {
                            let path = entry.path().canonicalize().with_context(|| "convert to full path error")?;
                            if file_type.is_file() {
                                let f_age = md.modified()?;
                                if CLI.file_newer_than.map_or(true, |x| x < f_age) && CLI.file_older_than.map_or(true, |x| x > f_age) {
                                    metalist.push((path.clone(), md.clone()));
                                    //write_meta(&path, &md);
                                }
                            } else if file_type.is_dir() {
                                metalist.push((path.clone(), md));
                                other_dirs.push(path);
                            }
                        } else {
                            if CLI.verbose > 0 { eprintln!("skipping sym link: {}", path.to_string_lossy()); }
                        }
                    }
                out_q.push(Some(metalist))?;

                for d in other_dirs {
                    queue.push(Some(d));
                }
            }
        }
    }
    Ok(())
}

#[cfg(target_family = "unix")]
fn write_meta(path: &PathBuf, meta: &Metadata) -> Result<()> {
    let file_type = match meta.file_type() {
        x if x.is_file() => 'f',
        x if x.is_dir() => 'd',
        x if x.is_symlink() => 's',
        _ => 'N',
    };
    println!("{}|{}|{}|{:o}|{}|{}", file_type, path.to_string_lossy(), meta.size(), meta.permissions().mode(), meta.uid(), meta.modified()?.duration_since(SystemTime::UNIX_EPOCH)?.as_secs());
    Ok(())
}


#[derive(Eq, Debug)]
struct TrackedPath {
    size: u64,
    path: String,
}

impl Ord for TrackedPath {
    fn cmp(&self, other: &TrackedPath) -> std::cmp::Ordering {
        self.size.cmp(&other.size).reverse()
    }
}

impl PartialOrd for TrackedPath {
    fn partial_cmp(&self, other: &TrackedPath) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TrackedPath {
    fn eq(&self, other: &TrackedPath) -> bool {
        self.size == other.size
    }
}

#[derive(Debug, Clone)]
struct DirStats {
    size_directly: u64,
    size_recursively: u64,
    file_count_directly: u64,
    file_count_recursively: u64,
    dir_count_directly: u64,
    dir_count_recursively: u64,
}


impl DirStats {
    pub fn new() -> Self {
        DirStats { size_recursively: 0, size_directly: 0, file_count_recursively: 0, file_count_directly: 0, dir_count_directly: 0, dir_count_recursively: 0 }
    }
}

#[derive(Debug)]
struct U2u {
    size: u64,
    uid: u32
}

struct AllStats {
    dtree: BTreeMap<PathBuf, DirStats>,
    user_map: BTreeMap<u32, u64>,
    top_dir: BinaryHeap<TrackedPath>,
    top_cnt_dir: BinaryHeap<TrackedPath>,
    top_cnt_file: BinaryHeap<TrackedPath>,
    top_cnt_overall: BinaryHeap<TrackedPath>,
    top_dir_overall: BinaryHeap<TrackedPath>,
    top_files: BinaryHeap<TrackedPath>,
}

fn track_top_n2(heap: &mut BinaryHeap<TrackedPath>, p: &PathBuf, s: u64, limit: usize) -> Result<bool> {
    if s == 0 { return Ok(false); }

    if limit > 0 {
        if heap.len() < limit {
            let p = p.to_string_lossy().to_string();
            heap.push(TrackedPath { size: s, path: p });
            return Ok(true);
        } else if heap.peek().expect("internal error: cannot peek when the size is greater than 0!?").size < s {
            heap.pop();
            let p = p.to_string_lossy().to_string();
            heap.push(TrackedPath { size: s, path: p });
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_family = "windows")]
fn write_meta(path: &PathBuf, meta: &Metadata) -> Result<()> {
    println!("{}|{}", path.to_string_lossy(), meta.len());
    Ok(())
}



fn perk_up_disk_usage(top: &mut AllStats, list: &Vec<(PathBuf, Metadata)>) -> Result<()> {
    if list.len() > 0 {
        if let Some(mut parent) = list[0].0.parent() {
            let dstats = {
                let mut dstats: &mut DirStats = if top.dtree.contains_key(parent) {
                    top.dtree.get_mut(parent).unwrap()
                } else {
                    let dstats = DirStats::new();
                    top.dtree.insert(parent.to_path_buf(), dstats);
                    top.dtree.get_mut(parent).unwrap()
                };
                for afile in list {

                    let filetype = afile.1.file_type();
                    let f_age = afile.1.modified()?;
                    track_top_n2(&mut top.top_files, &afile.0.to_path_buf(), afile.1.len(), CLI.limit);

                    #[cfg(target_family = "windows")]
                    let uid = 0;
                    #[cfg(target_family = "unix")]
                    let uid = afile.1.uid();
                    *top.user_map.entry(uid).or_insert(0) += afile.1.len();

                    if filetype.is_file() {
                        if CLI.file_newer_than.map_or(true, |x| x < f_age) && CLI.file_older_than.map_or(true, |x| x > f_age) {
                            dstats.file_count_directly += 1;
                            dstats.file_count_recursively += 1;
                            dstats.size_directly += afile.1.len();
                            dstats.size_recursively += afile.1.len();
                        }
                    } else if filetype.is_dir() {
                        if CLI.file_newer_than.map_or(true, |x| x < f_age) && CLI.file_older_than.map_or(true, |x| x > f_age) {
                            dstats.dir_count_directly += 1;
                            dstats.dir_count_recursively += 1;
                            // eprintln!("dir size {} :: {}", afile.0.display(), afile.1.len());
                            dstats.size_directly += afile.1.len();
                            dstats.size_recursively += afile.1.len();
                        }
                    }
                }
                dstats.clone()
            };
            // go up tree and add stuff
            loop {
                if let Some(mut nextpar) = parent.parent() {
                    if nextpar == parent {
                        break;
                    }
                    let mut upstats = if top.dtree.contains_key(nextpar) {
                        top.dtree.get_mut(nextpar).unwrap()
                    } else {
                        let dstats = DirStats::new();
                        top.dtree.insert(nextpar.to_path_buf(), dstats);
                        top.dtree.get_mut(nextpar).unwrap()
                    };
                    upstats.size_recursively += dstats.size_recursively;
                    upstats.file_count_recursively += dstats.file_count_recursively;
                    upstats.dir_count_recursively += dstats.dir_count_recursively;

                    //eprintln!("up: {} from {}", nextpar.display(), parent.display());
                    parent = nextpar;
                } else {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn file_track(stats: &mut AllStats, out_q: &mut WorkerQueue<Option<Vec<(PathBuf, Metadata)>>>, work_q: &mut WorkerQueue<Option<PathBuf>>) -> Result<()> {
    let mut count = Arc::new(AtomicUsize::new(0));

    let sub_count = count.clone();
    let sub_out_q = out_q.clone();
    let sub_work_q = work_q.clone();
    thread::spawn(move || {
        let mut last = 0;
        let start_f = Instant::now();

        loop {
            thread::sleep(Duration::from_millis(CLI.ticker_interval));
            let thiscount = sub_count.load(Ordering::Relaxed);

            let elapsed = start_f.elapsed();
            let sec: f64 = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
            let rate = (thiscount as f64 / sec) as usize;

            let stats_workers: QueueStats = sub_work_q.get_stats();
            let stats_io: QueueStats = sub_out_q.get_stats();
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
            Some(list) => {
                if CLI.usage_mode {
                    perk_up_disk_usage(stats, &list)?;
                } else {
                    for (path, md) in list {
                        let f_age = md.modified()?;
                        if CLI.file_newer_than.map_or(true, |x| x < f_age) && CLI.file_older_than.map_or(true, |x| x > f_age) {
                            write_meta(&path, &md)?
                        }
                    }
                }
            }
            None => break,
        }
        count.fetch_add(1, Ordering::Relaxed);
    }

    count.store(0, Ordering::Relaxed);

    if CLI.usage_mode {
        for x in stats.dtree.iter() {
            if CLI.verbose > 1 { println!("{},{},{}", x.0.display(), mem_metric_digit(x.1.size_recursively as usize, 4), x.1.file_count_recursively); }
            track_top_n2(&mut stats.top_dir, &x.0, x.1.size_directly, CLI.limit); // track single immediate space
            track_top_n2(&mut stats.top_cnt_dir, &x.0, x.1.dir_count_directly, CLI.limit); // track dir with most # of dir right under it
            track_top_n2(&mut stats.top_cnt_file, &x.0, x.1.file_count_directly, CLI.limit); // track dir with most # of file right under it
            track_top_n2(&mut stats.top_cnt_overall, &x.0, x.1.file_count_recursively, CLI.limit); // track overall count
            track_top_n2(&mut stats.top_dir_overall, &x.0, x.1.size_recursively, CLI.limit); // track overall size
        }
        print_top(&stats);
    }

    Ok(())
}

fn to_sort_vec(heap: &BinaryHeap<TrackedPath>) -> Vec<TrackedPath> {
    let mut v = Vec::with_capacity(heap.len());
    for i in heap {
        v.push(TrackedPath {
            path: i.path.clone(),
            size: i.size,
        });
    }
    v.sort();
    v
}

fn print_top(stats: &AllStats) {

    #[derive(Debug)]
    struct U2u {
        size: u64,
        uid: u32
    };
    let mut user_vec: Vec<U2u> = stats.user_map.iter().map( |(&x,&y)| U2u {size: y, uid:x } ).collect();
    user_vec.sort_by( |b,a| a.size.cmp(&b.size).then(b.uid.cmp(&b.uid)) );
        //println!("File space scanned: {} and {} files in {} seconds", greek(total as f64), count, sec);
        if !user_vec.is_empty() {
            println!("\nSpace used per user");
            for ue in &user_vec {
                match get_user_by_uid(ue.uid) {
                    None => println!("uid{:7} {} ", ue.uid, greek(ue.size as f64)),
                    Some(user) => println!("{:10} {} ", user.name().to_string_lossy(), greek(ue.size as f64)),
                }

            }
        }

    println!("\nTop dir with space usage directly inside them:");
    for v in to_sort_vec(&stats.top_dir) {
        println!("{:10} {}", greek(v.size as f64), v.path);
    }

    println!("\nTop dir size recursive:");
    for v in to_sort_vec(&stats.top_dir_overall) {
        println!("{:10} {}", greek(v.size as f64), v.path);
    }

    println!("\nTop count of files recursive:");
    for v in to_sort_vec(&stats.top_cnt_overall) {
        println!("{:10} {}", v.size, v.path);
    }

    println!("\nTop counts of files in a single directory:");
    for v in to_sort_vec(&stats.top_cnt_file) {
        println!("{:10} {}", v.size, v.path);
    }

    println!("\nTop counts of directories in a single directory:");
    for v in to_sort_vec(&stats.top_cnt_dir) {
        println!("{:10} {}", v.size, v.path);
    }

    println!("\nLargest file(s):");
    for v in to_sort_vec(&stats.top_files) {
        println!("{:10} {}", greek(v.size as f64), v.path);
    }
}

fn parls() -> Result<()> {
    if CLI.verbose > 0 { eprintln!("CLI: {:#?}", *CLI); }
    let mut q: WorkerQueue<Option<PathBuf>> = WorkerQueue::new(CLI.no_threads, CLI.queue_limit);
    let mut oq: WorkerQueue<Option<Vec<(PathBuf, Metadata)>>> = WorkerQueue::new(1, 0);

    let mut allstats = AllStats {
        dtree: BTreeMap::new(),
        top_files: BinaryHeap::new(),
        top_dir: BinaryHeap::new(),
        top_cnt_dir: BinaryHeap::new(),
        top_cnt_file: BinaryHeap::new(),
        top_cnt_overall: BinaryHeap::new(),
        top_dir_overall: BinaryHeap::new(),
        user_map: BTreeMap::new(),
    };

    q.push(Some(CLI.dir.to_path_buf())).expect("Cannot push first item");
    // CLI.dirs.iter().for_each(|x| q.push(Some(x.to_path_buf())).expect("Could not ever prime the pump with initial directories"));

    let mut handles = vec![];
    for i in 0..CLI.no_threads {
        let mut q = q.clone();
        let mut oq = oq.clone();
        let h = spawn(move || worker(&mut q, &mut oq));
        handles.push(h);
    }

    let mut c_oq = oq.clone();
    let mut c_q = q.clone();
    let w_h = spawn(move || file_track(&mut allstats, &mut c_oq, &mut c_q));

    let n_threads = CLI.no_threads;
    loop {
        let x = q.wait_for_finish_timeout(Duration::from_millis(250))?;
        if x != -1 { break; }
        if CLI.verbose > 0 { q.status() };
    }
    if CLI.verbose > 0 { q.print_max_queue(); }
    if CLI.verbose > 0 { eprintln!("finished so sends the Nones and join"); }
    for _ in 0..CLI.no_threads { q.push(None); }
    for h in handles {
        h.join();
    }
    if CLI.verbose > 0 { eprintln!("waiting on out finish"); }
    oq.wait_for_finish()?;
    if CLI.verbose > 0 { eprintln!("push none of out queue"); }
    oq.push(None)?;
    if CLI.verbose > 0 { eprintln!("joining out thread"); }
    w_h.join();


    Ok(())
}



