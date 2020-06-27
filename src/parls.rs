use std::thread::spawn;
use std::time::{Duration, Instant};
use structopt::StructOpt;

mod worker_queue;

use worker_queue::*;
use std::path::{PathBuf, Path};
use std::fs::{metadata, read_dir, symlink_metadata, Metadata, FileType};
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_family = "unix")]
use std::os::unix::fs::{PermissionsExt, MetadataExt};

#[cfg(target_family = "unix")]
use users::{get_user_by_uid, get_current_uid};

#[cfg(target_family = "windows")]
use std::os::windows::fs::{MetadataExt};


use lazy_static::lazy_static;
use std::thread;
use std::sync::{Arc, Mutex, MutexGuard};


use anyhow::{Context, anyhow, Result};
use std::time::SystemTime;
use std::collections::{LinkedList, BTreeMap, BinaryHeap, BTreeSet};

mod util;

use util::{mem_metric_digit, dur_from_str, greek, gettid};
use std::borrow::{Borrow, Cow};
use std::cmp::max;
use cpu_time::ProcessTime;
use crate::util::multi_extension;


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
    #[structopt(name = "DIRECTORY", parse(try_from_str = dir_check))]
    /// A list of directories to walk
    pub dir: PathBuf,

    #[structopt(short = "u", long = "usage_trees")]
    /// Disk usage mode - do not write the files found
    pub usage_mode: bool,

    #[structopt(short = "l", name = "list_files")]
    /// Report top usage limit
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
    /// Latency vs throughput:
    /// The theory here is that parallel listing overcomes latency issues
    /// by having multiple requests in play at once, and is not cpu bound.
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

    #[structopt(long = "write_thread_status")]
    /// Writes thread status every ticker interval - used to debug things
    pub write_thread_status: bool,

    #[structopt(long = "file_newer_than", parse(try_from_str = parse_timespec))]
    /// Only count/sum entries newer than this age
    pub file_newer_than: Option<SystemTime>,

    #[structopt(long = "file_older_than", parse(try_from_str = parse_timespec))]
    /// Only count/sum entries older than this age
    pub file_older_than: Option<SystemTime>,

    #[structopt(long = "write_thread_cpu_time")]
    /// write cpu time consumed by each thread
    pub write_thread_cpu_time: bool,
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

lazy_static! {
    static ref CLI: ParLsCfg = {
       get_cli()
    };
    static ref EXE: String = get_exe_name();
}

fn get_cli() -> ParLsCfg {
    let mut cfg = ParLsCfg::from_args();
    if cfg.no_threads == 0 {
        cfg.no_threads = max(num_cpus::get(), 4);
    }
    if !cfg.usage_mode && !cfg.list_files {
        cfg.usage_mode = true;
    }
    cfg
}

fn get_exe_name() -> String {
    std::env::args().nth(0).unwrap()
}

fn read_dir_thread(queue: &mut WorkerQueue<Option<PathBuf>>, out_q: &mut WorkerQueue<Option<Vec<(PathBuf, Metadata)>>>, t_status: &mut Arc<Mutex<ThreadStatus>>) {
    // get back to work slave loop....
    let t_cpu_time = cpu_time::ThreadTime::now();
    loop {
        match _read_dir_worker(queue, out_q, t_status) {
            Err(e) => {
                // filthy filthy error catch
                eprintln!("{}: major error: {}  cause: {}", *EXE, e, e.root_cause());
            }
            Ok(()) => break,
        }
    }

    if CLI.write_thread_cpu_time {
        eprintln!("read dir thread cpu time: {:.3}", t_cpu_time.elapsed().as_secs_f64());
    }

}

fn _read_dir_worker(queue: &mut WorkerQueue<Option<PathBuf>>, out_q: &mut WorkerQueue<Option<Vec<(PathBuf, Metadata)>>>, t_status: &mut Arc<Mutex<ThreadStatus>>) -> Result<()> {
    loop {
        if CLI.write_thread_status {
            if let Ok(ref mut guard) = t_status.lock() {
                guard.set_state("pop blocked");
                guard.set_tid(gettid());
            }
        }

        match queue.pop() {
            None => break,
            Some(p) => { //println!("path: {}", p.to_str().unwrap()),
                if CLI.verbose > 1 {
                    if p.to_str().is_none() { break; } else { eprintln!("{}: listing for {}", *EXE, p.to_str().unwrap()); }
                }
                let mut other_dirs = vec![];
                let mut metalist = vec![];
                if CLI.write_thread_status {
                    t_status.lock().unwrap().set_state("reading dir");
                }
                let dir_itr = match std::fs::read_dir(&p) {
                    Err(e) => {
                        eprintln!("{}: stat of dir: '{}', error: {}", *EXE, p.display(), e);
                        continue;
                    }
                    Ok(i) => i,
                };
                for entry in dir_itr {
                    let entry = entry?;
                    let path = entry.path();
                    let md = match symlink_metadata(&entry.path()) {
                        Err(e) => {
                            eprintln!("{}: stat of file for symlink: '{}', error: {}", *EXE, p.display(), e);
                            continue;
                        }
                        Ok(md) => md,
                    };
                    if CLI.verbose > 2 {
                        eprintln!("{}: raw meta: {:#?}", *EXE, &md);
                    }
                    let file_type: FileType = md.file_type();
                    if !file_type.is_symlink() {
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
                        if CLI.verbose > 0 { eprintln!("{}: skipping sym link: {}", *EXE, path.to_string_lossy()); }
                    }
                }

                if CLI.write_thread_status {
                    t_status.lock().unwrap().set_state("push meta");
                }
                out_q.push(Some(metalist))?;

                if CLI.write_thread_status {
                    t_status.lock().unwrap().set_state("push dirs");
                }
                for d in other_dirs {
                    queue.push(Some(d));
                }
            }
        }
    }
    if CLI.write_thread_status {
        t_status.lock().unwrap().set_state("exit");
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
    println!("{}{}{}{}{}{}{:o}{}{}{}{}", file_type, CLI.delimiter, path.to_string_lossy(),
             CLI.delimiter, meta.size(), CLI.delimiter, meta.permissions().mode(), CLI.delimiter,
             meta.uid(), CLI.delimiter, meta.modified()?.duration_since(SystemTime::UNIX_EPOCH)?.as_secs());
    Ok(())
}


#[derive(Eq, Debug)]
struct TrackedPath {
    size: u64,
    path: PathBuf,
}

#[derive(Eq, Debug)]
struct TrackedExtension {
    size: u64,
    extension: String,
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

impl Ord for TrackedExtension {
    fn cmp(&self, other: &TrackedExtension) -> std::cmp::Ordering {
        self.size.cmp(&other.size).reverse()
    }
}

impl PartialOrd for TrackedExtension {
    fn partial_cmp(&self, other: &TrackedExtension) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TrackedExtension {
    fn eq(&self, other: &TrackedExtension) -> bool {
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

#[derive(Debug, Clone)]
struct ThreadStatus {
    name: String,
    // this does NOT allow dynamic strings to be placed
    // here but it is less overhead OR maybe
    // optimization overkill
    state: &'static str,
    tid: usize,
}

impl ThreadStatus {
    pub fn new(state: &'static str, name: &str) -> ThreadStatus {
        ThreadStatus {
            name: name.to_string(),
            state: state,
            tid: 0,
        }
    }

    pub fn set_state(&mut self, state: &'static str) {
        self.state = state;
    }
    pub fn set_tid(&mut self, tid: usize) {
        self.tid = tid;
    }
}

impl DirStats {
    pub fn new() -> Self {
        DirStats { size_recursively: 0, size_directly: 0, file_count_recursively: 0, file_count_directly: 0, dir_count_directly: 0, dir_count_recursively: 0 }
    }
}

#[derive(Debug)]
struct UserUsage {
    size: u64,
    uid: u32,
}

struct AllStats {
    dtree: BTreeMap<PathBuf, DirStats>,
    extensions: BTreeMap<String, u64>,
    user_map: BTreeMap<u32, (u64, u64)>,
    top_dir: BinaryHeap<TrackedPath>,
    top_cnt_dir: BinaryHeap<TrackedPath>,
    top_cnt_file: BinaryHeap<TrackedPath>,
    top_cnt_overall: BinaryHeap<TrackedPath>,
    top_dir_overall: BinaryHeap<TrackedPath>,
    top_files: BinaryHeap<TrackedPath>,
    top_ext: BinaryHeap<TrackedExtension>,
    total_usage: u64,
}

fn track_top_n_ext(heap: &mut BinaryHeap<TrackedExtension>, ext: &String, s: u64, limit: usize) -> Result<bool> {
    if s == 0 { return Ok(false); }

    if limit > 0 {
        if heap.len() < limit {
            heap.push(TrackedExtension { size: s, extension: ext.clone() });
            return Ok(true);
        } else if heap.peek().expect("internal error: cannot peek when the size is greater than 0!?").size < s {
            heap.pop();
            heap.push(TrackedExtension { size: s, extension: ext.clone() });
            return Ok(true);
        }
    }
    Ok(false)
}

fn track_top_n(heap: &mut BinaryHeap<TrackedPath>, p: &PathBuf, s: u64, limit: usize) -> Result<bool> {
    if s == 0 { return Ok(false); }

    if limit > 0 {
        if heap.len() < limit {
            heap.push(TrackedPath { size: s, path: p.clone() });
            return Ok(true);
        } else if heap.peek().expect("internal error: cannot peek when the size is greater than 0!?").size < s {
            heap.pop();
            heap.push(TrackedPath { size: s, path: p.clone() });
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_family = "windows")]
fn write_meta(path: &PathBuf, meta: &Metadata) -> Result<()> {
    println!("{}{}{}", path.canonicalize()?.display(), CLI.delimiter, meta.len());
    Ok(())
}

fn perk_up_disk_usage(top: &mut AllStats, list: &Vec<(PathBuf, Metadata)>) -> Result<()> {
    if list.len() > 0 {
        if let Some(mut parent) = list[0].0.ancestors().skip(1).next() {
            if parent == CLI.dir { return Ok(()); }
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

                    track_top_n(&mut top.top_files, &afile.0.to_path_buf(), afile.1.len(), CLI.limit);

                    #[cfg(target_family = "windows")]
                        let uid = 0;
                    #[cfg(target_family = "unix")]
                        let uid = afile.1.uid();
                    let ref mut tt = *top.user_map.entry(uid).or_insert((0, 0));
                    tt.0 += 1;
                    tt.1 += afile.1.len();
                    top.total_usage += afile.1.len();

                    if filetype.is_file() {
                        if CLI.file_newer_than.map_or(true, |x| x < f_age) && CLI.file_older_than.map_or(true, |x| x > f_age) {
                            if let Some(ext) = multi_extension(&afile.0) {
                                match top.extensions.get_mut(ext.as_ref()) {
                                    Some(ext_sz) => *ext_sz += afile.1.len(),
                                    None => { top.extensions.insert(ext.to_string(), afile.1.len());},
                                }
                            };

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
                if let Some(mut nextpar) = parent.ancestors().skip(1).next() {
                    if parent == CLI.dir { break; }

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

fn file_track(startout: Instant,
              cputime: ProcessTime,
              stats: &mut AllStats,
              out_q: &mut WorkerQueue<Option<Vec<(PathBuf, Metadata)>>>,
              work_q: &mut WorkerQueue<Option<PathBuf>>,
              t_status: Arc<Mutex<ThreadStatus>>,
) -> Result<()> {
    let t_cpu_thread_time = cpu_time::ThreadTime::now();
    let count = Arc::new(AtomicUsize::new(0));

    let sub_count = count.clone();
    let sub_out_q = out_q.clone();
    let sub_work_q = work_q.clone();
    if CLI.progress {
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
    }

    loop {
        let mut c_t_status = t_status.clone();
        if CLI.write_thread_status {
            t_status.lock().unwrap().set_state("popping");
        }
        match out_q.pop() {
            Some(list) => {
                count.fetch_add(list.len(), Ordering::Relaxed);
                if CLI.usage_mode {
                    if CLI.write_thread_status {
                        t_status.lock().unwrap().set_state("recording stats");
                    }
                    perk_up_disk_usage(stats, &list)?;
                }
                if CLI.list_files {
                    for (path, md) in list {
                        let f_age = md.modified()?;
                        if CLI.file_newer_than.map_or(true, |x| x < f_age) && CLI.file_older_than.map_or(true, |x| x > f_age) {
                            if CLI.write_thread_status {
                                t_status.lock().unwrap().set_state("writing meta data");
                            }
                            write_meta(&path, &md)?
                        }
                    }
                }
            }
            None => break,
        }
    }
    if CLI.write_thread_status {
        t_status.lock().unwrap().set_state("tracks");
    }
    let last_count = count.load(Ordering::Relaxed);
    count.store(0, Ordering::Relaxed);

    if CLI.usage_mode {
        use num_format::{Locale, ToFormattedString};
        println!("Scanned {} files / {} usage in [{:.3} / {:.3}] (real / cpu) seconds",
                 last_count.to_formatted_string(&Locale::en),
                 greek(stats.total_usage as f64),
                 (Instant::now() - startout).as_secs_f64(), cputime.elapsed().as_secs_f64());
        for x in stats.dtree.iter() {
            track_top_n(&mut stats.top_dir, &x.0, x.1.size_directly, CLI.limit); // track single immediate space
            track_top_n(&mut stats.top_cnt_dir, &x.0, x.1.dir_count_directly, CLI.limit); // track dir with most # of dir right under it
            track_top_n(&mut stats.top_cnt_file, &x.0, x.1.file_count_directly, CLI.limit); // track dir with most # of file right under it
            track_top_n(&mut stats.top_cnt_overall, &x.0, x.1.file_count_recursively, CLI.limit); // track overall count
            track_top_n(&mut stats.top_dir_overall, &x.0, x.1.size_recursively, CLI.limit); // track overall size
        }

        for x in stats.extensions.iter() {
            track_top_n_ext(&mut stats.top_ext, &x.0, *x.1, CLI.limit);
        }
        if CLI.write_thread_status {
            t_status.lock().unwrap().set_state("print");
        }
        print_disk_report(&stats);
    }
    if CLI.write_thread_status {
        t_status.lock().unwrap().set_state("exit");
    }
    if CLI.write_thread_cpu_time {
        eprintln!("file track thread cpu time: {:.3}", t_cpu_thread_time.elapsed().as_secs_f64());
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

fn to_sort_vec_file_ext(heap: &BinaryHeap<TrackedExtension>) -> Vec<TrackedExtension> {
    let mut v = Vec::with_capacity(heap.len());
    for i in heap {
        v.push(TrackedExtension {
            extension: i.extension.clone(),
            size: i.size,
        });
    }
    v.sort();
    v
}

fn print_disk_report(stats: &AllStats) {
    #[derive(Debug)]
    struct U2u {
        count: u64,
        size: u64,
        uid: u32,
    }
    ;
    let mut user_vec: Vec<U2u> = stats.user_map.iter().map(|(&x, &y)| U2u { count: y.0, size: y.1, uid: x }).collect();
    user_vec.sort_by(|b, a| a.size.cmp(&b.size).then(b.uid.cmp(&b.uid)));
    //println!("File space scanned: {} and {} files in {} seconds", greek(total as f64), count, sec);
    if !user_vec.is_empty() {
        println!("\nSpace/file-count per user");
        for ue in &user_vec {
            #[cfg(target_family = "unix")]
            match get_user_by_uid(ue.uid) {
                None => println!("uid{:7} {} / {}", ue.uid, greek(ue.size as f64), ue.count),
                Some(user) => println!("{:10} {} / {}", user.name().to_string_lossy(), greek(ue.size as f64), ue.count),
            }
            #[cfg(target_family = "windows")]
            println!("uid{:>7} {} / {}", ue.uid, greek(ue.size as f64), ue.count);
        }
    }

    if !stats.top_dir.is_empty() {
        println!("\nTop dir with space usage directly inside them: {}", stats.top_dir.len());
        for v in to_sort_vec(&stats.top_dir) {
            println!("{:>14} {}", greek(v.size as f64), &v.path.display());
        }
    }

    if !stats.top_dir_overall.is_empty() {
        println!("\nTop dir size recursive: {}", stats.top_dir_overall.len());
        for v in to_sort_vec(&stats.top_dir_overall) {
            //let rel = v.path.as_path().strip_prefix(CLI.dir.as_path()).unwrap();
            println!("{:>14} {}", greek(v.size as f64), &v.path.display());
        }
    }
    use num_format::{Locale, ToFormattedString};

    if !stats.top_cnt_overall.is_empty() {
        println!("\nTop count of files recursive: {}", stats.top_cnt_overall.len());
        for v in to_sort_vec(&stats.top_cnt_overall) {
            println!("{:>14} {}", v.size.to_formatted_string(&Locale::en), &v.path.display());
        }
    }

    if !stats.top_cnt_file.is_empty() {
        println!("\nTop counts of files in a single directory: {}", stats.top_cnt_file.len());
        for v in to_sort_vec(&stats.top_cnt_file) {
            println!("{:>14} {}", v.size.to_formatted_string(&Locale::en), &v.path.display());
        }
    }

    if !stats.top_cnt_dir.is_empty() {
        println!("\nTop counts of directories in a single directory: {}", stats.top_cnt_dir.len());
        for v in to_sort_vec(&stats.top_cnt_dir) {
            println!("{:>14} {}", v.size.to_formatted_string(&Locale::en), &v.path.display());
        }
    }
    if !stats.top_files.is_empty() {
        println!("\nLargest file(s): {}", stats.top_files.len());
        for v in to_sort_vec(&stats.top_files) {
            println!("{:>14} {}", greek(v.size as f64), &v.path.display());
        }
    }
    if !stats.top_ext.is_empty() {
        println!("\nSpace by file extension: {}", stats.top_ext.len());
        for v in to_sort_vec_file_ext(&stats.top_ext) {
            println!("{:>14} {}", greek(v.size as f64), &v.extension);
        }
    }
}


fn parls() -> Result<()> {
    if CLI.verbose > 0 { eprintln!("CLI: {:#?}", *CLI); }
    let mut q: WorkerQueue<Option<PathBuf>> = WorkerQueue::new(CLI.no_threads, CLI.queue_limit);
    let mut oq: WorkerQueue<Option<Vec<(PathBuf, Metadata)>>> = WorkerQueue::new(1, 0);

    let mut allstats = AllStats {
        dtree: BTreeMap::new(),
        extensions: BTreeMap::new(),
        top_files: BinaryHeap::new(),
        top_dir: BinaryHeap::new(),
        top_cnt_dir: BinaryHeap::new(),
        top_cnt_file: BinaryHeap::new(),
        top_cnt_overall: BinaryHeap::new(),
        top_dir_overall: BinaryHeap::new(),
        top_ext: BinaryHeap::new(),
        user_map: BTreeMap::new(),
        total_usage: 0u64,
    };

    let mut thread_status: BTreeMap<usize, Arc<Mutex<ThreadStatus>>> = BTreeMap::new();

    q.push(Some(CLI.dir.to_path_buf())).with_context(|| format!("Cannot push top path: {}", CLI.dir.display()))?;
    let startout = Instant::now();
    let startcpu = ProcessTime::now();

    let mut handles = vec![];
    for i in 0..CLI.no_threads {
        let mut q = q.clone();
        let mut oq = oq.clone();
        let mut t_status = Arc::new(Mutex::new(ThreadStatus::new("NA", "read_dir")));
        let mut c_t_status = t_status.clone();
        thread_status.insert(thread_status.len(), t_status);
        let h = spawn(move || read_dir_thread(&mut q, &mut oq, &mut c_t_status));
        handles.push(h);
    }

    let w_h = {
        let mut c_oq = oq.clone();
        let mut c_q = q.clone();
        let mut ft_status = Arc::new(Mutex::new(ThreadStatus::new("NA", "file_trk")));
        let mut c_ft_status = ft_status.clone();
        thread_status.insert(thread_status.len(), ft_status);
        spawn(move || file_track(startout, startcpu, &mut allstats, &mut c_oq, &mut c_q, c_ft_status))
    };

    if CLI.write_thread_status {
        thread::spawn(move || {
            let mut last = 0;
            let start_f = Instant::now();

            loop {
                thread::sleep(Duration::from_millis(CLI.ticker_interval));
                for (i, ts) in thread_status.iter() {
                    let ts_x = ts.lock().unwrap();
                    eprintln!("index: {:2} {}  tid:  {:6}  status: \"{}\"", i, &ts_x.name, &ts_x.tid,  &ts_x.state);
                }
                eprintln!();
            }
        });
    }
    match (CLI.list_files, CLI.usage_mode) {
        (true, true) => println!("List file stats and disk usage summary for: {}", CLI.dir.display()),
        (false, true) => println!("Scanning disk usage summary for: {}", CLI.dir.display()),
        (true, false) => println!("List file stats under: {}", CLI.dir.display()),
        _ => Err(anyhow!("Error - neither usage or list mode specified"))?,
    }
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


    println!("last cpu time: {}", startcpu.elapsed().as_secs_f32());
    Ok(())
}

