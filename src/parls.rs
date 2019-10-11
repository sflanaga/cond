use std::thread::spawn;
use std::time::Duration;
use structopt::StructOpt;

mod worker_queue;

use worker_queue::*;
use std::path::PathBuf;
use std::fs::{metadata, read_dir, symlink_metadata};
use lazy_static::lazy_static;

fn main() {
    if let Err(err) = parls() {
        eprintln!("error: {}", &err);
        std::process::exit(1);
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

    #[structopt(short = "q", long = "queue_limit", default_value("10000"))]
    /// Limit of the queue size so that we do not get too greedy with memory
    pub queue_limit: usize,

    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,
}

fn worker(queue: &mut WorkerQueue<Option<PathBuf>>) {
    // get back to work slave loop....
    loop {
        match _worker(queue) {
            Err(e) => {
                // filthy filthy error catch
                eprintln!("error: {}   get back to work", e);
            }
            Ok(()) => return,
        }
    }
}

fn _worker(queue: &mut WorkerQueue<Option<PathBuf>>) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        match queue.pop() {
            None => break,
            Some(p) => { //println!("path: {}", p.to_str().unwrap()),
                if CLI.verbose > 0 {
                    eprintln!("listing for {}", p.to_string_lossy());
                }
                let mut other_dirs = vec![];
                {
                    for entry in std::fs::read_dir(p)? {
                        let entry = entry?;
                        let path = entry.path();
                        let md = symlink_metadata(&path)?;
                        if CLI.verbose > 1 {
                            eprintln!("raw meta: {:#?}", &md);
                        }
                        let file_type = md.file_type();
                        if !file_type.is_symlink() {
                            if file_type.is_file() {
                                println!("{}", path.to_string_lossy());
                            } else if file_type.is_dir() {
                                other_dirs.push(path);
                            }
                        } else {
                            if CLI.verbose>0 { eprintln!("skipping sym link: {}", path.to_string_lossy()); }
                        }
                    }
                    // making extra sure we drop or close out the read_dir
                }
                for d in other_dirs {
                    queue.push(Some(d));
                }
            }
        }
    }
    Ok(())
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


fn parls() -> Result<(), Box<dyn std::error::Error>> {
    let mut q: WorkerQueue<Option<PathBuf>> = WorkerQueue::new(CLI.no_threads,  CLI.queue_limit);

    CLI.dirs.iter().for_each(|x| q.push(Some(x.to_path_buf())));

    let mut handles = vec![];
    for i in 0..CLI.no_threads {
        let mut q = q.clone();
        let h = spawn(move || worker(&mut q));
        handles.push(h);
    }

    loop {
        let x = q.wait_for_finish_timeout(Duration::from_millis(250));
        if x == CLI.no_threads { break; } else {
            q.notify_all();
            if CLI.verbose > 0 {
                q.status();
            }
        }
    }

    if CLI.verbose > 0 { eprintln!("finished so sends the Nones and join"); }
    for _ in 0..CLI.no_threads { q.push(None); }
    for h in handles {
        h.join();
    }

    Ok(())
}










