use std::thread::spawn;
use std::time::Duration;

mod worker_queue;
use worker_queue::*;
fn main() {
    if let Err(err) = _main() {
        eprintln!("error: {}", &err);
        std::process::exit(1);
    }
}

fn _main() -> Result<(), Box<dyn std::error::Error>> {
    let no_workers = 4;

    let mut q: WorkerQueue<Option<usize>> = WorkerQueue::new(no_workers, 200);
    q.push(Some(1000000));
    let mut handles = vec![];
    for t in 0..no_workers {
        let mut q_c = q.clone();
        let h = spawn(move || {
            let mut c = 0usize;
            loop {
                match q_c.pop() {
                    None => {
                        println!("got none so quit");
                        break;
                    },
                    Some(s) => {
                        c += 1;
                        let n = s - 1;
                        if n % 30000 == 0 {
                            println!("at {}", n);
                        }
                        if n > 0 {
                            q_c.push(Some(n));
                        } else if n == 0 {
                            println!("got last one");
                        }
                    }
                }
            }
            println!("quiting... worker {}", c);
        });
        handles.push(h);
    }
    println!("waiting for finish");

//    for i in 0..10 {
//        q.status();
//        thread::sleep(Duration::from_millis(500));
//    }

    let mut q_stops = 0;
    loop {
        q_stops = q.wait_for_finish_timeout(Duration::from_millis(500))?;
        if q_stops != -1 { break; }
        println!("WAIT timed-out");
    }

    println!("FINISHED so pushing Nones");
    for i in 0..q_stops {
        q.push(None);
    }
    for h in handles {
        h.join();
        println!("worker joined");
    }
    Ok(())
}