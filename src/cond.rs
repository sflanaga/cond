use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::collections::LinkedList;
use std::time::Duration;


struct InnerQ<T> {
    queue: LinkedList<T>,
    curr_waiters: usize,
    max_waiters: usize,
    limit: usize,
}

#[derive(Clone)]
struct Requeue<T> {
    tqueue: Arc<Mutex<InnerQ<T>>>,
    cond_more: Arc<Condvar>,
    cond_hasroom: Arc<Condvar>,
    looks_done: Arc<Condvar>,
}


impl<T> Requeue<T> {
    pub fn new(max_waits: usize, limit_: usize) -> Requeue<T> {
        Requeue {
            tqueue: Arc::new(Mutex::new(
                InnerQ {
                    queue: LinkedList::new(),
                    curr_waiters: 0,
                    max_waiters: max_waits,
                    limit: limit_,
                })),
            cond_more: Arc::new(Condvar::new()),
            cond_hasroom: Arc::new(Condvar::new()),
            looks_done: Arc::new(Condvar::new()),
        }
    }
    pub fn push(&mut self, item: T) {
        let mut l = self.tqueue.lock().unwrap();
        while l.limit > 0 && l.queue.len() >= l.limit {
            l = self.cond_hasroom.wait(l).unwrap();
        }
        l.queue.push_front(item);

        self.cond_more.notify_one();
    }
    pub fn pop(&mut self) -> T {
        let mut l = self.tqueue.lock().unwrap();
        l.curr_waiters += 1;
        while l.queue.len() < 1 {
            if l.curr_waiters == l.max_waiters {
                self.looks_done.notify_one();
            }
            l = self.cond_more.wait(l).unwrap();
        }
        let res = l.queue.pop_back().unwrap();
        if l.limit > 0 {
            self.cond_hasroom.notify_one();
            l.curr_waiters -= 1;
        }
        res
    }
    pub fn waiters(&self) -> usize {
        let mut l = self.tqueue.lock().unwrap();
        l.curr_waiters
    }

    pub fn wait_for_finish_timeout(&self, dur: Duration) -> usize {
        let mut l = self.tqueue.lock().unwrap();
        while !(l.queue.len() <= 0 && l.curr_waiters == l.max_waiters) {
            let x = self.looks_done.wait_timeout(l, dur).unwrap();
            l = x.0;
            if x.1.timed_out() { return 0; }
        }
        l.curr_waiters
    }

    pub fn wait_for_finish(&self) -> usize {
        let mut l = self.tqueue.lock().unwrap();
        while !(l.queue.len() <= 0 && l.curr_waiters == l.max_waiters) {
            l = self.looks_done.wait(l).unwrap();
        }
        l.curr_waiters
    }

    pub fn status(&self) {
        let mut l = self.tqueue.lock().unwrap();
        println!("q len: {}  waiters: {}", l.queue.len(), l.curr_waiters);
    }
}

fn main() {
    let no_workers = 4;

    let mut q: Requeue<Option<usize>> = Requeue::new(no_workers, 200);
    q.push(Some(1000000));
    let mut handles = vec![];
    for t in 0..no_workers {
        let mut q_c = q.clone();
        let h = thread::spawn(move || {
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
        println!("WAITING");
        q_stops = q.wait_for_finish_timeout(Duration::from_millis(500));
        if q_stops == 0 {
            println!("timedout on wait");
        } else {
            println!("wait DONE");
            break;
        }
    }

    println!("FINISHED pushing Nones");
    for i in 0..q_stops {
        q.push(None);
    }
    for h in handles {
        h.join();
        println!("worker joined");
    }
}