use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::collections::LinkedList;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};

struct InnerQ<T> {
    queue: LinkedList<T>,
    curr_poppers: usize,
    curr_pushers: usize,
    max_waiters: usize,
    dead: usize,
    max_q_len_reached: usize,
    limit: usize,
}

#[derive(Clone)]
pub struct WorkerQueue<T> {
    tqueue: Arc<Mutex<InnerQ<T>>>,
    cond_more: Arc<Condvar>,
    cond_hasroom: Arc<Condvar>,
    looks_done: Arc<Condvar>,
}

#[derive(Debug)]
pub struct QueueStats {
    pub curr_poppers: usize,
    pub curr_pushers: usize,
    pub curr_q_len: usize,
}

impl<T> WorkerQueue<T> {
    pub fn new(max_waits: usize, limit_: usize) -> WorkerQueue<T> {
        WorkerQueue {
            tqueue: Arc::new(Mutex::new(
                InnerQ {
                    queue: LinkedList::new(),
                    curr_poppers: 0,
                    curr_pushers: 0,
                    dead: 0,
                    max_waiters: max_waits,
                    max_q_len_reached: 0,
                    limit: limit_,
                })),
            cond_more: Arc::new(Condvar::new()),
            cond_hasroom: Arc::new(Condvar::new()),
            looks_done: Arc::new(Condvar::new()),
        }
    }
    pub fn push(&mut self, item: T) -> Result<()> {
        let mut l = self.tqueue.lock().unwrap();
        l.curr_pushers += 1;
        if l.queue.len() > l.max_q_len_reached {
            l.max_q_len_reached = l.queue.len();
        }
        if l.limit == 0 {
            l.queue.push_front(item);
        } else {
            if l.limit <= l.queue.len() && l.curr_pushers >= l.max_waiters {
                l.dead += 1;
                self.looks_done.notify_all();
                Err(anyhow!("Queue overflow reached - cannot push another, len at {} and this is the {}(th) pusher", l.queue.len(), l.curr_pushers))?;
            }
            while l.limit > 0 && l.queue.len() >= l.limit {
                l = self.cond_hasroom.wait(l).unwrap();
            }
            l.queue.push_front(item);
        }
        l.curr_pushers -= 1;
        self.cond_more.notify_one();
        Ok(())
    }
    pub fn pop(&mut self) -> T {
        let mut l = self.tqueue.lock().unwrap();
        l.curr_poppers += 1;
        while l.queue.len() < 1 {
            if l.curr_poppers == l.max_waiters {
                self.looks_done.notify_one();
            }
            l = self.cond_more.wait(l).unwrap();
        }
        let res = l.queue.pop_back().unwrap();
        self.cond_hasroom.notify_one();
        l.curr_poppers -= 1;
        res
    }
    pub fn waiters(&self) -> usize {
        let mut l = self.tqueue.lock().unwrap();
        l.curr_poppers
    }

    pub fn wait_for_finish_timeout(&self, dur: Duration) -> Result<i64> {
        let mut ret = 0i64;
        {
            let mut l = self.tqueue.lock().unwrap();
            // sanity check because we have more new work than the queue can hold
            while !(l.queue.len() <= 0 && l.curr_poppers == l.max_waiters) {
                let x = self.looks_done.wait_timeout(l, dur).unwrap();
                l = x.0;
                if x.1.timed_out() {
                    return Ok(-1);
                }
                if l.limit != 0 && l.curr_pushers >= l.max_waiters && l.queue.len() >= (l.limit) {
                    Err(anyhow!("Queue looks stuck at limit {} and pushers {}", &l.queue.len(), &l.curr_pushers))?;
                }
                if l.dead > 0 {
                    Err(anyhow!("Thread death detected - likely due to overflow, #dead: {}", l.dead))?;
                }
            }
            ret = l.curr_poppers as i64;
        }

        Ok(ret)
    }

    pub fn wait_for_finish(&self) -> Result<usize> {
        let mut l = self.tqueue.lock().unwrap();
        // sanity check because we have more new work than the queue can hold
        if l.limit > 0 && l.curr_pushers >= l.max_waiters && l.queue.len() >= l.limit {
            Err(anyhow!("Queue looks stuck at limit {} and waiters {}", &l.queue.len(), &l.curr_poppers))?;
        }
        while !(l.queue.len() <= 0 && l.curr_poppers >= l.max_waiters) {
            l = self.looks_done.wait(l).unwrap();
        }
        Ok(l.curr_poppers)
    }

    pub fn notify_all(&self) {
        self.cond_hasroom.notify_all();
        self.cond_more.notify_all();
    }

    pub fn status(&self) {
        let mut l = self.tqueue.lock().unwrap();
        eprintln!("q len: {}  threads:  {}  dead:  {}  poppers: {}  pushers: {}", l.queue.len(), l.max_waiters, l.dead, l.curr_poppers, l.curr_pushers);
    }
    pub fn print_max_queue(&self) {
        let mut l = self.tqueue.lock().unwrap();
        eprintln!("max q reached: {}", l.max_q_len_reached);
    }

    pub fn get_stats(&self) -> QueueStats {
        let mut l = self.tqueue.lock().unwrap();
        QueueStats {
            curr_pushers: l.curr_pushers,
            curr_poppers: l.curr_poppers,
            curr_q_len: l.queue.len()
        }
    }
}
