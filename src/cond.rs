
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
}


impl<T> Requeue<T> {
    pub fn new(max_waits: usize, limit_: usize) -> Requeue<T> {
        Requeue{
            tqueue: Arc::new(Mutex::new(
                InnerQ {
                    queue: LinkedList::new(),
                    curr_waiters: 0,
                    max_waiters: max_waits,
                    limit: limit_,
            })),
            cond_more: Arc::new(Condvar::new()),
            cond_hasroom: Arc::new(Condvar::new()),
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
            l = self.cond_more.wait(l).unwrap();
        }
        let res =l.queue.pop_back().unwrap();
        if l.limit > 0 {
            self.cond_hasroom.notify_one();
            l.curr_waiters -= 1;
        }
        res
    }
    pub fn waiters(& self,) -> usize {
        let mut l = self.tqueue.lock().unwrap();
        l.curr_waiters
    }
}

fn main() {

    let mut q:Requeue<Option<String>> = Requeue::new(4, 10);
    q.push(Some("dog".to_string()));
    let mut q_c = q.clone();
    let h = thread::spawn(move|| {
        loop {
            match q_c.pop() {
                None => break,
                Some(s) => {}, //println!("other thread {}", s),
            }
        }
    });
    for i in 0 .. 10_000_000 {
        let s = i.to_string();
        //println!("waiters {}", q.waiters());
        q.push(Some(s));
        //thread::sleep(Duration::from_millis(1000));

    }
    println!("sending None and DONE");
    q.push(None);

    h.join();

}