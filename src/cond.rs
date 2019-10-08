
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::collections::LinkedList;


struct InnerQ<T> {
    queue: LinkedList<T>,
    curr_waiters: usize,
    max_waiters: usize,
    limit: usize,
}

#[derive(Clone)]
struct Requeue<T> {
    tqueue: Arc<Mutex<InnerQ<T>>>,
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
        }
    }
    pub fn push(&mut self, item: T) {
        let mut l = self.tqueue.lock().unwrap();
        l.queue.push_front(item);
    }
    pub fn pop(&mut self) -> Option<T> {
        let mut l = self.tqueue.lock().unwrap();
        l.queue.pop_back()
    }
}

fn main() {

    let mut q:Requeue<String> = Requeue::new(4, 10);
    q.push("dog".to_string());
    let mut q_c = q.clone();
    let h = thread::spawn(move|| {
        println!("other thread: {}", q_c.pop().unwrap());
    });
    h.join();

}