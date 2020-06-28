use std::sync::{Arc, Mutex};
use num_cpus::get;
use std::thread;
use std::time::Duration;
use std::path::Iter;

#[cfg(target_os = "windows")]
pub fn gettid() -> usize {
    unsafe { winapi::um::processthreadsapi::GetCurrentThreadId() as usize }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn gettid() -> usize {
    unsafe { libc::syscall(libc::SYS_gettid) as usize }
}

struct ThreadStatusInner {
    name: String,
    state: String,
    tid: usize,
}

#[derive(Clone)]
pub struct ThreadStatus {
    status: Arc<Mutex<ThreadStatusInner>>,
}

impl ThreadStatus {
    fn new(state: &str, name: &str) -> ThreadStatus {
        ThreadStatus {
            status: Arc::new(Mutex::new(
                ThreadStatusInner {
                    name: name.to_string(),
                    state: state.into(),
                    tid: 0,
                }))
        }
    }
    pub fn set_state(&mut self, s: &str) {
        self.status.lock().unwrap().state = s.into();
    }
    pub fn register(&mut self, state: &str) {
        let mut l = self.status.lock().unwrap();
        l.state = state.into();
        l.tid = gettid();
    }
}

pub struct ThreadTracker {
    list: Vec<ThreadStatus>,
}

impl ThreadTracker {
    pub fn new() -> ThreadTracker {
        ThreadTracker {
            list: Vec::new(),
        }
    }
    pub fn setup_thread(&mut self, name: &str, state: &str) -> ThreadStatus {
        let ts = ThreadStatus::new(state, name);
        let cl = ts.clone();
        self.list.push(ts);
        cl
    }
    pub fn eprint_status(&self) {
        for ts in self.list.iter().enumerate() {
            let g_ts = ts.1.status.lock().unwrap();
            eprintln!("index: {:2} {}  tid:  {:6}  status: \"{}\"", ts.0, &g_ts.name, &g_ts.tid, &g_ts.state);
        }
    }

    // once you start monitor you can no longer add/change it
    pub fn monitor(&self, interval_ms: u64) {
        loop {
            self.eprint_status();
            thread::sleep(Duration::from_millis(interval_ms));
        }
    }

    pub fn monitor_on_enter(&self) {
        loop {
            let mut buff = String::new();
            std::io::stdin().read_line(&mut buff);
            self.eprint_status();
        }
    }

    pub fn iter(&self) -> std::slice::Iter<ThreadStatus> { self.list.iter() }
}

/*
pub trait SafeThreadStatus: Clone {
    fn set_state(&mut self, s: &str);
    fn register(&mut self, state: &str);
}

impl SafeThreadStatus for Arc<Mutex<ThreadStatus>> {
    fn set_state(&mut self, s: &str) {
        self.lock().unwrap().state = s.into();
    }
    fn register(&mut self, state: &str) {
        let mut l = self.lock().unwrap();
        l.state = state.into();
        l.tid = gettid();
    }
}


struct Athing(Arc<Mutex<String>>);

impl Athing {
    fn new(state: &str) -> Athing {
        Athing(Arc::new(Mutex::new(state.into())))
    }
    fn there() {}
}
*/