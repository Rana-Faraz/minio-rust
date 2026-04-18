use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

pub const UPDATE_INTERVAL: Duration = Duration::from_millis(250);

pub struct DeadlineConn {
    conn: TcpStream,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    read_deadline: Option<Duration>,
    read_set_at: Option<Instant>,
    write_deadline: Option<Duration>,
    write_set_at: Option<Instant>,
    abort_reads: bool,
    abort_writes: bool,
    inf_reads: bool,
    inf_writes: bool,
}

impl DeadlineConn {
    pub fn new(conn: TcpStream) -> Self {
        Self {
            conn,
            state: Mutex::new(State::default()),
        }
    }

    pub fn with_read_deadline(self, deadline: Duration) -> Self {
        self.state
            .lock()
            .expect("deadline state should lock")
            .read_deadline = Some(deadline);
        self
    }

    pub fn with_write_deadline(self, deadline: Duration) -> Self {
        self.state
            .lock()
            .expect("deadline state should lock")
            .write_deadline = Some(deadline);
        self
    }

    pub fn set_deadline(&self, time: Option<SystemTime>) -> io::Result<()> {
        let mut state = self.state.lock().expect("deadline state should lock");
        state.read_set_at = None;
        state.write_set_at = None;
        let abort = matches!(time, Some(moment) if moment < SystemTime::now());
        let infinite = time.is_none();
        state.abort_reads = abort;
        state.abort_writes = abort;
        state.inf_reads = infinite;
        state.inf_writes = infinite;
        self.conn.set_read_timeout(timeout_from_system_time(time))?;
        self.conn
            .set_write_timeout(timeout_from_system_time(time))?;
        Ok(())
    }

    pub fn set_read_deadline(&self, time: Option<SystemTime>) -> io::Result<()> {
        let mut state = self.state.lock().expect("deadline state should lock");
        state.abort_reads = matches!(time, Some(moment) if moment < SystemTime::now());
        state.inf_reads = time.is_none();
        state.read_set_at = None;
        self.conn.set_read_timeout(timeout_from_system_time(time))
    }

    pub fn set_write_deadline(&self, time: Option<SystemTime>) -> io::Result<()> {
        let mut state = self.state.lock().expect("deadline state should lock");
        state.abort_writes = matches!(time, Some(moment) if moment < SystemTime::now());
        state.inf_writes = time.is_none();
        state.write_set_at = None;
        self.conn.set_write_timeout(timeout_from_system_time(time))
    }

    pub fn close(&self) -> io::Result<()> {
        let mut state = self.state.lock().expect("deadline state should lock");
        state.abort_reads = true;
        state.abort_writes = true;
        self.conn.shutdown(std::net::Shutdown::Both)
    }

    fn refresh_read_deadline(&self) -> io::Result<()> {
        let mut state = self.state.lock().expect("deadline state should lock");
        if state.abort_reads || state.inf_reads {
            return Ok(());
        }
        let Some(deadline) = state.read_deadline else {
            return Ok(());
        };
        let now = Instant::now();
        if state
            .read_set_at
            .map(|last| now.duration_since(last) > UPDATE_INTERVAL)
            .unwrap_or(true)
        {
            self.conn
                .set_read_timeout(Some(deadline + UPDATE_INTERVAL))?;
            state.read_set_at = Some(now);
        }
        Ok(())
    }

    fn refresh_write_deadline(&self) -> io::Result<()> {
        let mut state = self.state.lock().expect("deadline state should lock");
        if state.abort_writes || state.inf_writes {
            return Ok(());
        }
        let Some(deadline) = state.write_deadline else {
            return Ok(());
        };
        let now = Instant::now();
        if state
            .write_set_at
            .map(|last| now.duration_since(last) > UPDATE_INTERVAL)
            .unwrap_or(true)
        {
            self.conn
                .set_write_timeout(Some(deadline + UPDATE_INTERVAL))?;
            state.write_set_at = Some(now);
        }
        Ok(())
    }
}

impl Read for DeadlineConn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self
            .state
            .lock()
            .expect("deadline state should lock")
            .abort_reads
        {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
        }
        self.refresh_read_deadline()?;
        self.conn.read(buf)
    }
}

impl Write for DeadlineConn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self
            .state
            .lock()
            .expect("deadline state should lock")
            .abort_writes
        {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
        }
        self.refresh_write_deadline()?;
        self.conn.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.conn.flush()
    }
}

fn timeout_from_system_time(time: Option<SystemTime>) -> Option<Duration> {
    match time {
        None => None,
        Some(moment) => moment.duration_since(SystemTime::now()).ok(),
    }
}
