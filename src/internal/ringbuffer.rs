use std::fmt;
use std::io::{self, Read, Write};
use std::sync::{Condvar, Mutex, MutexGuard, TryLockError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    TooMuchDataToWrite,
    IsFull,
    IsEmpty,
    IsNotEmpty,
    AcquireLock,
    WriteOnClosed,
    Closed(String),
    Eof,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooMuchDataToWrite => f.write_str("too much data to write"),
            Self::IsFull => f.write_str("ringbuffer is full"),
            Self::IsEmpty => f.write_str("ringbuffer is empty"),
            Self::IsNotEmpty => f.write_str("ringbuffer is not empty"),
            Self::AcquireLock => f.write_str("unable to acquire lock"),
            Self::WriteOnClosed => f.write_str("write on closed ringbuffer"),
            Self::Closed(message) => f.write_str(message),
            Self::Eof => f.write_str("EOF"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
struct State {
    buf: Vec<u8>,
    size: usize,
    r: usize,
    w: usize,
    is_full: bool,
    err: Option<Error>,
    blocking: bool,
}

impl State {
    fn new(size: usize) -> Self {
        Self {
            buf: vec![0; size],
            size,
            r: 0,
            w: 0,
            is_full: false,
            err: None,
            blocking: false,
        }
    }

    fn len(&self) -> usize {
        if self.w == self.r {
            if self.is_full {
                self.size
            } else {
                0
            }
        } else if self.w > self.r {
            self.w - self.r
        } else {
            self.size - self.r + self.w
        }
    }

    fn free(&self) -> usize {
        self.size - self.len()
    }

    fn is_empty(&self) -> bool {
        !self.is_full && self.w == self.r
    }

    fn available_to_write(&self) -> usize {
        self.free()
    }

    fn current_read_error(&self) -> Option<Error> {
        match &self.err {
            Some(Error::Eof) if self.is_empty() => Some(Error::Eof),
            Some(Error::Closed(message)) => Some(Error::Closed(message.clone())),
            _ => None,
        }
    }

    fn current_write_error(&self) -> Option<Error> {
        match &self.err {
            Some(Error::Eof) => Some(Error::WriteOnClosed),
            Some(Error::Closed(message)) => Some(Error::Closed(message.clone())),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct RingBuffer {
    state: Mutex<State>,
    read_cond: Condvar,
    write_cond: Condvar,
}

impl RingBuffer {
    pub fn new(size: usize) -> Self {
        Self {
            state: Mutex::new(State::new(size)),
            read_cond: Condvar::new(),
            write_cond: Condvar::new(),
        }
    }

    pub fn set_blocking(self, blocking: bool) -> Self {
        {
            let mut state = self.state.lock().expect("ringbuffer mutex poisoned");
            state.blocking = blocking;
        }
        self
    }

    pub fn read(&self, dest: &mut [u8]) -> Result<usize, Error> {
        if dest.is_empty() {
            let state = self.state.lock().expect("ringbuffer mutex poisoned");
            return match state.current_read_error() {
                Some(error) => Err(error),
                None => Ok(0),
            };
        }

        let mut state = self.state.lock().expect("ringbuffer mutex poisoned");
        loop {
            if state.len() > 0 {
                let n = self.read_from_state(&mut state, dest);
                self.read_cond.notify_all();
                return Ok(n);
            }
            if let Some(error) = state.current_read_error() {
                return Err(error);
            }
            if !state.blocking {
                return Err(Error::IsEmpty);
            }
            state = self
                .write_cond
                .wait(state)
                .expect("ringbuffer mutex poisoned");
        }
    }

    pub fn try_read(&self, dest: &mut [u8]) -> Result<usize, Error> {
        let mut state = self.try_lock_state()?;
        if dest.is_empty() {
            return match state.current_read_error() {
                Some(error) => Err(error),
                None => Ok(0),
            };
        }
        if state.len() == 0 {
            if let Some(error) = state.current_read_error() {
                return Err(error);
            }
            return Err(Error::IsEmpty);
        }
        let n = self.read_from_state(&mut state, dest);
        self.read_cond.notify_all();
        Ok(n)
    }

    pub fn read_byte(&self) -> Result<u8, Error> {
        let mut buf = [0u8; 1];
        let n = self.read(&mut buf)?;
        if n == 0 {
            return Err(Error::IsEmpty);
        }
        Ok(buf[0])
    }

    pub fn write(&self, src: &[u8]) -> Result<usize, Error> {
        let mut state = self.state.lock().expect("ringbuffer mutex poisoned");
        if src.is_empty() {
            return match state.current_write_error() {
                Some(error) => Err(error),
                None => Ok(0),
            };
        }
        if let Some(error) = state.current_write_error() {
            return Err(error);
        }

        let mut written = 0usize;
        let mut remaining = src;

        loop {
            if remaining.is_empty() {
                if written > 0 {
                    self.write_cond.notify_all();
                }
                return Ok(written);
            }

            let available = state.available_to_write();
            if available == 0 {
                if !state.blocking {
                    return if written == 0 {
                        Err(Error::IsFull)
                    } else {
                        Err(Error::TooMuchDataToWrite)
                    };
                }
                state = self
                    .read_cond
                    .wait(state)
                    .expect("ringbuffer mutex poisoned");
                if let Some(error) = state.current_write_error() {
                    return if written > 0 { Ok(written) } else { Err(error) };
                }
                continue;
            }

            let to_write = if state.blocking {
                available.min(remaining.len())
            } else if remaining.len() > available {
                available
            } else {
                remaining.len()
            };

            self.write_to_state(&mut state, &remaining[..to_write]);
            written += to_write;
            remaining = &remaining[to_write..];
            self.write_cond.notify_all();

            if !state.blocking && !remaining.is_empty() {
                return Err(Error::TooMuchDataToWrite);
            }
        }
    }

    pub fn try_write(&self, src: &[u8]) -> Result<usize, Error> {
        let mut state = self.try_lock_state()?;
        if src.is_empty() {
            return match state.current_write_error() {
                Some(error) => Err(error),
                None => Ok(0),
            };
        }
        if let Some(error) = state.current_write_error() {
            return Err(error);
        }
        let available = state.available_to_write();
        if available == 0 {
            return Err(Error::IsFull);
        }
        let to_write = available.min(src.len());
        self.write_to_state(&mut state, &src[..to_write]);
        self.write_cond.notify_all();
        if to_write < src.len() {
            Err(Error::TooMuchDataToWrite)
        } else {
            Ok(to_write)
        }
    }

    pub fn write_byte(&self, byte: u8) -> Result<(), Error> {
        match self.write(&[byte]) {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn try_write_byte(&self, byte: u8) -> Result<(), Error> {
        match self.try_write(&[byte]) {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn write_string(&self, value: &str) -> Result<usize, Error> {
        self.write(value.as_bytes())
    }

    pub fn length(&self) -> usize {
        self.state.lock().expect("ringbuffer mutex poisoned").len()
    }

    pub fn capacity(&self) -> usize {
        self.state.lock().expect("ringbuffer mutex poisoned").size
    }

    pub fn free(&self) -> usize {
        self.state.lock().expect("ringbuffer mutex poisoned").free()
    }

    pub fn is_full(&self) -> bool {
        self.state
            .lock()
            .expect("ringbuffer mutex poisoned")
            .is_full
    }

    pub fn is_empty(&self) -> bool {
        self.state
            .lock()
            .expect("ringbuffer mutex poisoned")
            .is_empty()
    }

    pub fn bytes(&self) -> Vec<u8> {
        let state = self.state.lock().expect("ringbuffer mutex poisoned");
        let len = state.len();
        if len == 0 {
            return Vec::new();
        }
        let mut out = vec![0u8; len];
        if state.w > state.r || state.is_full && state.r == 0 {
            out.copy_from_slice(&state.buf[state.r..state.r + len]);
            return out;
        }
        let first = state.size - state.r;
        out[..first].copy_from_slice(&state.buf[state.r..]);
        out[first..].copy_from_slice(&state.buf[..state.w]);
        out
    }

    pub fn close_with_error(&self, error: Option<Error>) {
        let mut state = self.state.lock().expect("ringbuffer mutex poisoned");
        if matches!(state.err, Some(Error::Closed(_)) | Some(Error::Eof)) {
            return;
        }
        state.err = Some(error.unwrap_or(Error::Eof));
        self.read_cond.notify_all();
        self.write_cond.notify_all();
    }

    pub fn close_writer(&self) {
        self.close_with_error(Some(Error::Eof));
    }

    pub fn flush(&self) -> Result<(), Error> {
        let mut state = self.state.lock().expect("ringbuffer mutex poisoned");
        loop {
            if state.is_empty() {
                return match state.current_read_error() {
                    Some(Error::Eof) | None => Ok(()),
                    Some(error) => Err(error),
                };
            }
            if let Some(error) = state.current_read_error() {
                return if error == Error::Eof {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            if !state.blocking {
                return Err(Error::IsNotEmpty);
            }
            state = self
                .read_cond
                .wait(state)
                .expect("ringbuffer mutex poisoned");
        }
    }

    pub fn reset(&self) {
        let mut state = self.state.lock().expect("ringbuffer mutex poisoned");
        state.r = 0;
        state.w = 0;
        state.is_full = false;
        state.err = None;
        self.read_cond.notify_all();
        self.write_cond.notify_all();
    }

    fn try_lock_state(&self) -> Result<MutexGuard<'_, State>, Error> {
        match self.state.try_lock() {
            Ok(guard) => Ok(guard),
            Err(TryLockError::WouldBlock) => Err(Error::AcquireLock),
            Err(TryLockError::Poisoned(_)) => {
                Err(Error::Closed("ringbuffer mutex poisoned".to_owned()))
            }
        }
    }

    fn write_to_state(&self, state: &mut State, src: &[u8]) {
        let first = (state.size - state.w).min(src.len());
        state.buf[state.w..state.w + first].copy_from_slice(&src[..first]);
        state.w = (state.w + first) % state.size;
        if first < src.len() {
            let second = src.len() - first;
            state.buf[..second].copy_from_slice(&src[first..]);
            state.w = second;
        }
        if state.w == state.r {
            state.is_full = true;
        }
    }

    fn read_from_state(&self, state: &mut State, dest: &mut [u8]) -> usize {
        let available = state.len();
        let n = available.min(dest.len());
        let first = (state.size - state.r).min(n);
        dest[..first].copy_from_slice(&state.buf[state.r..state.r + first]);
        state.r = (state.r + first) % state.size;
        if first < n {
            let second = n - first;
            dest[first..n].copy_from_slice(&state.buf[..second]);
            state.r = second;
        }
        state.is_full = false;
        n
    }
}

impl Read for RingBuffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        RingBuffer::read(self, buf).map_err(|error| io::Error::new(io::ErrorKind::Other, error))
    }
}

impl Write for RingBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        RingBuffer::write(self, buf).map_err(|error| io::Error::new(io::ErrorKind::Other, error))
    }

    fn flush(&mut self) -> io::Result<()> {
        RingBuffer::flush(self).map_err(|error| io::Error::new(io::ErrorKind::Other, error))
    }
}
