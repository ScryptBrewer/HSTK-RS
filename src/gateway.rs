// src/gateway.rs
// Shadow command gateway system for Hammerspace CLI
// DIAGNOSTIC BUILD - uses raw POSIX syscalls via nix, with heavy debug to stderr

use anyhow::{Context, Result};
use nix::fcntl::{open, OFlag};
use nix::libc;
use nix::sys::stat::Mode;
use nix::unistd::{close, fsync, lseek, read, unlink, write, Whence};
use rand::Rng;
use std::io::Write as IoWrite;
use std::os::fd::BorrowedFd;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};

const WIN_PADDING: &[u8] = &[0u8; 50];
const READ_BUF_SIZE: usize = 65536;

pub fn is_windows() -> bool {
    cfg!(windows)
}

pub fn generate_work_id() -> String {
    let mut rng = rand::thread_rng();
    format!("{:08x}", rng.gen_range(0..99999999))
}

pub fn gateway_path(fname: &Path, work_id: &str) -> Result<PathBuf> {
    let gw = if fname.is_dir() {
        fname.to_path_buf()
    } else {
        fname
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Cannot get parent of {:?}", fname))?
            .to_path_buf()
    };
    Ok(gw.join(format!(".fs_command_gateway {}", work_id)))
}

fn hex_dump(bytes: &[u8]) -> String {
    let take = bytes.len().min(200);
    let hex: Vec<String> = bytes[..take].iter().map(|b| format!("{:02x}", b)).collect();
    if bytes.len() > take {
        format!("{} ... ({} more bytes)", hex.join(" "), bytes.len() - take)
    } else {
        hex.join(" ")
    }
}

macro_rules! dlog {
    ($($arg:tt)*) => {{
        eprintln!("D: {}", format!($($arg)*));
        let _ = std::io::stderr().flush();
    }};
}

#[derive(Debug, Clone)]
pub struct GatewayExecutor {
    pub dry_run: bool,
    pub verbose: bool,
    pub debug: bool,
}

impl GatewayExecutor {
    pub fn new(dry_run: bool, verbose: bool, debug: bool) -> Self {
        Self {
            dry_run,
            verbose,
            debug,
        }
    }

    pub fn execute(&self, fname: &Path, command: &str) -> Result<Vec<String>> {
        dlog!("============================================================");
        dlog!("GatewayExecutor::execute() called");
        dlog!("============================================================");

        let work_id = generate_work_id();
        let gw_path = gateway_path(fname, &work_id)?;

        dlog!("Target fname:           {:?}", fname);
        dlog!("Target exists:          {}", fname.exists());
        dlog!("Target is_dir:          {}", fname.is_dir());
        dlog!("Target is_file:         {}", fname.is_file());
        match fname.canonicalize() {
            Ok(p) => dlog!("Target canonical:       {:?}", p),
            Err(e) => dlog!("Target canonical:       <error: {}>", e),
        }
        dlog!("Generated work_id:      {}", work_id);
        dlog!("Gateway path:           {:?}", gw_path);
        dlog!("command argument (raw): {:?}", command);

        let dir = if fname.is_dir() {
            fname.to_path_buf()
        } else {
            fname.parent().unwrap_or(Path::new(".")).to_path_buf()
        };
        dlog!("--- Pre-existing gateway files in {:?} ---", dir);
        let mut stale = 0usize;
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let s = entry.file_name().to_string_lossy().to_string();
                if s.starts_with(".fs_command_gateway") {
                    dlog!("  stale: {:?}", s);
                    stale += 1;
                }
            }
        }
        if stale == 0 {
            dlog!("  (none)");
        } else {
            dlog!(
                "  ^^^ {} stale gateway file(s) - earlier runs leaked them ^^^",
                stale
            );
        }

        if self.dry_run {
            self.vnprint("  [DRY RUN] gateway operation skipped");
            return Ok(vec!["dry run output".to_string()]);
        }

        // Build command bytes
        let mut cmd_bytes = Vec::new();
        if fname.is_dir() {
            cmd_bytes.extend_from_slice(b"./");
            dlog!("Path prefix:            ./   (directory)");
        } else {
            cmd_bytes.extend_from_slice(b"./");
            let name = fname
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("Cannot get filename from {:?}", fname))?;
            cmd_bytes.extend_from_slice(name.to_string_lossy().as_bytes());
            dlog!(
                "Path prefix:            ./{}  (file)",
                name.to_string_lossy()
            );
        }
        cmd_bytes.extend_from_slice(command.as_bytes());

        if is_windows() {
            cmd_bytes.extend_from_slice(WIN_PADDING);
            dlog!("Windows padding:        +{} bytes", WIN_PADDING.len());
        }

        dlog!("--- cmd_bytes to write ({} bytes) ---", cmd_bytes.len());
        dlog!("ASCII: {:?}", String::from_utf8_lossy(&cmd_bytes));
        dlog!("HEX:   {}", hex_dump(&cmd_bytes));

        // ---------- open(O_RDWR | O_CREAT, 0644) ----------
        let oflags = OFlag::O_RDWR | OFlag::O_CREAT;
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        dlog!(
            "syscall: open({:?}, O_RDWR|O_CREAT (0x{:x}), 0644)",
            gw_path,
            oflags.bits()
        );
        let fd: RawFd = open(&gw_path, oflags, mode)
            .with_context(|| format!("open() failed for gateway file {:?}", gw_path))?;
        dlog!("                        -> fd = {}", fd);

        // SAFETY: we own this fd until the close() call at the bottom of this
        // function. BorrowedFd is only used to satisfy nix 0.29's AsFd bound on write().
        let bfd = unsafe { BorrowedFd::borrow_raw(fd) };

        let inner: Result<Vec<u8>> = (|| {
            // ---------- write() (needs AsFd in nix 0.29) ----------
            dlog!("syscall: write(fd={}, len={})", fd, cmd_bytes.len());
            let written = write(bfd, &cmd_bytes).with_context(|| "write() failed")?;
            dlog!("                        -> {} bytes written", written);
            if written != cmd_bytes.len() {
                dlog!(
                    "  WARNING: short write! expected {}, got {}",
                    cmd_bytes.len(),
                    written
                );
            }

            // ---------- fsync() (takes RawFd in nix 0.29) ----------
            // CRITICAL: forces the buffered write to actually go to the NFS
            // server, where the Hammerspace driver lives and processes commands.
            dlog!("syscall: fsync(fd={})", fd);
            fsync(fd).with_context(|| "fsync() failed")?;
            dlog!("                        -> fsync ok (write committed to server)");

            // ---------- posix_fadvise(POSIX_FADV_DONTNEED) ----------
            // CRITICAL: drops the local page cache for this fd so the next
            // read() actually goes to the server, where the driver has now
            // staged the response (instead of being served from local cache
            // which still holds our written bytes).
            dlog!("syscall: posix_fadvise(fd={}, POSIX_FADV_DONTNEED)", fd);
            let r = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
            if r != 0 {
                dlog!(
                    "                        -> posix_fadvise rc={} ({}) - continuing",
                    r,
                    std::io::Error::from_raw_os_error(r)
                );
            } else {
                dlog!("                        -> page cache dropped");
            }

            // ---------- lseek(0) ----------
            dlog!("syscall: lseek(fd={}, 0, SEEK_SET)", fd);
            let pos = lseek(fd, 0, Whence::SeekSet).with_context(|| "lseek() failed")?;
            dlog!("                        -> position = {}", pos);

            // ---------- read() ----------
            let mut buf = vec![0u8; READ_BUF_SIZE];
            dlog!("syscall: read(fd={}, max={})", fd, READ_BUF_SIZE);
            let n = read(fd, &mut buf).with_context(|| "read() failed")?;
            dlog!("                        -> {} bytes read", n);
            buf.truncate(n);

            dlog!("--- response ({} bytes) ---", buf.len());
            dlog!("ASCII: {:?}", String::from_utf8_lossy(&buf));
            dlog!("HEX:   {}", hex_dump(&buf));

            dlog!("--- DIAGNOSTIC: write/read comparison ---");
            if buf == cmd_bytes {
                dlog!("!!! READ BUFFER STILL BYTE-IDENTICAL TO WRITTEN BYTES !!!");
                dlog!("!!! Even with fsync + posix_fadvise(POSIX_FADV_DONTNEED).");
                dlog!("!!! NEXT STEP - capture working hs syscalls for comparison:");
                dlog!("!!!");
                dlog!("!!!   strace -f -e trace=openat,read,write,lseek,close,fsync,fcntl,fadvise64 \\");
                dlog!("!!!     -o /tmp/hs.strace -s 256 \\");
                dlog!("!!!     hs eval -r -e 'IS_FILE&&ACCESS_AGE>=2DAYS?NAME' >/dev/null 2>&1");
                dlog!("!!!   grep -E 'fs_command_gateway|eval_rec' /tmp/hs.strace");
                dlog!("!!!");
                dlog!("!!! Send me the grep output and I'll diff it against ours.");
            } else if buf.is_empty() {
                dlog!("!!! READ BUFFER IS EMPTY (response not staged yet?)");
            } else {
                dlog!("OK: read differs from write -- Hammerspace responded!");
            }

            Ok(buf)
        })();

        // ---------- close() ----------
        dlog!("syscall: close(fd={})", fd);
        match close(fd) {
            Ok(_) => dlog!("                        -> closed ok"),
            Err(e) => dlog!("                        -> close error: {}", e),
        }

        // ---------- unlink (cleanup leaked gateway files) ----------
        if gw_path.exists() {
            dlog!("syscall: unlink({:?}) (cleanup)", gw_path);
            match unlink(&gw_path) {
                Ok(_) => dlog!("                        -> unlinked"),
                Err(e) => dlog!("                        -> unlink failed: {}", e),
            }
        } else {
            dlog!("Gateway file already gone after close (driver cleaned it up)");
        }

        let buf = inner?;
        let buffer_str = String::from_utf8_lossy(&buf).to_string();
        let lines: Vec<String> = buffer_str.lines().map(|s| s.to_string()).collect();
        dlog!("Returning {} lines to caller", lines.len());
        dlog!("============================================================");

        Ok(lines)
    }

    fn vnprint(&self, line: &str) {
        if self.verbose || self.dry_run {
            let tag = if self.dry_run { "N: " } else { "V: " };
            println!("{}{}", tag, line);
        }
    }

    #[allow(dead_code)]
    fn dprint(&self, line: &str) {
        if self.debug {
            println!("D: {}", line);
        }
    }
}

#[allow(dead_code)]
pub fn execute_on_paths(
    paths: &[PathBuf],
    command_generator: impl Fn(&Path) -> Result<String>,
    executor: &GatewayExecutor,
) -> Result<Vec<(PathBuf, Vec<String>)>> {
    let mut results = Vec::new();
    for path in paths {
        let command = command_generator(path)?;
        let output = executor.execute(path, &command)?;
        results.push((path.clone(), output));
    }
    Ok(results)
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MockGatewayExecutor {
    pub dry_run: bool,
    pub verbose: bool,
    pub debug: bool,
    pub mock_output: Vec<String>,
}

#[allow(dead_code)]
impl MockGatewayExecutor {
    pub fn new(dry_run: bool, verbose: bool, debug: bool) -> Self {
        Self {
            dry_run,
            verbose,
            debug,
            mock_output: vec![
                "mock result line 1".to_string(),
                "mock result line 2".to_string(),
            ],
        }
    }

    pub fn execute(&self, _fname: &Path, _command: &str) -> Result<Vec<String>> {
        if self.dry_run {
            return Ok(vec!["dry run output".to_string()]);
        }
        Ok(self.mock_output.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_work_id() {
        let id1 = generate_work_id();
        let id2 = generate_work_id();
        assert_ne!(id1, id2);
        assert_eq!(id1.len(), 8);
    }

    #[test]
    fn test_gateway_path_file() {
        let fname = PathBuf::from("/tmp/test.txt");
        let gw = gateway_path(&fname, "12345678").unwrap();
        assert_eq!(gw, PathBuf::from("/tmp/.fs_command_gateway 12345678"));
    }

    #[test]
    fn test_gateway_path_dir() {
        let fname = PathBuf::from("/tmp/testdir");
        let gw = gateway_path(&fname, "12345678").unwrap();
        assert_eq!(gw, PathBuf::from("/tmp/.fs_command_gateway 12345678"));
    }

    #[test]
    fn test_gateway_executor_dry_run() {
        let executor = GatewayExecutor::new(true, false, false);
        let results = executor
            .execute(Path::new("/tmp/test"), "test command")
            .unwrap();
        assert_eq!(results, vec!["dry run output".to_string()]);
    }
}