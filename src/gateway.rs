// src/gateway.rs
// Shadow command gateway system for Hammerspace CLI
//
// Protocol (verified via strace of the official `hs` binary on a Hammerspace
// NFS4.2 mount, 2026-05-01):
//
//   1. openat(".fs_command_gateway 0xXXXXXXX",
//             O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC, 0666)   -> wfd
//   2. write(wfd, "./PREFIX COMMAND")
//   3. close(wfd)                  <-- forces NFS commit (close-to-open)
//   4. openat(same path, O_RDONLY|O_CLOEXEC)              -> rfd
//   5. read(rfd, response)         <-- fresh fd = no stale page cache
//   6. close(rfd)
//
// The two-open / close-between dance is ESSENTIAL. With a single O_RDWR fd
// the write data stays in the local NFS page cache and the read is served
// from cache - the Hammerspace driver on the SERVER never sees the request.
// Adding fsync()/posix_fadvise(POSIX_FADV_DONTNEED) is NOT sufficient; only
// close()-then-open() triggers proper NFS close-to-open cache semantics.

use anyhow::{Context, Result};
use nix::fcntl::{open, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{close, read, unlink, write};
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

/// Generate a work ID matching `hs`'s `0x` + 7-hex-digits format.
pub fn generate_work_id() -> String {
    let mut rng = rand::thread_rng();
    format!("0x{:07x}", rng.gen_range(0..0x1000_0000u32))
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
        dlog!("GatewayExecutor::execute()  (two-open close-to-open protocol)");
        dlog!("============================================================");

        let work_id = generate_work_id();
        let gw_path = gateway_path(fname, &work_id)?;

        dlog!("Target fname:           {:?}", fname);
        dlog!("Generated work_id:      {}", work_id);
        dlog!("Gateway path:           {:?}", gw_path);
        dlog!("command argument (raw): {:?}", command);

        if self.dry_run {
            self.vnprint("  [DRY RUN] gateway operation skipped");
            return Ok(vec!["dry run output".to_string()]);
        }

        // ---------- Build command bytes ----------
        let mut cmd_bytes = Vec::new();
        if fname.is_dir() {
            cmd_bytes.extend_from_slice(b"./");
        } else {
            cmd_bytes.extend_from_slice(b"./");
            let name = fname
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("Cannot get filename from {:?}", fname))?;
            cmd_bytes.extend_from_slice(name.to_string_lossy().as_bytes());
        }
        cmd_bytes.extend_from_slice(command.as_bytes());
        if is_windows() {
            cmd_bytes.extend_from_slice(WIN_PADDING);
        }

        dlog!("--- cmd_bytes to write ({} bytes) ---", cmd_bytes.len());
        dlog!("ASCII: {:?}", String::from_utf8_lossy(&cmd_bytes));
        dlog!("HEX:   {}", hex_dump(&cmd_bytes));

        // ============================================================
        // PHASE 1: WRITE
        //   open(O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC, 0666)
        //   write
        //   close          <- forces NFS to flush our write to the server
        // ============================================================
        let write_flags =
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_CLOEXEC;
        // 0666 (matches hs)
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;

        dlog!(
            "PHASE 1 syscall: open({:?}, O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC, 0666)",
            gw_path
        );
        let wfd: RawFd = open(&gw_path, write_flags, mode)
            .with_context(|| format!("write-phase open() failed for {:?}", gw_path))?;
        dlog!("                         -> wfd = {}", wfd);

        // SAFETY: we own wfd until the close() call below.
        let bwfd = unsafe { BorrowedFd::borrow_raw(wfd) };

        let write_outcome: Result<()> = (|| {
            dlog!("syscall: write(wfd={}, len={})", wfd, cmd_bytes.len());
            let n = write(bwfd, &cmd_bytes).with_context(|| "write() failed")?;
            dlog!("                         -> {} bytes written", n);
            if n != cmd_bytes.len() {
                anyhow::bail!("short write: {} of {}", n, cmd_bytes.len());
            }
            Ok(())
        })();

        // CRITICAL: close the write fd BEFORE opening for read. This is what
        // triggers NFS close-to-open consistency - the kernel flushes our
        // write to the server, where the Hammerspace driver intercepts it
        // and stages a response in place of the file's contents.
        dlog!("syscall: close(wfd={})  <-- forces NFS commit to server", wfd);
        match close(wfd) {
            Ok(_) => dlog!("                         -> closed ok"),
            Err(e) => dlog!("                         -> close error: {}", e),
        }
        write_outcome?;

        // ============================================================
        // PHASE 2: READ
        //   open(O_RDONLY|O_CLOEXEC)   <- fresh fd, no stale page cache
        //   read
        //   close
        // ============================================================
        let read_flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC;
        dlog!(
            "PHASE 2 syscall: open({:?}, O_RDONLY|O_CLOEXEC)",
            gw_path
        );
        let rfd: RawFd = open(&gw_path, read_flags, Mode::empty())
            .with_context(|| format!("read-phase open() failed for {:?}", gw_path))?;
        dlog!("                         -> rfd = {}", rfd);

        let read_outcome: Result<Vec<u8>> = (|| {
            let mut buf = vec![0u8; READ_BUF_SIZE];
            dlog!("syscall: read(rfd={}, max={})", rfd, READ_BUF_SIZE);
            let n = read(rfd, &mut buf).with_context(|| "read() failed")?;
            dlog!("                         -> {} bytes read", n);
            buf.truncate(n);

            dlog!("--- response ({} bytes) ---", buf.len());
            dlog!("ASCII: {:?}", String::from_utf8_lossy(&buf));
            dlog!("HEX:   {}", hex_dump(&buf));

            dlog!("--- DIAGNOSTIC: write/read comparison ---");
            if buf == cmd_bytes {
                dlog!("!!! READ STILL == WRITE  (driver did not intercept)");
            } else if buf.is_empty() {
                dlog!("!!! READ IS EMPTY (no response staged)");
            } else {
                dlog!("OK: Hammerspace responded with real data!");
            }
            Ok(buf)
        })();

        dlog!("syscall: close(rfd={})", rfd);
        match close(rfd) {
            Ok(_) => dlog!("                         -> closed ok"),
            Err(e) => dlog!("                         -> close error: {}", e),
        }

        // Cleanup: on Hammerspace, the driver typically leaves the gateway
        // file in place after a successful query (we observed this in the
        // strace - hs doesn't unlink either). Try to clean it up anyway so
        // we don't leak files; ignore any error.
        if gw_path.exists() {
            match unlink(&gw_path) {
                Ok(_) => dlog!("Cleanup: unlinked {:?}", gw_path),
                Err(e) => dlog!("Cleanup: unlink failed ({})", e),
            }
        }

        let buf = read_outcome?;
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
    fn test_generate_work_id_format() {
        let id = generate_work_id();
        assert!(id.starts_with("0x"));
        assert_eq!(id.len(), 9); // "0x" + 7 hex digits
        let id2 = generate_work_id();
        assert_ne!(id, id2);
    }

    #[test]
    fn test_gateway_path_file() {
        let fname = PathBuf::from("/tmp/test.txt");
        let gw = gateway_path(&fname, "0x12345ab").unwrap();
        assert_eq!(gw, PathBuf::from("/tmp/.fs_command_gateway 0x12345ab"));
    }

    #[test]
    fn test_gateway_path_dir() {
        let fname = PathBuf::from("/tmp/testdir");
        let gw = gateway_path(&fname, "0x12345ab").unwrap();
        assert_eq!(gw, PathBuf::from("/tmp/.fs_command_gateway 0x12345ab"));
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