// src/gateway.rs
// Shadow command gateway system for Hammerspace CLI.

use anyhow::{Context, Result};
use nix::fcntl::{open, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{close, read, unlink, write};
use rand::Rng;
use std::os::fd::BorrowedFd;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};

/// Windows clients append 50 NUL bytes of padding to gateway writes.
const WIN_PADDING: &[u8] = &[0u8; 50];

/// Maximum response size we'll read from the gateway file.
const READ_BUF_SIZE: usize = 65536;

pub fn is_windows() -> bool {
    cfg!(windows)
}

/// Generate a work ID matching the `hs` binary's format: `0x` + 7 hex digits.
pub fn generate_work_id() -> String {
    let mut rng = rand::thread_rng();
    format!("0x{:07x}", rng.gen_range(0..0x1000_0000u32))
}

/// Build the `.fs_command_gateway <work_id>` path for the given target.
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

    /// Execute a Hammerspace shadow command against the given path.
    ///
    /// Protocol (verified via strace of the official `hs` binary on a
    /// Hammerspace NFS 4.2 mount):
    ///
    /// 1. `open(".fs_command_gateway 0xXXXXXXX", O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC, 0666)`
    /// 2. `write(wfd, "./PREFIX COMMAND")`
    /// 3. `close(wfd)`  -- forces NFS commit via close-to-open consistency
    /// 4. `open(same path, O_RDONLY|O_CLOEXEC)` -- fresh fd avoids stale page cache
    /// 5. `read(rfd, response)`
    /// 6. `close(rfd)`
    ///
    /// The two-open dance with a close in between is essential. A single
    /// `O_RDWR` fd lets the write data sit in the local NFS page cache and
    /// the read returns the bytes we just wrote, so the Hammerspace driver
    /// on the server never sees the request. `fsync()` plus
    /// `posix_fadvise(POSIX_FADV_DONTNEED)` is NOT sufficient -- only
    /// `close()` then `open()` trips proper NFS close-to-open semantics.
    pub fn execute(&self, fname: &Path, command: &str) -> Result<Vec<String>> {
        let work_id = generate_work_id();
        let gw_path = gateway_path(fname, &work_id)?;

        self.dprint(&format!("gateway path: {:?}", gw_path));
        self.dprint(&format!("command:      {:?}", command));

        if self.dry_run {
            self.vnprint(&format!("  [DRY RUN] gateway: {:?}  cmd: {:?}", gw_path, command));
            return Ok(vec!["dry run output".to_string()]);
        }

        // ---- Build the command bytes ----
        let mut cmd_bytes = Vec::new();
        cmd_bytes.extend_from_slice(b"./");
        if !fname.is_dir() {
            let name = fname
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("Cannot get filename from {:?}", fname))?;
            cmd_bytes.extend_from_slice(name.to_string_lossy().as_bytes());
        }
        cmd_bytes.extend_from_slice(command.as_bytes());
        if is_windows() {
            cmd_bytes.extend_from_slice(WIN_PADDING);
        }

        // ---- PHASE 1: write the command, then close ----
        let write_flags =
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_CLOEXEC;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;

        let wfd: RawFd = open(&gw_path, write_flags, mode)
            .with_context(|| format!("write-phase open() failed for {:?}", gw_path))?;

        // SAFETY: we own wfd until the close() call below; BorrowedFd is only
        // used to satisfy nix 0.29's AsFd bound on write().
        let bwfd = unsafe { BorrowedFd::borrow_raw(wfd) };

        let write_outcome: Result<()> = (|| {
            let n = write(bwfd, &cmd_bytes).with_context(|| "write() failed")?;
            if n != cmd_bytes.len() {
                anyhow::bail!("short write: {} of {}", n, cmd_bytes.len());
            }
            Ok(())
        })();

        // CRITICAL: close before reopening - this is what triggers NFS to
        // flush our write to the server, where the Hammerspace driver
        // intercepts and stages the response.
        let _ = close(wfd);
        write_outcome?;

        // ---- PHASE 2: open fresh for read, then close ----
        let read_flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC;
        let rfd: RawFd = open(&gw_path, read_flags, Mode::empty())
            .with_context(|| format!("read-phase open() failed for {:?}", gw_path))?;

        let read_outcome: Result<Vec<u8>> = (|| {
            let mut buf = vec![0u8; READ_BUF_SIZE];
            let n = read(rfd, &mut buf).with_context(|| "read() failed")?;
            buf.truncate(n);
            Ok(buf)
        })();

        let _ = close(rfd);

        // Cleanup: best-effort removal of the gateway file. Ignore errors.
        if gw_path.exists() {
            let _ = unlink(&gw_path);
        }

        let buf = read_outcome?;
        let buffer_str = String::from_utf8_lossy(&buf).to_string();
        let lines: Vec<String> = buffer_str.lines().map(|s| s.to_string()).collect();

        self.dprint(&format!("response: {} bytes, {} lines", buf.len(), lines.len()));

        Ok(lines)
    }

    fn vnprint(&self, line: &str) {
        if self.verbose || self.dry_run {
            let tag = if self.dry_run { "N: " } else { "V: " };
            println!("{}{}", tag, line);
        }
    }

    fn dprint(&self, line: &str) {
        if self.debug {
            eprintln!("D: {}", line);
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

    #[test]
    fn test_execute_on_paths() {
        let executor = GatewayExecutor::new(true, false, false);
        let paths = vec![PathBuf::from("/tmp/test1"), PathBuf::from("/tmp/test2")];
        let results =
            execute_on_paths(&paths, |_| Ok("test command".to_string()), &executor).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_mock_gateway_executor() {
        let executor = MockGatewayExecutor::new(false, false, false);
        let results = executor
            .execute(Path::new("/tmp/test"), "test command")
            .unwrap();
        assert_eq!(results.len(), 2);
    }
}