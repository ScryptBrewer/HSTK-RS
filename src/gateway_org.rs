// src/gateway.rs
// Shadow command gateway system for Hammerspace CLI
// Handles .fs_command_gateway file creation and execution

use anyhow::{Context, Result};
use rand::Rng;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Windows padding for gateway writes
/// Windows doesn't push writes through the stack if there's not enough data
const WIN_PADDING: &[u8] = &[0u8; 50];

/// Detect if running on Windows
pub fn is_windows() -> bool {
    cfg!(windows)
}

/// Generate a random work ID for gateway files
pub fn generate_work_id() -> String {
    let mut rng = rand::thread_rng();
    format!("{:08x}", rng.gen_range(0..99999999))
}

/// Create gateway file path
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

/// Shadow command gateway executor
#[derive(Debug, Clone)]
pub struct GatewayExecutor {
    /// Dry run mode - don't actually execute commands
    pub dry_run: bool,
    /// Verbose output
    pub verbose: bool,
    /// Debug output
    pub debug: bool,
}

impl GatewayExecutor {
    /// Create a new gateway executor
    pub fn new(dry_run: bool, verbose: bool, debug: bool) -> Self {
        Self {
            dry_run,
            verbose,
            debug,
        }
    }

    /// Execute a shadow command on a file.
    ///
    /// CRITICAL: The Hammerspace gateway protocol REQUIRES that the write and
    /// the read occur on the SAME open file descriptor. The FS driver correlates
    /// the request and response via the open-file context. If you close the
    /// file between writing and reading (as the previous implementation did),
    /// the read returns the literal bytes you wrote rather than the
    /// hammerscript evaluation result.
    pub fn execute(&self, fname: &Path, command: &str) -> Result<Vec<String>> {
        let work_id = generate_work_id();
        let gw_path = gateway_path(fname, &work_id)?;

        self.dprint(&format!("Gateway path: {:?}", gw_path));
        self.dprint(&format!("Command to write: {}", command));
        self.dprint(&format!("Target path exists: {}", fname.exists()));
        self.dprint(&format!("Target path is_dir: {}", fname.is_dir()));

        if self.dry_run {
            self.vnprint("  [DRY RUN] gateway operation skipped");
            return Ok(vec!["dry run output".to_string()]);
        }

        // Build command bytes with the path prefix Hammerspace expects:
        //   ./           for directories
        //   ./<filename> for files
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

        // Windows doesn't flush small writes through the stack reliably,
        // so pad to ensure the write is dispatched.
        if is_windows() {
            cmd_bytes.extend_from_slice(WIN_PADDING);
        }

        self.dprint(&format!("open({:?}) [O_RDWR|O_CREAT]", gw_path));
        self.dprint(&format!("write({:?})", String::from_utf8_lossy(&cmd_bytes)));

        // Open ONCE with O_RDWR | O_CREAT. Do NOT close between write and read.
        // (Avoid O_TRUNC: the file is uniquely named via work_id, so it should
        // not pre-exist, and we want the create+write+read pattern to mirror
        // the reference Python implementation as closely as possible.)
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&gw_path)
            .with_context(|| format!("Failed to open gateway file {:?}", gw_path))?;

        // Push the command into the gateway.
        file.write_all(&cmd_bytes)
            .with_context(|| format!("Failed to write to gateway file {:?}", gw_path))?;

        self.dprint("flush()");
        file.flush()
            .with_context(|| format!("Failed to flush gateway file {:?}", gw_path))?;

        // Rewind so the read returns the response (NOT the bytes we just wrote).
        self.dprint("seek(0)");
        file.seek(SeekFrom::Start(0))
            .with_context(|| format!("Failed to seek gateway file {:?}", gw_path))?;

        // Read the hammerscript evaluation result on the SAME fd.
        self.dprint("read()");
        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .with_context(|| format!("Failed to read from gateway file {:?}", gw_path))?;

        self.dprint(&format!("close({:?})", gw_path));
        drop(file);

        self.dprint(&format!(
            "Read {} bytes: {}",
            buffer.len(),
            if buffer.len() > 100 {
                format!("{}...", &buffer[..100])
            } else {
                buffer.clone()
            }
        ));

        let lines: Vec<String> = buffer.lines().map(|s| s.to_string()).collect();
        self.dprint(&format!("Returning {} lines", lines.len()));

        Ok(lines)
    }

    /// Print verbose/dry-run message
    fn vnprint(&self, line: &str) {
        if self.verbose || self.dry_run {
            let tag = if self.dry_run { "N: " } else { "V: " };
            println!("{}{}", tag, line);
        }
    }

    /// Print debug message
    fn dprint(&self, line: &str) {
        if self.debug {
            println!("D: {}", line);
        }
    }
}

/// Execute a shadow command on multiple paths
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

/// Mock gateway executor for testing
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
        self.dprint("MockGatewayExecutor::execute() called");

        if self.dry_run {
            self.vnprint("  [DRY RUN] mock gateway skipped");
            return Ok(vec!["dry run output".to_string()]);
        }

        Ok(self.mock_output.clone())
    }

    fn vnprint(&self, line: &str) {
        if self.verbose || self.dry_run {
            let tag = if self.dry_run { "N: " } else { "V: " };
            println!("{}{}", tag, line);
        }
    }

    fn dprint(&self, line: &str) {
        if self.debug {
            println!("D: {}", line);
        }
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
        // In actual usage, paths are validated to exist before being passed.
        // This test uses a path that doesn't exist, so is_dir() returns false
        // and the parent is used.
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

    #[test]
    fn test_gateway_executor_verbose() {
        let executor = GatewayExecutor::new(true, true, false);
        let results = executor
            .execute(Path::new("/tmp/test"), "test command")
            .unwrap();
        assert_eq!(results, vec!["dry run output".to_string()]);
    }

    #[test]
    fn test_gateway_executor_debug() {
        let executor = GatewayExecutor::new(true, false, true);
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
        assert_eq!(results[0].1, vec!["dry run output".to_string()]);
        assert_eq!(results[1].1, vec!["dry run output".to_string()]);
    }

    #[test]
    fn test_mock_gateway_executor() {
        let executor = MockGatewayExecutor::new(false, false, false);
        let results = executor
            .execute(Path::new("/tmp/test"), "test command")
            .unwrap();
        assert_eq!(
            results,
            vec![
                "mock result line 1".to_string(),
                "mock result line 2".to_string()
            ]
        );
    }

    #[test]
    fn test_mock_gateway_executor_dry_run() {
        let executor = MockGatewayExecutor::new(true, false, false);
        let results = executor
            .execute(Path::new("/tmp/test"), "test command")
            .unwrap();
        assert_eq!(results, vec!["dry run output".to_string()]);
    }
}