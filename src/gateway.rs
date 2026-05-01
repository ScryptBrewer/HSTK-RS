// ============================================================
// REPLACE the imports at the top of src/gateway.rs with these:
// ============================================================
use anyhow::{Context, Result};
use nix::fcntl::{open, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{close, lseek, read, write, Whence};
use rand::Rng;
use std::io::Write as IoWrite;
use std::os::fd::BorrowedFd;            // <-- NEW
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};

// ============================================================
// REPLACE the entire execute() method body with this version.
// Only the open/write section is materially different - we wrap
// the RawFd in a BorrowedFd so it satisfies nix 0.29's AsFd bound
// on write(). lseek/read/close still take RawFd in 0.29, so those
// stay as-is.
// ============================================================
    pub fn execute(&self, fname: &Path, command: &str) -> Result<Vec<String>> {
        dlog!("============================================================");
        dlog!("GatewayExecutor::execute() called");
        dlog!("============================================================");

        let work_id = generate_work_id();
        let gw_path = gateway_path(fname, &work_id)?;

        // ---------- Pre-flight info ----------
        dlog!("Target fname:           {:?}", fname);
        dlog!("Target exists:          {}", fname.exists());
        dlog!("Target is_dir:          {}", fname.is_dir());
        dlog!("Target is_file:         {}", fname.is_file());
        match fname.canonicalize() {
            Ok(p) => dlog!("Target canonical:       {:?}", p),
            Err(e) => dlog!("Target canonical:       <error: {}>", e),
        }
        if let Ok(meta) = fname.metadata() {
            dlog!("Target size:            {} bytes", meta.len());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                dlog!("Target mode:            {:o}", meta.permissions().mode());
            }
        }
        dlog!("Generated work_id:      {}", work_id);
        dlog!("Gateway path:           {:?}", gw_path);
        dlog!("Gateway exists pre:     {}", gw_path.exists());
        dlog!("command argument (raw): {:?}", command);

        // List any pre-existing gateway files in the target directory
        let dir = if fname.is_dir() {
            fname.to_path_buf()
        } else {
            fname.parent().unwrap_or(Path::new(".")).to_path_buf()
        };
        dlog!("--- Pre-existing gateway files in {:?} ---", dir);
        if let Ok(entries) = std::fs::read_dir(&dir) {
            let mut found = 0;
            for entry in entries.flatten() {
                let name = entry.file_name();
                let s = name.to_string_lossy();
                if s.starts_with(".fs_command_gateway") {
                    dlog!("  stale: {:?}", s);
                    found += 1;
                }
            }
            if found == 0 {
                dlog!("  (none)");
            }
        } else {
            dlog!("  (could not read dir)");
        }

        if self.dry_run {
            self.vnprint("  [DRY RUN] gateway operation skipped");
            return Ok(vec!["dry run output".to_string()]);
        }

        // ---------- Build command bytes ----------
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
            dlog!("Path prefix:            ./{}  (file)", name.to_string_lossy());
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
            "syscall: open({:?}, flags=O_RDWR|O_CREAT (0x{:x}), mode=0644)",
            gw_path,
            oflags.bits()
        );
        let fd: RawFd = open(&gw_path, oflags, mode)
            .with_context(|| format!("open() failed for gateway file {:?}", gw_path))?;
        dlog!("                        -> fd = {}", fd);

        // SAFETY: we own this fd until we call close() at the bottom of this
        // function. BorrowedFd is only used to satisfy nix 0.29's AsFd bound
        // on write(); we don't keep it past these calls.
        let bfd = unsafe { BorrowedFd::borrow_raw(fd) };

        let inner: Result<Vec<u8>> = (|| {
            // ---------- single write() ----------
            dlog!("syscall: write(fd={}, len={})", fd, cmd_bytes.len());
            let written = write(bfd, &cmd_bytes).with_context(|| "write() failed".to_string())?;
            dlog!("                        -> {} bytes written", written);
            if written != cmd_bytes.len() {
                dlog!(
                    "  WARNING: short write! expected {}, got {}",
                    cmd_bytes.len(),
                    written
                );
            }

            // ---------- lseek(0, SEEK_SET) ----------
            dlog!("syscall: lseek(fd={}, offset=0, SEEK_SET)", fd);
            let pos =
                lseek(fd, 0, Whence::SeekSet).with_context(|| "lseek() failed".to_string())?;
            dlog!("                        -> position = {}", pos);

            // ---------- single read() ----------
            let mut buf = vec![0u8; READ_BUF_SIZE];
            dlog!("syscall: read(fd={}, max={})", fd, READ_BUF_SIZE);
            let n = read(fd, &mut buf).with_context(|| "read() failed".to_string())?;
            dlog!("                        -> {} bytes read", n);
            buf.truncate(n);

            dlog!("--- response ({} bytes) ---", buf.len());
            dlog!("ASCII: {:?}", String::from_utf8_lossy(&buf));
            dlog!("HEX:   {}", hex_dump(&buf));

            // CRITICAL DIAGNOSTIC: did Hammerspace actually intercept?
            dlog!("--- DIAGNOSTIC: write/read comparison ---");
            if buf == cmd_bytes {
                dlog!("!!! READ BUFFER IS BYTE-IDENTICAL TO WRITTEN BYTES !!!");
                dlog!("!!! Hammerspace did NOT process the command.        !!!");
                dlog!("!!! The .fs_command_gateway file is behaving as a   !!!");
                dlog!("!!! plain regular file - the FS driver is not       !!!");
                dlog!("!!! intercepting it.                                !!!");
                dlog!("Possible causes:");
                dlog!("  1. Mount is not actually Hammerspace (or wrong NFS variant)");
                dlog!("  2. The 'hs' binary uses a different syscall pattern");
                dlog!("  3. Page cache is interposing - try O_DIRECT");
                dlog!("  4. Filename encoding mismatch (look at HEX above carefully)");
            } else if buf.starts_with(&cmd_bytes) {
                dlog!("!!! READ BUFFER STARTS WITH WRITTEN BYTES !!!");
                dlog!("    Possibly two writes or buffered read.");
            } else if buf.is_empty() {
                dlog!("!!! READ BUFFER IS EMPTY - response not yet available?");
            } else {
                dlog!("OK: read buffer differs from written bytes (Hammerspace responded)");
            }

            Ok(buf)
        })();

        // ---------- close() always ----------
        dlog!("syscall: close(fd={})", fd);
        match close(fd) {
            Ok(_) => dlog!("                        -> closed ok"),
            Err(e) => dlog!("                        -> close error: {}", e),
        }

        dlog!("Gateway exists post:    {}", gw_path.exists());
        if gw_path.exists() {
            dlog!("NOTE: gateway file still exists after close.");
            dlog!("      (real hs typically lets the FS driver clean it up)");
        }

        let buf = inner?;
        let buffer_str = String::from_utf8_lossy(&buf).to_string();
        let lines: Vec<String> = buffer_str.lines().map(|s| s.to_string()).collect();
        dlog!("Returning {} lines to caller", lines.len());
        dlog!("============================================================");

        Ok(lines)
    }