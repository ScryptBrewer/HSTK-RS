// src/helpers.rs
// Helper functions for Hammerspace CLI

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{stdout, Write};
use std::path::{Path, PathBuf};

/// Global settings structure
#[derive(Debug, Clone)]
pub struct Globals {
    pub verbose: u8,
    pub dry_run: bool,
    pub debug: bool,
    pub output_json: bool,
}

impl Globals {
    pub fn new(verbose: u8, dry_run: bool, debug: bool, output_json: bool) -> Self {
        Self {
            verbose,
            dry_run,
            debug,
            output_json,
        }
    }
}

/// Print verbose/dry-run message
pub fn vnprint(globals: &Globals, line: &str) {
    if globals.verbose > 0 || globals.dry_run {
        let tag = if globals.dry_run { "N: " } else { "V: " };
        println!("{}{}", tag, line);
    }
}

/// Print debug message
pub fn dprint(globals: &Globals, line: &str) {
    if globals.debug {
        println!("D: {}", line);
    }
}

/// Create or verify .stats file for performance counters
pub fn create_stats_file(path: &Path, dry_run: bool) -> Result<PathBuf> {
    let stats_path = path.join(".stats");

    if dry_run {
        vnprint(
            &Globals::new(1, true, false, false),
            &format!("dry run, not creating .stats file {:?}", stats_path),
        );
        return Ok(stats_path);
    }

    if !stats_path.exists() {
        vnprint(
            &Globals::new(1, false, false, false),
            &format!("creating .stats file {:?}", stats_path),
        );
        File::create(&stats_path)
            .with_context(|| format!("Failed to create .stats file {:?}", stats_path))?;
    }

    Ok(stats_path)
}

/// Create .stats files for multiple paths
pub fn create_stats_files(paths: &[PathBuf], dry_run: bool) -> Result<HashMap<PathBuf, PathBuf>> {
    let mut stats_files = HashMap::new();

    for path in paths {
        let stats_path = create_stats_file(path, dry_run)?;
        stats_files.insert(path.clone(), stats_path);
    }

    Ok(stats_files)
}

/// Copy metadata from source to destination
/// Copies ownership and permissions
pub fn copy_metadata(src: &Path, dest: &Path, dry_run: bool) -> Result<()> {
    let globals = Globals::new(0, dry_run, false, false);

    vnprint(&globals, &format!("stat {:?}", src));

    if dry_run {
        vnprint(&globals, &format!("chown dry_run.dry_run {:?}", dest));
        vnprint(&globals, &format!("chmod dry_run {:?}", dest));
        return Ok(());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let src_metadata =
            fs::metadata(src).with_context(|| format!("Failed to get metadata for {:?}", src))?;

        let uid = src_metadata.uid();
        let gid = src_metadata.gid();
        let mode = src_metadata.mode();

        vnprint(&globals, &format!("chown {}.{} {:?}", uid, gid, dest));
        nix::unistd::chown(
            dest,
            Some(nix::unistd::Uid::from_raw(uid)),
            Some(nix::unistd::Gid::from_raw(gid)),
        )
        .with_context(|| format!("Failed to chown {:?}", dest))?;

        vnprint(&globals, &format!("chmod {:o} {:?}", mode, dest));
        fs::set_permissions(dest, fs::Permissions::from_mode(mode))
            .with_context(|| format!("Failed to chmod {:?}", dest))?;
    }

    #[cfg(not(unix))]
    {
        // On non-Unix systems, we can't copy ownership
        vnprint(&globals, "metadata copying not supported on this platform");
    }

    // TODO: Add copying of ACLs
    // TODO: Add copying of HS metadata like tags, objectives, etc

    Ok(())
}

/// Check if a path exists
pub fn path_exists(path: &Path) -> bool {
    path.exists()
}

/// Check if a path is a directory
pub fn is_directory(path: &Path) -> bool {
    path.is_dir()
}

/// Check if a path is a file
pub fn is_file(path: &Path) -> bool {
    path.is_file()
}

/// Create a directory if it doesn't exist
pub fn ensure_directory(path: &Path, dry_run: bool) -> Result<()> {
    let globals = Globals::new(0, dry_run, false, false);

    if !path.exists() {
        vnprint(&globals, &format!("mkdir {:?}", path));

        if !dry_run {
            fs::create_dir_all(path)
                .with_context(|| format!("Failed to create directory {:?}", path))?;
        }
    }

    Ok(())
}

/// Remove a file
pub fn remove_file(path: &Path, dry_run: bool) -> Result<()> {
    let globals = Globals::new(0, dry_run, false, false);

    vnprint(&globals, &format!("unlink {:?}", path));

    if !dry_run {
        fs::remove_file(path).with_context(|| format!("Failed to remove file {:?}", path))?;
    }

    Ok(())
}

/// Remove a directory
pub fn remove_directory(path: &Path, dry_run: bool) -> Result<()> {
    let globals = Globals::new(0, dry_run, false, false);

    vnprint(&globals, &format!("rmdir {:?}", path));

    if !dry_run {
        fs::remove_dir(path).with_context(|| format!("Failed to remove directory {:?}", path))?;
    }

    Ok(())
}

/// Get file statistics
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

pub fn get_file_stats(path: &Path) -> Result<FileStats> {
    let metadata =
        fs::metadata(path).with_context(|| format!("Failed to get metadata for {:?}", path))?;

    #[cfg(unix)]
    {
        Ok(FileStats {
            ino: metadata.ino(),
            dev: metadata.dev(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
        })
    }

    #[cfg(windows)]
    {
        Ok(FileStats {
            ino: metadata.file_index(),
            dev: 0, // Windows doesn't have device numbers
            mode: 0,
            uid: 0,
            gid: 0,
        })
    }
}

/// File statistics structure
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FileStats {
    pub ino: u64,
    pub dev: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

/// Check if two paths are on the same filesystem
pub fn same_filesystem(path1: &Path, path2: &Path) -> Result<bool> {
    let stats1 = get_file_stats(path1)?;
    let stats2 = get_file_stats(path2)?;

    Ok(stats1.dev == stats2.dev)
}

/// Flush stdout
pub fn flush_stdout() {
    let _ = stdout().flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_vnprint_verbose() {
        let globals = Globals::new(1, false, false, false);
        vnprint(&globals, "test message");
    }

    #[test]
    fn test_vnprint_dry_run() {
        let globals = Globals::new(0, true, false, false);
        vnprint(&globals, "test message");
    }

    #[test]
    fn test_dprint() {
        let globals = Globals::new(0, false, true, false);
        dprint(&globals, "test debug message");
    }

    #[test]
    fn test_path_exists() {
        assert!(path_exists(Path::new(".")));
        assert!(!path_exists(Path::new("/nonexistent/path/12345")));
    }

    #[test]
    fn test_is_directory() {
        assert!(is_directory(Path::new(".")));
        assert!(!is_directory(Path::new("Cargo.toml")));
    }

    #[test]
    fn test_is_file() {
        assert!(is_file(Path::new("Cargo.toml")));
        assert!(!is_file(Path::new(".")));
    }

    #[test]
    fn test_same_filesystem() {
        let result = same_filesystem(Path::new("."), Path::new("Cargo.toml"));
        assert!(result.is_ok());
        assert!(result.unwrap());
    }
}
