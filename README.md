# Hammerspace HSTK

A Rust CLI tool for interacting with Hammerspace software-defined storage via shadow commands. This is a Rust reimplementation of the original Python-based `hs` CLI (`hstk/hscli.py`).

## Overview

HSTK communicates with Hammerspace storage by writing commands to special `.fs_command_gateway` files on NFS 4.2 mounts. The Hammerspace server-side driver intercepts these files and processes the commands, enabling offloaded metadata operations, fast file copies, and storage management directly from the command line.

## Building

```bash
cargo build --release
```

The binary is named `hs`.

## Usage

```
hs [OPTIONS] <COMMAND>

Options:
  -v, --verbose    Increase verbosity (can be repeated)
  -n, --dry-run    Show what would be done without executing
  -d, --debug      Enable debug output
  -j, --json       Output results as JSON
  --cmd-tree       Print the full command tree and exit

Commands:
  eval                Evaluate hsscript expressions on files
  sum                 Perform fast calculations on files
  attribute           Manage inode attributes (list, get, has, set, add, delete)
  tag                 Manage inode tags (list, get, has, set, add, delete)
  keyword             Manage inode keywords (list, has, add, delete)
  label               Manage inode labels (list, has, add, delete)
  rekognition-tag     Manage rekognition tags (list, get, has, set, add, delete)
  objective           Manage objectives (list, has, add, delete)
  rm                  Fast offloaded rm -rf (falls back to system rm for unsupported options)
  cp                  Fast offloaded recursive copy (-a archive mode)
  rsync               Fast offloaded recursive directory equalizer (--archive --delete)
  collsum             Collection usage summary
  status              System/component/task status subcommands
  usage               Resource utilization subcommands
  perf                Performance and operation statistics
  dump                Dump info about inodes, shares, volumes, objectives, etc.
  keep-on-site        Manage GNS replication site pinning
```

### Examples

```bash
# List all attributes on a path
hs attribute list /mnt/hs/data

# Set a tag recursively
hs tag set mytag "value" -r /mnt/hs/data

# Add a keyword
hs keyword add important /mnt/hs/data/file.txt

# Fast recursive delete (offloaded to Hammerspace)
hs rm -rf /mnt/hs/data/old_dir

# Fast archive copy
hs cp -a /mnt/hs/src /mnt/hs/dest

# Evaluate an expression
hs eval -e "SIZE>1GB" /mnt/hs/data

# Get storage volume status
hs status volume /mnt/hs/data

# Dump inode info
hs dump inode --full /mnt/hs/data

# Dry run any command
hs -n tag add test_tag /mnt/hs/data
```

## Project Structure

```
src/
  main.rs         CLI entry point, argument parsing, command handlers
  gateway.rs      Shadow command gateway - writes/reads .fs_command_gateway files
  expression.rs   Hammerspace expression builder (eval, sum, set, list, read, update, delete)
  helpers.rs      Utility functions (metadata, filesystem checks, verbose/debug output)
```

## How It Works

The gateway system uses a two-phase file I/O protocol over NFS:

1. **Write phase** — Creates `.fs_command_gateway 0xXXXXXXX`, writes the command, then closes the file descriptor. The close triggers NFS close-to-open consistency, flushing the write to the server where the Hammerspace driver intercepts it.
2. **Read phase** — Reopens the same file for reading with a fresh file descriptor. The Hammerspace driver has staged the response. After reading, the gateway file is cleaned up.

This close-then-reopen pattern is critical — a single `O_RDWR` descriptor would read back stale data from the local NFS page cache.

## Dependencies

- **clap** — CLI argument parsing
- **anyhow** — Error handling
- **serde_json** — JSON output/parsing
- **rand** — Work ID generation
- **nix** — Low-level Unix file I/O (open, read, write, close, unlink, chown)
- **tempfile** — Temporary file support

## Requirements

- Rust 2024 edition
- Unix-like OS (uses POSIX file operations)
- Hammerspace NFS 4.2 mount for gateway commands to function

## Testing

```bash
cargo test
```
