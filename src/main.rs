// src/main.rs
// Hammerspace CLI - Rust implementation
// Based on hstk/hscli.py and hstk/hsscript.py

mod expression;
mod gateway;
mod helpers;

use anyhow::{bail, Result};
use clap::{Args, Parser, Subcommand};
use helpers::{create_stats_files, dprint, ensure_directory, flush_stdout, get_file_stats,
              is_directory, is_file, path_exists, remove_directory, remove_file, same_filesystem,
              Globals};
use serde_json::to_string as json_to_string;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use expression::*;
use gateway::*;
use helpers::*;

// CLI Structure
#[derive(Parser, Debug)]
#[command(name = "hs", version = "2.0", about = "Hammerspace hammerscript CLI")]
struct Cli {
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(short = 'n', long = "dry-run", action)]
    dry_run: bool,

    #[arg(short = 'd', long = "debug", action)]
    debug: bool,

    #[arg(short = 'j', long = "json", action)]
    output_json: bool,

    #[arg(long = "cmd-tree", action)]
    cmd_tree: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Evaluate hsscript expressions on files
    Eval(EvalArgs),

    /// Perform fast calculations on files
    Sum(SumArgs),

    /// Manage inode attributes
    Attribute(AttributeArgs),

    /// Manage inode tags
    Tag(TagArgs),

    /// Manage inode keywords
    Keyword(KeywordArgs),

    /// Manage inode labels
    Label(LabelArgs),

    /// Manage rekognition tags
    RekognitionTag(RekognitionTagArgs),

    /// Manage objectives
    Objective(ObjectiveArgs),

    /// Fast offloaded rm -rf
    Rm(RmArgs),

    /// Fast offloaded recursive copy
    Cp(CpArgs),

    /// Fast offloaded recursive directory equalizer
    Rsync(RsyncArgs),

    /// Collection usage summary
    Collsum(CollsumArgs),

    /// Status commands
    Status(StatusArgs),

    /// Usage commands
    Usage(UsageArgs),

    /// Performance commands
    Perf(PerfArgs),

    /// Dump commands
    Dump(DumpArgs),

    /// Keep-on-site commands
    KeepOnSite(KeepOnSiteArgs),
}

// ALL STRUCTS WITH Debug DERIVE
#[derive(Args, Debug)]
struct EvalArgs {
    #[arg(short = 'e', long = "exp")]
    exp: Option<String>,

    #[arg(short = 'i', long = "exp-stdin", action)]
    exp_stdin: bool,

    #[arg(short = 'j', long = "json", action)]
    input_json: bool,

    #[arg(short = 's', long = "string", action)]
    string: bool,

    #[arg(short = 'r', long = "recursive", action)]
    recursive: bool,

    #[arg(long = "nonfiles", action)]
    nonfiles: bool,

    #[arg(long = "raw", action)]
    raw: bool,

    #[arg(long = "compact", action)]
    compact: bool,

    #[arg(long = "interactive", action)]
    interactive: bool,

    /// Paths (defaults to current directory)
    #[arg(default_value = ".")]
    paths: Vec<PathBuf>,
}

#[derive(Args, Debug)]
struct SumArgs {
    #[arg(short = 'e', long = "exp")]
    exp: Option<String>,

    #[arg(long = "raw", action)]
    raw: bool,

    #[arg(long = "compact", action)]
    compact: bool,

    #[arg(long = "nonfiles", action)]
    nonfiles: bool,

    /// Paths (defaults to current directory)
    #[arg(default_value = ".")]
    paths: Vec<PathBuf>,
}

#[derive(Args, Debug)]
struct AttributeArgs {
    #[command(subcommand)]
    action: AttributeAction,
}

#[derive(Subcommand, Debug)]
enum AttributeAction {
    List {
        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        paths: Vec<PathBuf>,
    },
    Get {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        paths: Vec<PathBuf>,
    },
    Has {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Set {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: String,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 's', long = "string", action)]
        string: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Add {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: String,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 's', long = "string", action)]
        string: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Delete {
        name: String,

        #[arg(short = 'f', long = "force", action)]
        force: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct TagArgs {
    #[command(subcommand)]
    action: TagAction,
}

#[derive(Subcommand, Debug)]
enum TagAction {
    List {
        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        paths: Vec<PathBuf>,
    },
    Get {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        paths: Vec<PathBuf>,
    },
    Has {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Set {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: String,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 's', long = "string", action)]
        string: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Add {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: String,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 's', long = "string", action)]
        string: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Delete {
        name: String,

        #[arg(short = 'f', long = "force", action)]
        force: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct KeywordArgs {
    #[command(subcommand)]
    action: KeywordAction,
}

#[derive(Subcommand, Debug)]
enum KeywordAction {
    List {
        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Has {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Add {
        name: String,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Delete {
        name: String,

        #[arg(short = 'f', long = "force", action)]
        force: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct LabelArgs {
    #[command(subcommand)]
    action: LabelAction,
}

#[derive(Subcommand, Debug)]
enum LabelAction {
    List {
        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Has {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Add {
        name: String,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Delete {
        name: String,

        #[arg(short = 'f', long = "force", action)]
        force: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct RekognitionTagArgs {
    #[command(subcommand)]
    action: RekognitionTagAction,
}

#[derive(Subcommand, Debug)]
enum RekognitionTagAction {
    List {
        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        paths: Vec<PathBuf>,
    },
    Get {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        paths: Vec<PathBuf>,
    },
    Has {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Set {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: String,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 's', long = "string", action)]
        string: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Add {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: String,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 's', long = "string", action)]
        string: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Delete {
        name: String,

        #[arg(short = 'f', long = "force", action)]
        force: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct ObjectiveArgs {
    #[command(subcommand)]
    action: ObjectiveAction,
}

#[derive(Subcommand, Debug)]
enum ObjectiveAction {
    List {
        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'a', long = "active", action)]
        active: bool,

        #[arg(long = "effective", action)]
        effective: bool,

        #[arg(long = "share", action)]
        share: bool,

        paths: Vec<PathBuf>,
    },
    Has {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: Option<String>,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'a', long = "active", action)]
        active: bool,

        #[arg(long = "effective", action)]
        effective: bool,

        #[arg(long = "share", action)]
        share: bool,

        paths: Vec<PathBuf>,
    },
    Add {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: String,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 's', long = "string", action)]
        string: bool,

        #[arg(short = 'u', long = "unbound", action)]
        unbound: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Delete {
        name: String,

        #[arg(short = 'e', long = "exp")]
        exp: Option<String>,

        #[arg(short = 'j', long = "json", action)]
        input_json: bool,

        #[arg(short = 'f', long = "force", action)]
        force: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct RmArgs {
    #[arg(short = 'r', long = "recursive", action)]
    recursive: bool,

    #[arg(short = 'f', long = "force", action)]
    force: bool,

    #[arg(short = 'i', action)]
    interactive: bool,

    #[arg(short = 'I', action)]
    i: bool,

    #[arg(long = "interactive", action)]
    interactive_value: Option<String>,

    #[arg(long = "one-file-system", action)]
    one_file_system: bool,

    #[arg(long = "no-preserve-root", action)]
    no_preserve_root: bool,

    #[arg(long = "preserve-root", action)]
    preserve_root: bool,

    #[arg(short = 'd', long = "dir", action)]
    dir: bool,

    #[arg(short = 'v', long = "verbose", action)]
    verbose: bool,

    paths: Vec<PathBuf>,
}

#[derive(Args, Debug)]
struct CpArgs {
    #[arg(short = 'a', long = "archive", action)]
    archive: bool,

    srcs: Vec<PathBuf>,
    dest: PathBuf,
}

#[derive(Args, Debug)]
struct RsyncArgs {
    #[arg(short = 'a', long = "archive", action)]
    archive: bool,

    #[arg(long = "delete", action)]
    delete: bool,

    src: PathBuf,
    dest: PathBuf,
}

#[derive(Args, Debug)]
struct CollsumArgs {
    collection: String,

    #[arg(long = "collation")]
    collation: Option<String>,

    paths: Vec<PathBuf>,
}

#[derive(Args, Debug)]
struct StatusArgs {
    #[command(subcommand)]
    action: StatusAction,
}

#[derive(Subcommand, Debug)]
enum StatusAction {
    Assimilation {
        paths: Vec<PathBuf>,
    },
    Csi {
        paths: Vec<PathBuf>,
    },
    Collections {
        paths: Vec<PathBuf>,
    },
    Errors {
        #[arg(long = "dump", action)]
        dump: bool,

        paths: Vec<PathBuf>,
    },
    Open {
        paths: Vec<PathBuf>,
    },
    Replication {
        paths: Vec<PathBuf>,
    },
    Sweeper {
        paths: Vec<PathBuf>,
    },
    Volume {
        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct UsageArgs {
    #[command(subcommand)]
    action: UsageAction,
}

#[derive(Subcommand, Debug)]
enum UsageAction {
    Alignment {
        #[arg(long = "top-files", action)]
        top_files: bool,

        paths: Vec<PathBuf>,
    },
    VirusScan {
        #[arg(long = "top-files", action)]
        top_files: bool,

        paths: Vec<PathBuf>,
    },
    Owner {
        #[arg(long = "top-files", action)]
        top_files: bool,

        paths: Vec<PathBuf>,
    },
    Online {
        paths: Vec<PathBuf>,
    },
    Volume {
        #[arg(long = "top-files", action)]
        top_files: bool,

        #[arg(long = "deep", action)]
        deep: bool,

        paths: Vec<PathBuf>,
    },
    User {
        #[arg(long = "details", action)]
        details: bool,

        paths: Vec<PathBuf>,
    },
    Objectives {
        paths: Vec<PathBuf>,
    },
    MimeTags {
        paths: Vec<PathBuf>,
    },
    RekognitionTags {
        paths: Vec<PathBuf>,
    },
    Dirs {
        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct PerfArgs {
    #[command(subcommand)]
    action: PerfAction,
}

#[derive(Subcommand, Debug)]
enum PerfAction {
    Clear {
        paths: Vec<PathBuf>,
    },
    TopCalls {
        paths: Vec<PathBuf>,
    },
    TopFuncs {
        #[arg(long = "op")]
        op: Option<String>,

        paths: Vec<PathBuf>,
    },
    TopOps {
        paths: Vec<PathBuf>,
    },
    Flushes {
        paths: Vec<PathBuf>,
    },
}

#[derive(Args, Debug)]
struct DumpArgs {
    #[command(subcommand)]
    action: DumpAction,
}

#[derive(Subcommand, Debug)]
enum DumpAction {
    Inode {
        #[arg(long = "full", action)]
        full: bool,

        paths: Vec<PathBuf>,
    },
    Iinfo {
        paths: Vec<PathBuf>,
    },
    Share {
        #[arg(long = "filter-volume")]
        filter_volume: Option<String>,

        paths: Vec<PathBuf>,
    },
    Misaligned {
        paths: Vec<PathBuf>,
    },
    Threat {
        paths: Vec<PathBuf>,
    },
    MapFileToObj {
        bucket_name: String,

        paths: Vec<PathBuf>,
    },
    FilesOnVolume {
        volume_name: String,

        paths: Vec<PathBuf>,
    },
    Volumes {
        path: PathBuf,
    },
    VolumeGroups {
        path: PathBuf,
    },
    Objectives {
        path: PathBuf,
    },
}

#[derive(Args, Debug)]
struct KeepOnSiteArgs {
    #[command(subcommand)]
    action: KeepOnSiteAction,
}

#[derive(Subcommand, Debug)]
enum KeepOnSiteAction {
    Available {
        paths: Vec<PathBuf>,
    },
    List {
        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Has {
        name: String,

        #[arg(short = 'l', long = "local", action)]
        local: bool,

        #[arg(short = 'h', long = "inherited", action)]
        inherited: bool,

        #[arg(short = 'o', long = "object", action)]
        object: bool,

        paths: Vec<PathBuf>,
    },
    Delete {
        name: String,

        #[arg(short = 'f', long = "force", action)]
        force: bool,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
    Add {
        name: String,

        #[arg(short = 'r', long = "recursive", action)]
        recursive: bool,

        #[arg(long = "nonfiles", action)]
        nonfiles: bool,

        paths: Vec<PathBuf>,
    },
}

/// Shadow command executor
#[derive(Debug)]
pub struct ShadCmd {
    globals: Globals,
    executor: GatewayExecutor,
    exit_status: i32,
}

impl ShadCmd {
    fn new(globals: Globals) -> Self {
        let executor = GatewayExecutor::new(globals.dry_run, globals.verbose > 0, globals.debug);

        Self {
            globals,
            executor,
            exit_status: 0,
        }
    }

    fn run_cmd(&self, fname: &PathBuf, command: &str) -> Result<Vec<String>> {
        vnprint(&self.globals, &format!("run_cmd({:?})", fname));
        self.executor.execute(fname, command)
    }

    fn run(&mut self, paths: &[PathBuf], command: &str) -> Result<&mut Self> {
        dprint(&self.globals, "ShadCmd::run() starting");

        let mut results = HashMap::new();

        for path in paths {
            let lines = self.run_cmd(path, command)?;
            results.insert(path.clone(), lines);
        }

        // JSON output mode
        if self.globals.output_json {
            let json_output = json_to_string(&results)?;
            println!("{}", json_output);
        } else {
            // Human-readable output
            let print_filenames = results.len() > 1;

            for (path, lines) in &results {
                if print_filenames {
                    vnprint(&self.globals, &format!("##### {:?}", path));
                }
                for line in lines {
                    println!("{}", line);
                }
            }
        }

        dprint(&self.globals, "ShadCmd::run() complete");
        self.exit_status = 0;
        Ok(self)
    }
}

/// Print command tree
fn print_cmd_tree() {
    println!("Hammerspace CLI Command Tree:");
    println!("  eval - Evaluate hsscript expressions on files");
    println!("  sum - Perform fast calculations on files");
    println!("  attribute - Manage inode attributes");
    println!("  tag - Manage inode tags");
    println!("  keyword - Manage inode keywords");
    println!("  label - Manage inode labels");
    println!("  rekognition-tag - Manage rekognition tags");
    println!("  objective - Manage objectives");
    println!("  rm - Fast offloaded rm -rf");
    println!("  cp - Fast offloaded recursive copy");
    println!("  rsync - Fast offloaded recursive directory equalizer");
    println!("  collsum - Collection usage summary");
    println!("  status - System, component, task status");
    println!("  usage - Resource utilization");
    println!("  perf - Performance and operation stats");
    println!("  dump - Dump info about various items");
    println!("  keep-on-site - GNS replication sites");
}

/// Determine inheritance option from boolean flags
fn determine_inheritance(
    local: bool,
    inherited: bool,
    object: bool,
    active: bool,
    effective: bool,
    share: bool,
) -> Result<Inheritance> {
    let count = [local, inherited, object, active, effective, share]
        .iter()
        .filter(|x| **x)
        .count();

    if count > 1 {
        anyhow::bail!("Specify only one of: local, inherited, object, active, effective, share");
    }

    if local {
        Ok(Inheritance::Local)
    } else if inherited {
        Ok(Inheritance::Inherited)
    } else if object {
        Ok(Inheritance::Object)
    } else if active {
        Ok(Inheritance::Active)
    } else if effective {
        Ok(Inheritance::Effective)
    } else if share {
        Ok(Inheritance::Share)
    } else {
        Ok(Inheritance::None)
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Handle --cmd-tree
    if cli.cmd_tree {
        print_cmd_tree();
        process::exit(0);
    }

    let globals = Globals::new(cli.verbose, cli.dry_run, cli.debug, cli.output_json);

    // Global verbose startup
    vnprint(&globals, "Hammerspace CLI starting");
    dprint(&globals, &format!("globals: {:?}", globals));

    match cli.command {
        Commands::Eval(args) => {
            handle_eval(&globals, args)?;
        }

        Commands::Sum(args) => {
            handle_sum(&globals, args)?;
        }

        Commands::Attribute(args) => {
            handle_attribute(&globals, args)?;
        }

        Commands::Tag(args) => {
            handle_tag(&globals, args)?;
        }

        Commands::Keyword(args) => {
            handle_keyword(&globals, args)?;
        }

        Commands::Label(args) => {
            handle_label(&globals, args)?;
        }

        Commands::RekognitionTag(args) => {
            handle_rekognition_tag(&globals, args)?;
        }

        Commands::Objective(args) => {
            handle_objective(&globals, args)?;
        }

        Commands::Rm(args) => {
            handle_rm(&globals, args)?;
        }

        Commands::Cp(args) => {
            handle_cp(&globals, args)?;
        }

        Commands::Rsync(args) => {
            handle_rsync(&globals, args)?;
        }

        Commands::Collsum(args) => {
            handle_collsum(&globals, args)?;
        }

        Commands::Status(args) => {
            handle_status(&globals, args)?;
        }

        Commands::Usage(args) => {
            handle_usage(&globals, args)?;
        }

        Commands::Perf(args) => {
            handle_perf(&globals, args)?;
        }

        Commands::Dump(args) => {
            handle_dump(&globals, args)?;
        }

        Commands::KeepOnSite(args) => {
            handle_keep_on_site(&globals, args)?;
        }
    }

    Ok(())
}

// Metadata command handlers
fn handle_attribute(globals: &Globals, args: AttributeArgs) -> Result<i32> {
    match args.action {
        AttributeAction::List {
            recursive,
            local,
            inherited,
            object,
            unbound,
            paths,
        } => {
            let exit_code = handle_attribute_list(
                globals, &paths, recursive, local, inherited, object, unbound,
            )?;
            process::exit(exit_code);
        }
        AttributeAction::Get {
            name,
            local,
            inherited,
            object,
            unbound,
            paths,
        } => {
            let exit_code =
                handle_attribute_get(globals, &paths, &name, local, inherited, object, unbound)?;
            process::exit(exit_code);
        }
        AttributeAction::Has {
            name,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code = handle_attribute_has(globals, &paths, &name, local, inherited, object)?;
            process::exit(exit_code);
        }
        AttributeAction::Set {
            name,
            exp,
            input_json,
            string,
            unbound,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_attribute_set(
                globals, &paths, &name, &exp, input_json, string, unbound, recursive, nonfiles,
            )?;
            process::exit(exit_code);
        }
        AttributeAction::Add {
            name,
            exp,
            input_json,
            string,
            unbound,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_attribute_add(
                globals, &paths, &name, &exp, input_json, string, unbound, recursive, nonfiles,
            )?;
            process::exit(exit_code);
        }
        AttributeAction::Delete {
            name,
            force,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code =
                handle_attribute_delete(globals, &paths, &name, force, recursive, nonfiles)?;
            process::exit(exit_code);
        }
    }
}

fn handle_attribute_list(
    globals: &Globals,
    paths: &[PathBuf],
    recursive: bool,
    local: bool,
    inherited: bool,
    object: bool,
    unbound: bool,
) -> Result<i32> {
    vnprint(globals, "Attribute list command");

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_list("attribute", &eval_args, inheritance, unbound)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_attribute_get(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
    unbound: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Attribute get command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "attribute",
        ReadType::Get,
        name,
        None,
        &eval_args,
        inheritance,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_attribute_has(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Attribute has command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "attribute",
        ReadType::Has,
        name,
        None,
        &eval_args,
        inheritance,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_attribute_set(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: &str,
    input_json: bool,
    string: bool,
    unbound: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(
        globals,
        &format!("Attribute set command: {} = {}", name, exp),
    );

    let hsexp = HSExp::new(exp.to_string())
        .with_string(string)
        .with_input_json(input_json);

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "attribute",
        UpdateType::Set,
        None,
        name,
        &hsexp,
        &set_args,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_attribute_add(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: &str,
    input_json: bool,
    string: bool,
    unbound: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(
        globals,
        &format!("Attribute add command: {} = {}", name, exp),
    );

    let hsexp = HSExp::new(exp.to_string())
        .with_string(string)
        .with_input_json(input_json);

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "attribute",
        UpdateType::Add,
        None,
        name,
        &hsexp,
        &set_args,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_attribute_delete(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    force: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Attribute delete command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_delete("attribute", None, name, None, &set_args, force)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_tag(globals: &Globals, args: TagArgs) -> Result<i32> {
    match args.action {
        TagAction::List {
            recursive,
            local,
            inherited,
            object,
            unbound,
            paths,
        } => {
            let exit_code = handle_tag_list(
                globals, &paths, recursive, local, inherited, object, unbound,
            )?;
            process::exit(exit_code);
        }
        TagAction::Get {
            name,
            local,
            inherited,
            object,
            unbound,
            paths,
        } => {
            let exit_code =
                handle_tag_get(globals, &paths, &name, local, inherited, object, unbound)?;
            process::exit(exit_code);
        }
        TagAction::Has {
            name,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code = handle_tag_has(globals, &paths, &name, local, inherited, object)?;
            process::exit(exit_code);
        }
        TagAction::Set {
            name,
            exp,
            input_json,
            string,
            unbound,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_tag_set(
                globals, &paths, &name, &exp, input_json, string, unbound, recursive, nonfiles,
            )?;
            process::exit(exit_code);
        }
        TagAction::Add {
            name,
            exp,
            input_json,
            string,
            unbound,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_tag_add(
                globals, &paths, &name, &exp, input_json, string, unbound, recursive, nonfiles,
            )?;
            process::exit(exit_code);
        }
        TagAction::Delete {
            name,
            force,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_tag_delete(globals, &paths, &name, force, recursive, nonfiles)?;
            process::exit(exit_code);
        }
    }
}

fn handle_tag_list(
    globals: &Globals,
    paths: &[PathBuf],
    recursive: bool,
    local: bool,
    inherited: bool,
    object: bool,
    unbound: bool,
) -> Result<i32> {
    vnprint(globals, "Tag list command");

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_list("tag", &eval_args, inheritance, unbound)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_tag_get(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
    unbound: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Tag get command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "tag",
        ReadType::Get,
        name,
        None,
        &eval_args,
        inheritance,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_tag_has(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Tag has command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "tag",
        ReadType::Has,
        name,
        None,
        &eval_args,
        inheritance,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_tag_set(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: &str,
    input_json: bool,
    string: bool,
    unbound: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Tag set command: {} = {}", name, exp));

    let hsexp = HSExp::new(exp.to_string())
        .with_string(string)
        .with_input_json(input_json);

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "tag",
        UpdateType::Set,
        Some("tags"),
        name,
        &hsexp,
        &set_args,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_tag_add(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: &str,
    input_json: bool,
    string: bool,
    unbound: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Tag add command: {} = {}", name, exp));

    let hsexp = HSExp::new(exp.to_string())
        .with_string(string)
        .with_input_json(input_json);

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "tag",
        UpdateType::Add,
        Some("tags"),
        name,
        &hsexp,
        &set_args,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_tag_delete(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    force: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Tag delete command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_delete("tag", Some("tags"), name, None, &set_args, force)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_keyword(globals: &Globals, args: KeywordArgs) -> Result<i32> {
    match args.action {
        KeywordAction::List {
            recursive,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code =
                handle_keyword_list(globals, &paths, recursive, local, inherited, object)?;
            process::exit(exit_code);
        }
        KeywordAction::Has {
            name,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code = handle_keyword_has(globals, &paths, &name, local, inherited, object)?;
            process::exit(exit_code);
        }
        KeywordAction::Add {
            name,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_keyword_add(globals, &paths, &name, recursive, nonfiles)?;
            process::exit(exit_code);
        }
        KeywordAction::Delete {
            name,
            force,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code =
                handle_keyword_delete(globals, &paths, &name, force, recursive, nonfiles)?;
            process::exit(exit_code);
        }
    }
}

fn handle_keyword_list(
    globals: &Globals,
    paths: &[PathBuf],
    recursive: bool,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, "Keyword list command");

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_list("keyword", &eval_args, inheritance, false)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_keyword_has(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Keyword has command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "keyword",
        ReadType::Has,
        name,
        None,
        &eval_args,
        inheritance,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_keyword_add(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Keyword add command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "keyword",
        UpdateType::Add,
        Some("keywords"),
        name,
        &HSExp::new("true".to_string()).with_string(true),
        &set_args,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_keyword_delete(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    force: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Keyword delete command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_delete("keyword", Some("keywords"), name, None, &set_args, force)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_label(globals: &Globals, args: LabelArgs) -> Result<i32> {
    match args.action {
        LabelAction::List {
            recursive,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code =
                handle_label_list(globals, &paths, recursive, local, inherited, object)?;
            process::exit(exit_code);
        }
        LabelAction::Has {
            name,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code = handle_label_has(globals, &paths, &name, local, inherited, object)?;
            process::exit(exit_code);
        }
        LabelAction::Add {
            name,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_label_add(globals, &paths, &name, recursive, nonfiles)?;
            process::exit(exit_code);
        }
        LabelAction::Delete {
            name,
            force,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code =
                handle_label_delete(globals, &paths, &name, force, recursive, nonfiles)?;
            process::exit(exit_code);
        }
    }
}

fn handle_label_list(
    globals: &Globals,
    paths: &[PathBuf],
    recursive: bool,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, "Label list command");

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_list("label", &eval_args, inheritance, false)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_label_has(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Label has command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "label",
        ReadType::Has,
        name,
        None,
        &eval_args,
        inheritance,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_label_add(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Label add command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "label",
        UpdateType::Add,
        Some("assigned_labels"),
        name,
        &HSExp::new("true".to_string()).with_string(true),
        &set_args,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_label_delete(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    force: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Label delete command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_delete(
        "label",
        Some("assigned_labels"),
        name,
        None,
        &set_args,
        force,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rekognition_tag(globals: &Globals, args: RekognitionTagArgs) -> Result<i32> {
    match args.action {
        RekognitionTagAction::List {
            recursive,
            local,
            inherited,
            object,
            unbound,
            paths,
        } => {
            let exit_code = handle_rekognition_tag_list(
                globals, &paths, recursive, local, inherited, object, unbound,
            )?;
            process::exit(exit_code);
        }
        RekognitionTagAction::Get {
            name,
            local,
            inherited,
            object,
            unbound,
            paths,
        } => {
            let exit_code = handle_rekognition_tag_get(
                globals, &paths, &name, local, inherited, object, unbound,
            )?;
            process::exit(exit_code);
        }
        RekognitionTagAction::Has {
            name,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code =
                handle_rekognition_tag_has(globals, &paths, &name, local, inherited, object)?;
            process::exit(exit_code);
        }
        RekognitionTagAction::Set {
            name,
            exp,
            input_json,
            string,
            unbound,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_rekognition_tag_set(
                globals, &paths, &name, &exp, input_json, string, unbound, recursive, nonfiles,
            )?;
            process::exit(exit_code);
        }
        RekognitionTagAction::Add {
            name,
            exp,
            input_json,
            string,
            unbound,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_rekognition_tag_add(
                globals, &paths, &name, &exp, input_json, string, unbound, recursive, nonfiles,
            )?;
            process::exit(exit_code);
        }
        RekognitionTagAction::Delete {
            name,
            force,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code =
                handle_rekognition_tag_delete(globals, &paths, &name, force, recursive, nonfiles)?;
            process::exit(exit_code);
        }
    }
}

fn handle_rekognition_tag_list(
    globals: &Globals,
    paths: &[PathBuf],
    recursive: bool,
    local: bool,
    inherited: bool,
    object: bool,
    unbound: bool,
) -> Result<i32> {
    vnprint(globals, "RekognitionTag list command");

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_list("rekognition_tag", &eval_args, inheritance, unbound)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rekognition_tag_get(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
    unbound: bool,
) -> Result<i32> {
    vnprint(globals, &format!("RekognitionTag get command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "rekognition_tag",
        ReadType::Get,
        name,
        None,
        &eval_args,
        inheritance,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rekognition_tag_has(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, &format!("RekognitionTag has command: {}", name));

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "rekognition_tag",
        ReadType::Has,
        name,
        None,
        &eval_args,
        inheritance,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rekognition_tag_set(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: &str,
    input_json: bool,
    string: bool,
    unbound: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(
        globals,
        &format!("RekognitionTag set command: {} = {}", name, exp),
    );

    let hsexp = HSExp::new(exp.to_string())
        .with_string(string)
        .with_input_json(input_json);

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "rekognition_tag",
        UpdateType::Set,
        Some("rekognition_tags"),
        name,
        &hsexp,
        &set_args,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rekognition_tag_add(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: &str,
    input_json: bool,
    string: bool,
    unbound: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(
        globals,
        &format!("RekognitionTag add command: {} = {}", name, exp),
    );

    let hsexp = HSExp::new(exp.to_string())
        .with_string(string)
        .with_input_json(input_json);

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "rekognition_tag",
        UpdateType::Add,
        Some("rekognition_tags"),
        name,
        &hsexp,
        &set_args,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rekognition_tag_delete(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    force: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("RekognitionTag delete command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_delete(
        "rekognition_tag",
        Some("rekognition_tags"),
        name,
        None,
        &set_args,
        force,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_objective(globals: &Globals, args: ObjectiveArgs) -> Result<i32> {
    match args.action {
        ObjectiveAction::List {
            local,
            inherited,
            active,
            effective,
            share,
            paths,
        } => {
            let exit_code =
                handle_objective_list(globals, &paths, local, inherited, active, effective, share)?;
            process::exit(exit_code);
        }
        ObjectiveAction::Has {
            name,
            exp,
            input_json,
            local,
            inherited,
            active,
            effective,
            share,
            paths,
        } => {
            let exit_code = handle_objective_has(
                globals,
                &paths,
                &name,
                exp.as_deref(),
                input_json,
                local,
                inherited,
                active,
                effective,
                share,
            )?;
            process::exit(exit_code);
        }
        ObjectiveAction::Add {
            name,
            exp,
            input_json,
            string,
            unbound,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_objective_add(
                globals, &paths, &name, &exp, input_json, string, unbound, recursive, nonfiles,
            )?;
            process::exit(exit_code);
        }
        ObjectiveAction::Delete {
            name,
            exp,
            input_json,
            force,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_objective_delete(
                globals,
                &paths,
                &name,
                exp.as_deref(),
                input_json,
                force,
                recursive,
                nonfiles,
            )?;
            process::exit(exit_code);
        }
    }
}

fn handle_objective_list(
    globals: &Globals,
    paths: &[PathBuf],
    local: bool,
    inherited: bool,
    active: bool,
    effective: bool,
    share: bool,
) -> Result<i32> {
    vnprint(globals, "Objective list command");

    let inheritance = determine_inheritance(local, inherited, false, active, effective, share)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_list("objective", &eval_args, inheritance, false)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_objective_has(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: Option<&str>,
    input_json: bool,
    local: bool,
    inherited: bool,
    active: bool,
    effective: bool,
    share: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Objective has command: {}", name));

    let inheritance = determine_inheritance(local, inherited, false, active, effective, share)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let hsexp = if let Some(exp_str) = exp {
        HSExp::new(exp_str.to_string()).with_input_json(input_json)
    } else {
        HSExp::new("true".to_string())
    };

    let command = gen_read(
        "objective",
        ReadType::Has,
        name,
        Some(&hsexp),
        &eval_args,
        inheritance,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_objective_add(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: &str,
    input_json: bool,
    string: bool,
    unbound: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(
        globals,
        &format!("Objective add command: {} = {}", name, exp),
    );

    let hsexp = HSExp::new(exp.to_string())
        .with_string(string)
        .with_input_json(input_json);

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_update(
        "objective",
        UpdateType::Add,
        Some("objectives"),
        name,
        &hsexp,
        &set_args,
        unbound,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_objective_delete(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    exp: Option<&str>,
    input_json: bool,
    force: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Objective delete command: {}", name));

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let hsexp = if let Some(exp_str) = exp {
        HSExp::new(exp_str.to_string()).with_input_json(input_json)
    } else {
        HSExp::new("true".to_string())
    };

    let command = gen_delete(
        "objective",
        Some("objectives"),
        name,
        Some(&hsexp),
        &set_args,
        force,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rm(globals: &Globals, args: RmArgs) -> Result<i32> {
    vnprint(globals, "Rm command");

    // Check for passthrough options that trigger fallback to system rm
    let passthrough_opts = [
        args.interactive,
        args.i,
        args.interactive_value.is_some() && args.interactive_value != Some("".to_string()),
        args.one_file_system,
        args.no_preserve_root,
        args.preserve_root,
        args.dir,
        args.verbose,
    ];

    let has_passthrough = passthrough_opts.iter().any(|&x| x);

    // Fallback to system rm if passthrough options or missing required flags
    if has_passthrough || !(args.force && args.recursive) {
        vnprint(
            globals,
            "Unsupported options supplied, falling back to system rm",
        );
        let mut cmd_args = vec!["rm".to_string()];

        // Add passthrough flags
        if args.interactive {
            cmd_args.push("-i".to_string());
        }
        if args.i {
            cmd_args.push("-I".to_string());
        }
        if let Some(ref val) = args.interactive_value {
            if !val.is_empty() {
                cmd_args.push("--interactive".to_string());
                cmd_args.push(val.clone());
            }
        }
        if args.one_file_system {
            cmd_args.push("--one-file-system".to_string());
        }
        if args.no_preserve_root {
            cmd_args.push("--no-preserve-root".to_string());
        }
        if args.preserve_root {
            cmd_args.push("--preserve-root".to_string());
        }
        if args.dir {
            cmd_args.push("-d".to_string());
        }
        if args.verbose {
            cmd_args.push("-v".to_string());
        }

        // Add paths
        for path in &args.paths {
            cmd_args.push(path.display().to_string());
        }

        vnprint(globals, &format!("Calling: {}", cmd_args.join(" ")));

        if globals.dry_run {
            return Ok(0);
        }

        let status = std::process::Command::new("rm")
            .args(&cmd_args[1..])
            .status()?;

        return Ok(status.code().unwrap_or(1));
    }

    // Fast mode: use shadow command for directories
    let mut dirs = Vec::new();
    let mut others = Vec::new();

    for path in &args.paths {
        if is_directory(path) {
            dirs.push(path.clone());
        } else if path_exists(path) {
            others.push(path.clone());
        } else {
            vnprint(
                globals,
                &format!("Path not found, ignoring due to --force: {:?}", path),
            );
        }
    }

    // Execute shadow command on directories
    let mut cmd = ShadCmd::new(globals.clone());

    // Build rm_rf expression
    let hsexp = HSExp::new("rm_rf".to_string());
    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: false,
        compact: false,
        json: false,
    };
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    // Run shadow command on directories
    if !dirs.is_empty() {
        cmd.run(&dirs, &command)?;
    }

    // Unlink non-directories
    for fpath in &others {
        remove_file(fpath, globals.dry_run)?;
    }

    // Remove base directories (shadow command doesn't clean these up)
    for fpath in &dirs {
        remove_directory(fpath, globals.dry_run)?;
    }

    Ok(cmd.exit_status)
}

fn handle_cp(globals: &Globals, args: CpArgs) -> Result<i32> {
    vnprint(globals, "Cp command");

    // Check for unsupported options - fallback to system cp
    if !args.archive {
        vnprint(
            globals,
            "Unsupported options supplied, falling back to system cp",
        );
        return fallback_to_system_cp(globals, &args);
    }

    let dest = &args.dest;
    let srcs = &args.srcs;

    // Handle single source mode
    if srcs.len() == 1 {
        let src = &srcs[0];

        // Single source file - not supported in fast mode
        if !is_directory(src) {
            vnprint(
                globals,
                "Single source is not a directory, use cp --reflink for faster copy",
            );
            return fallback_to_system_cp(globals, &args);
        }

        // Check if dest exists
        if path_exists(dest) && !is_directory(dest) {
            vnprint(globals, "Destination exists but is not a directory");
            return fallback_to_system_cp(globals, &args);
        }

        // Handle single directory source
        let mut fast_sources = Vec::new();

        if !path_exists(dest) {
            // Create destination directory
            ensure_directory(dest, globals.dry_run)?;

            // Copy metadata from source parent to destination
            if src.parent().is_some() {
                if let Some(src_parent) = src.parent() {
                    copy_metadata(src_parent, dest, globals.dry_run)?;
                }
            }

            // Copy contents of src into dest
            for entry in std::fs::read_dir(src)? {
                let entry = entry?;
                fast_sources.push(src.join(entry.file_name()));
            }
        } else {
            // Dest exists and is directory - copy whole src directory as child
            fast_sources.push(src.clone());
        }

        // Perform fast copy
        perform_fast_cp(globals, &fast_sources, dest)?;
    } else {
        // Multi source mode
        if !is_directory(dest) {
            vnprint(globals, "Destination directory does not exist");
            return fallback_to_system_cp(globals, &args);
        }

        let mut fast_sources = Vec::new();

        for src in srcs {
            // Check if source exists
            if !path_exists(src) {
                vnprint(globals, &format!("Source {:?} does not exist", src));
                return fallback_to_system_cp(globals, &args);
            }

            // Check same filesystem
            if !same_filesystem(src, dest)? {
                vnprint(
                    globals,
                    &format!(
                        "Source {:?} is on different filesystem from destination",
                        src
                    ),
                );
                return fallback_to_system_cp(globals, &args);
            }

            // Check for collisions
            let entry = src.file_name().unwrap_or_else(|| src.as_os_str());
            let tgt = dest.join(entry);
            if path_exists(&tgt) {
                vnprint(
                    globals,
                    &format!(
                        "Source item \"{:?}\" collides with existing item \"{:?}\" in destination",
                        src, tgt
                    ),
                );
                return fallback_to_system_cp(globals, &args);
            }

            fast_sources.push(src.clone());
        }

        // Perform fast copy
        perform_fast_cp(globals, &fast_sources, dest)?;
    }

    // Walk tree, following any assimilation to block returning till assims are complete
    vnprint(globals, "Walking tree to wait for assimilation");
    let _ = hs_dirs_count(globals, &[dest.clone()]);

    Ok(0)
}

fn fallback_to_system_cp(globals: &Globals, args: &CpArgs) -> Result<i32> {
    vnprint(globals, "Falling back to system cp");

    let mut cmd_args = vec!["cp".to_string()];

    if args.archive {
        cmd_args.push("-a".to_string());
    }

    for src in &args.srcs {
        cmd_args.push(src.display().to_string());
    }
    cmd_args.push(args.dest.display().to_string());

    vnprint(globals, &format!("Calling: {}", cmd_args.join(" ")));

    if globals.dry_run {
        return Ok(0);
    }

    let status = std::process::Command::new("cp")
        .args(&cmd_args[1..])
        .status()?;

    Ok(status.code().unwrap_or(1))
}

fn perform_fast_cp(globals: &Globals, sources: &[PathBuf], dest: &PathBuf) -> Result<i32> {
    vnprint(
        globals,
        &format!("Performing fast cp of {:?} to {:?}", sources, dest),
    );

    // Get destination inode for shadow command
    let dest_stat = std::fs::metadata(dest)?;
    let dest_inode = dest_stat.ino();

    // Build cp_a expression
    let hsexp = HSExp::new("cp_a".to_string());
    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: false,
        compact: false,
        json: false,
    };
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    // Execute shadow command
    let mut cmd = ShadCmd::new(globals.clone());

    // Set dest_inode in kwargs
    // Note: The shadow command expects dest_inode to be passed
    // We'll need to modify the expression to include this
    let command_with_dest = format!("{}; dest_inode={}", command, dest_inode);

    cmd.run(sources, &command_with_dest)?;

    if cmd.exit_status != 0 {
        eprintln!(
            "Error {} processing offloaded cp -a of paths {:?}: Unknown error",
            cmd.exit_status, sources
        );
        eprintln!("Aborting");
        return Ok(cmd.exit_status);
    }

    Ok(0)
}

fn hs_dirs_count(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Counting directories");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: true,
        json: globals.output_json,
    };

    let exp = "1";
    let hsexp = HSExp::new(exp.to_string());
    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_rsync(globals: &Globals, args: RsyncArgs) -> Result<i32> {
    vnprint(globals, "Rsync command");

    // Validate required options
    if !args.archive || !args.delete {
        eprintln!("Must provide both --delete and --archive options, this is the only supported method for this tool, which may remove data at the destination path");
        return Ok(1);
    }

    let src = &args.src;
    let dest = &args.dest;

    // Check source type
    let src_is_file = is_file(src);
    let src_is_dir = is_directory(src);
    let src_ends_slash = src.to_string_lossy().ends_with('/');

    // Check destination type
    let dest_is_dir = is_directory(dest);
    let dest_ends_slash = dest.to_string_lossy().ends_with('/');
    let dest_exists = path_exists(dest);

    // Convert dest to absolute path
    let dest_orig = dest.clone();
    let dest = std::fs::canonicalize(&dest_orig)?;

    // Handle file source
    if src_is_file {
        let (dest_parent, dest_fname) = if !src_ends_slash
            && !dest_ends_slash
            && !src_is_dir
            && !dest_is_dir
            && src.file_name() == dest_orig.file_name()
        {
            // Assume should be file on both sides
            (
                dest.parent().unwrap_or(dest.as_path()).to_path_buf(),
                dest_orig
                    .file_name()
                    .unwrap_or(dest_orig.as_os_str())
                    .to_string_lossy()
                    .to_string(),
            )
        } else if !dest_orig.to_string_lossy().ends_with('/') && !dest_exists {
            // In this case, rsync would create a file, not a dir, don't support that
            (
                dest.parent().unwrap_or(dest.as_path()).to_path_buf(),
                dest_orig
                    .file_name()
                    .unwrap_or(dest_orig.as_os_str())
                    .to_string_lossy()
                    .to_string(),
            )
        } else if dest_ends_slash || dest_is_dir || !dest_exists {
            // Should be directory or already is directory
            (
                dest.clone(),
                src.file_name()
                    .unwrap_or(src.as_os_str())
                    .to_string_lossy()
                    .to_string(),
            )
        } else {
            // Assume it is a file that was specified
            (
                dest.parent().unwrap_or(dest.as_path()).to_path_buf(),
                dest_orig
                    .file_name()
                    .unwrap_or(dest_orig.as_os_str())
                    .to_string_lossy()
                    .to_string(),
            )
        };

        let src_fname: String = src
            .file_name()
            .unwrap_or(src.as_os_str())
            .to_string_lossy()
            .to_string();
        let src_parent = src.parent().unwrap_or(src).to_path_buf();

        // Check for renaming
        if dest_fname != src_fname {
            // Check if it's an undelete file
            if !src_fname.starts_with(&format!("{}[#D", dest_fname)) {
                eprintln!("This rsync like tool can not handle renaming a file as part of copy\nplease provide a source filename and a target directory/ (include trailing /)\nsrc filename: {}\tdest filename: {}", src.display(), dest.display());
                return Ok(1);
            }
        }

        // Create parent directory if needed
        if !path_exists(&dest_parent) {
            ensure_directory(&dest_parent, globals.dry_run)?;
            // Copy metadata from source parent to destination parent
            copy_metadata(&src_parent, &dest_parent, globals.dry_run)?;
        } else if !globals.dry_run && !is_directory(&dest_parent) {
            eprintln!(
                "Source {} is a file but unable to find destination/parent directory {}",
                src.display(),
                dest_parent.display()
            );
            return Ok(1);
        }

        let dest_tgt = dest_parent;

        // Check filesystem
        if !same_filesystem(src, &dest_tgt)? {
            eprintln!(
                "Source ({}) is on different filesystem from destination ({})",
                src.display(),
                dest_tgt.display()
            );
            return Ok(1);
        }

        // Perform fast copy
        perform_rsync_copy(globals, src, &dest_tgt)?;
    } else if src_is_dir {
        // Handle directory source
        let (_dest_parent, dest_fname): (PathBuf, Option<std::ffi::OsString>) =
            if (!src_ends_slash && !dest_ends_slash) || (!src_ends_slash && dest_ends_slash) {
                let src_name: &std::ffi::OsStr = src.file_name().unwrap_or(src.as_os_str());
                (dest.join(src_name), Some(src_name.to_os_string()))
            } else {
                (dest.clone(), None)
            };

        let dest_tgt = if let Some(fname) = dest_fname {
            dest.join(fname)
        } else {
            dest.clone()
        };

        // Create dest directory if needed
        if !path_exists(&dest) {
            ensure_directory(&dest, globals.dry_run)?;
            // Copy metadata from source parent to destination
            let src_parent = src.parent().unwrap_or(src).to_path_buf();
            copy_metadata(&src_parent, &dest, globals.dry_run)?;
        }

        // Check filesystem
        if !same_filesystem(src, &dest_tgt)? {
            eprintln!(
                "Source ({}) is on different filesystem from destination ({})",
                src.display(),
                dest_tgt.display()
            );
            return Ok(1);
        }

        // Perform fast copy
        perform_rsync_copy(globals, src, &dest_tgt)?;

        // Manually copy metadata for directory sources
        let src_parent = src.parent().unwrap_or(src).to_path_buf();
        copy_metadata(&src_parent, &dest_tgt, globals.dry_run)?;
    } else {
        eprintln!("Source {} is not a file or directory", src.display());
        return Ok(1);
    }

    // Walk tree, following any assimilation to block returning till assim is complete
    vnprint(globals, "Walking tree to wait for assimilation");
    let _ = hs_dirs_count(globals, &[dest.clone()]);

    Ok(0)
}

fn perform_rsync_copy(globals: &Globals, src: &PathBuf, dest: &PathBuf) -> Result<i32> {
    vnprint(
        globals,
        &format!("Performing rsync copy of {:?} to {:?}", src, dest),
    );

    // Get destination inode for shadow command
    let dest_stat = get_file_stats(dest)?;
    let dest_inode = dest_stat.ino;

    // Build rsync_a expression
    let hsexp = HSExp::new("rsync_a".to_string());
    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: false,
        compact: false,
        json: false,
    };
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    // Execute shadow command
    let mut cmd = ShadCmd::new(globals.clone());

    // Set dest_inode in kwargs
    // Note: The shadow command expects dest_inode to be passed
    // We'll need to modify the expression to include this
    let command_with_dest = format!("{}; dest_inode={}", command, dest_inode);

    cmd.run(&[src.clone()], &command_with_dest)?;

    if cmd.exit_status != 0 {
        eprintln!(
            "Error {} processing offloaded rsync -a --delete of path {:?}: Unknown error",
            cmd.exit_status, src
        );
        eprintln!("Aborting");
        return Ok(cmd.exit_status);
    }

    Ok(0)
}

fn handle_collsum(globals: &Globals, args: CollsumArgs) -> Result<i32> {
    vnprint(globals, "Collsum command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let exp = if let Some(collation) = &args.collation {
        format!(
            "collection_sums(\"{}\")[SUMMATION(\"{}\")]",
            args.collection, collation
        )
    } else {
        format!("collection_sums(\"{}\")", args.collection)
    };

    let hsexp = HSExp::new(exp);
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&args.paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_status(globals: &Globals, args: StatusArgs) -> Result<i32> {
    match args.action {
        StatusAction::Assimilation { paths } => {
            let exit_code = handle_assimilation(globals, &paths)?;
            process::exit(exit_code);
        }
        StatusAction::Csi { paths } => {
            let exit_code = handle_csi(globals, &paths)?;
            process::exit(exit_code);
        }
        StatusAction::Collections { paths } => {
            let exit_code = handle_collections(globals, &paths)?;
            process::exit(exit_code);
        }
        StatusAction::Errors { dump, paths } => {
            let exit_code = handle_errors(globals, dump, &paths)?;
            process::exit(exit_code);
        }
        StatusAction::Open { paths } => {
            let exit_code = handle_open(globals, &paths)?;
            process::exit(exit_code);
        }
        StatusAction::Replication { paths } => {
            let exit_code = handle_replication(globals, &paths)?;
            process::exit(exit_code);
        }
        StatusAction::Sweeper { paths } => {
            let exit_code = handle_sweeper(globals, &paths)?;
            process::exit(exit_code);
        }
        StatusAction::Volume { paths } => {
            let exit_code = handle_volume(globals, &paths)?;
            process::exit(exit_code);
        }
    }
}

fn handle_assimilation(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Assimilation status command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = expression::gen_eval(
        &expression::HSExp::new("assimilation_details".to_string()),
        &eval_args,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_csi(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "CSI status command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = expression::gen_eval(
        &expression::HSExp::new("attributes.csi_details".to_string()),
        &eval_args,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_collections(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Collections status command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = expression::gen_eval(
        &expression::HSExp::new("collections".to_string()),
        &eval_args,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_errors(globals: &Globals, dump: bool, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Errors status command");

    if dump {
        let sum_args = expression::SumArgs {
            raw: false,
            compact: false,
            nonfiles: false,
            json: globals.output_json,
        };

        let exp = "(IS_FILE AND ERRORS)?SUMS_TABLE{|KEY=ERRORS,|VALUE={1FILE,SPACE_USED,TOP10_TABLE{{space_used,dpath}}}}";
        let hsexp = HSExp::new(exp.to_string());
        let command = expression::gen_sum(&hsexp, &sum_args)?;

        let mut cmd = ShadCmd::new(globals.clone());
        cmd.run(paths, &command)?;
        Ok(cmd.exit_status)
    } else {
        let eval_args = expression::EvalArgs {
            recursive: true,
            nonfiles: false,
            raw: false,
            compact: false,
            json: globals.output_json,
        };

        let exp = "IS_FILE and errors!=0?dump_inode";
        let hsexp = HSExp::new(exp.to_string());
        let command = expression::gen_eval(&hsexp, &eval_args)?;

        let mut cmd = ShadCmd::new(globals.clone());
        cmd.run(paths, &command)?;
        Ok(cmd.exit_status)
    }
}

fn handle_open(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Open files status command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = expression::gen_eval(
        &expression::HSExp::new(
            "(IS_FILE AND IS_OPEN)?{1FILE,SPACE_USED,TOP10_TABLE{{space_used,dpath}}}".to_string(),
        ),
        &eval_args,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_replication(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Replication status command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = expression::gen_eval(
        &expression::HSExp::new("replication_details".to_string()),
        &eval_args,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_sweeper(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Sweeper status command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = expression::gen_eval(
        &expression::HSExp::new("sweep_details".to_string()),
        &eval_args,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_volume(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Volume status command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = expression::gen_eval(
        &expression::HSExp::new("storage_volumes".to_string()),
        &eval_args,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage(globals: &Globals, args: UsageArgs) -> Result<i32> {
    match args.action {
        UsageAction::Alignment { top_files, paths } => {
            let exit_code = handle_usage_alignment(globals, top_files, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::VirusScan { top_files, paths } => {
            let exit_code = handle_usage_virus_scan(globals, top_files, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::Owner { top_files, paths } => {
            let exit_code = handle_usage_owner(globals, top_files, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::Online { paths } => {
            let exit_code = handle_usage_online(globals, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::Volume {
            top_files,
            deep,
            paths,
        } => {
            let exit_code = handle_usage_volume(globals, top_files, deep, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::User { details, paths } => {
            let exit_code = handle_usage_user(globals, details, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::Objectives { paths } => {
            let exit_code = handle_usage_objectives(globals, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::MimeTags { paths } => {
            let exit_code = handle_usage_mime_tags(globals, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::RekognitionTags { paths } => {
            let exit_code = handle_usage_rekognition_tags(globals, &paths)?;
            process::exit(exit_code);
        }
        UsageAction::Dirs { paths } => {
            let exit_code = handle_usage_dirs(globals, &paths)?;
            process::exit(exit_code);
        }
    }
}

fn handle_usage_alignment(globals: &Globals, top_files: bool, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage alignment command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = if top_files {
        "IS_FILE?SUMS_TABLE{|KEY=OVERALL_ALIGNMENT,|VALUE={1FILE,SPACE_USED,TOP10_TABLE{{space_used,dpath}}}"
    } else {
        "IS_FILE?SUMS_TABLE{|KEY=OVERALL_ALIGNMENT,|VALUE=1}"
    };

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_virus_scan(globals: &Globals, top_files: bool, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage virus-scan command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = if top_files {
        "IS_FILE?SUMS_TABLE{|KEY=ATTRIBUTES.VIRUS_SCAN,|VALUE={1FILE,SPACE_USED,TOP10_TABLE{{space_used,dpath}}}"
    } else {
        "IS_FILE?SUMS_TABLE{|KEY=ATTRIBUTES.VIRUS_SCAN,|VALUE=1}"
    };

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_owner(globals: &Globals, top_files: bool, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage owner command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = if top_files {
        "IS_FILE?SUMS_TABLE{|KEY=OWNER,|VALUE={1FILE,SPACE_USED,TOP10_TABLE{{space_used,dpath}}}"
    } else {
        "IS_FILE?SUMS_TABLE{|KEY=OWNER,|VALUE=1}"
    };

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_online(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage online command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = "IS_ONLINE?{1FILE,SPACE_USED,TOP10_TABLE{{space_used,DPATH}}}";

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_volume(
    globals: &Globals,
    top_files: bool,
    deep: bool,
    paths: &[PathBuf],
) -> Result<i32> {
    vnprint(globals, "Usage volume command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = if top_files {
        "IS_FILE?ROWS(INSTANCES)?SUMS_TABLE{|::KEY=INSTANCES[ROW].VOLUME,|::VALUE={1FILE,INSTANCES[ROW].SPACE_USED,TOP10_TABLE{{space_used,dpath}}}[ROWS(INSTANCES)]:SUMS_TABLE{|KEY=#EMPTY,|::VALUE={1FILE, SPACE_USED, TOP10_TABLE{{space_used,dpath}}}"
    } else if deep {
        "IS_FILE?ROWS(INSTANCES)?SUMS_TABLE{|::KEY=INSTANCES[ROW].VOLUME,|::VALUE={1FILE,INSTANCES[ROW].SPACE_USED,TOP100_TABLE{{space_used,dpath}}}[ROWS(INSTANCES)]:SUMS_TABLE{|KEY=#EMPTY,|::VALUE={1FILE, SPACE_USED, TOP100_TABLE{{space_used,dpath}}}"
    } else {
        "IS_FILE?ROWS(INSTANCES)?SUMS_TABLE{|::KEY=INSTANCES[ROW].VOLUME,|::VALUE=1}[ROWS(INSTANCES)]:SUMS_TABLE{|KEY=#EMPTY,|::VALUE=1}"
    };

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_user(globals: &Globals, details: bool, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage user command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = if details {
        "IS_FILE?SUMS_TABLE{|KEY={OWNER,OWNER_GROUP},|VALUE={1FILE,SPACE_USED,TOP10_TABLE{{space_used,dpath}}}"
    } else {
        "IS_FILE?SUMS_TABLE{|KEY={OWNER,OWNER_GROUP},|VALUE=SPACE_USED}"
    };

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_objectives(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage objectives command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = "IS_FILE?SUMS_TABLE{|::KEY=LIST_OBJECTIVES_ACTIVE[ROW],|::VALUE={1FILE,SPACE_USED,TOP10_TABLE{{space_used,dpath}}}[ROWS(LIST_OBJECTIVES_ACTIVE)]";

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_mime_tags(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage mime-tags command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = "IS_FILE?SUMS_TABLE{attributes.mime.string,{1FILE,SPACE_USED,TOP10_TABLE{{SPACE_USED,DPATH}}}";

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_rekognition_tags(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage rekognition-tags command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = "IS_FILE?ISTABLE(LIST_REKOGNITION_TAGS)?SUMS_TABLE{|::KEY=LIST_REKOGNITION_TAGS()[ROW].NAME,|::VALUE={1FILE,TOP10_TABLE{{LIST_REKOGNITION_TAGS()[ROW].value,dpath}}}[ROWS(LIST_REKOGNITION_TAGS())]";

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_usage_dirs(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Usage dirs command");

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: true,
        json: globals.output_json,
    };

    let exp = "1";

    let hsexp = HSExp::new(exp.to_string());

    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_perf(globals: &Globals, args: PerfArgs) -> Result<i32> {
    match args.action {
        PerfAction::Clear { paths } => {
            let exit_code = handle_perf_clear(globals, &paths)?;
            process::exit(exit_code);
        }
        PerfAction::TopCalls { paths } => {
            let exit_code = handle_perf_top_calls(globals, &paths)?;
            process::exit(exit_code);
        }
        PerfAction::TopFuncs { op, paths } => {
            let exit_code = handle_perf_top_funcs(globals, op.as_deref(), &paths)?;
            process::exit(exit_code);
        }
        PerfAction::TopOps { paths } => {
            let exit_code = handle_perf_top_ops(globals, &paths)?;
            process::exit(exit_code);
        }
        PerfAction::Flushes { paths } => {
            let exit_code = handle_perf_flushes(globals, &paths)?;
            process::exit(exit_code);
        }
    }
}

fn handle_perf_clear(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Perf clear command");

    let stats_files_map = create_stats_files(paths, globals.dry_run)?;
    let stats_files: Vec<PathBuf> = stats_files_map.values().cloned().collect();

    let set_args = SetArgs {
        recursive: false,
        nonfiles: false,
    };

    let hsexp = HSExp::new("fs_stats.op_stats".to_string());
    let command = gen_update(
        "tag",
        UpdateType::Set,
        Some("tags"),
        "old_stats",
        &hsexp,
        &set_args,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&stats_files, &command)?;
    Ok(cmd.exit_status)
}

fn handle_perf_top_calls(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Perf top-calls command");

    let stats_files_map = create_stats_files(paths, globals.dry_run)?;
    let stats_files: Vec<PathBuf> = stats_files_map.values().cloned().collect();

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let exp = "{(fs_stats.op_stats-get-tag(\"old_stats\")),TOP100_TABLE{|::KEY={#A[PARENT.ROW].op_count,#A[PARENT.ROW].name,#A[PARENT.ROW].op_count,#A[PARENT.ROW].op_time,#A[PARENT.ROW].op_avg}}[ROWS(#A)]}.#B";

    let hsexp = HSExp::new(exp.to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&stats_files, &command)?;
    Ok(cmd.exit_status)
}

fn handle_perf_top_funcs(globals: &Globals, op: Option<&str>, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, &format!("Perf top-funcs command (op: {:?})", op));

    let stats_files_map = create_stats_files(paths, globals.dry_run)?;
    let stats_files: Vec<PathBuf> = stats_files_map.values().cloned().collect();

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let op_filter = op.unwrap_or("all");
    let exp = format!("{{(FS_STATS.OP_STATS-get-tag(\"old_stats\"))[|NAME=\"{}\"].func_stats,TOP100_TABLE{{|::KEY={{#A[PARENT.ROW].op_time,#A[PARENT.ROW].name,#A[PARENT.ROW].op_count,#A[PARENT.ROW].op_avg}}[ROWS(#A)]}}.#B", op_filter);

    let hsexp = HSExp::new(exp);
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&stats_files, &command)?;
    Ok(cmd.exit_status)
}

fn handle_perf_top_ops(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Perf top-ops command");

    let stats_files_map = create_stats_files(paths, globals.dry_run)?;
    let stats_files: Vec<PathBuf> = stats_files_map.values().cloned().collect();

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let exp = "{(fs_stats.op_stats-get-tag(\"old_stats\")),TOP100_TABLE{|::KEY={#A[PARENT.ROW].op_time,#A[PARENT.ROW].name,#A[PARENT.ROW].op_count,#A[PARENT.ROW].op_time,#A[PARENT.ROW].op_avg}}[ROWS(#A)]}.#B";

    let hsexp = HSExp::new(exp.to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&stats_files, &command)?;
    Ok(cmd.exit_status)
}

fn handle_perf_flushes(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Perf flushes command");

    let stats_files_map = create_stats_files(paths, globals.dry_run)?;
    let stats_files: Vec<PathBuf> = stats_files_map.values().cloned().collect();

    let sum_args = expression::SumArgs {
        raw: false,
        compact: false,
        nonfiles: false,
        json: globals.output_json,
    };

    let exp = "sum({|::#A=(fs_stats.op_stats-get-tag(\"old_stats\"))[ROW].flush_count}[ROWS(fs_stats.op_stats)])";

    let hsexp = HSExp::new(exp.to_string());
    let command = expression::gen_sum(&hsexp, &sum_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&stats_files, &command)?;
    Ok(cmd.exit_status)
}

fn handle_dump(globals: &Globals, args: DumpArgs) -> Result<i32> {
    match args.action {
        DumpAction::Inode { full, paths } => {
            let exit_code = handle_dump_inode(globals, full, &paths)?;
            process::exit(exit_code);
        }
        DumpAction::Iinfo { paths } => {
            let exit_code = handle_dump_iinfo(globals, &paths)?;
            process::exit(exit_code);
        }
        DumpAction::Share {
            filter_volume,
            paths,
        } => {
            let exit_code = handle_dump_share(globals, filter_volume.as_deref(), &paths)?;
            process::exit(exit_code);
        }
        DumpAction::Misaligned { paths } => {
            let exit_code = handle_dump_misaligned(globals, &paths)?;
            process::exit(exit_code);
        }
        DumpAction::Threat { paths } => {
            let exit_code = handle_dump_threat(globals, &paths)?;
            process::exit(exit_code);
        }
        DumpAction::MapFileToObj { bucket_name, paths } => {
            let exit_code = handle_dump_map_file_to_obj(globals, &bucket_name, &paths)?;
            process::exit(exit_code);
        }
        DumpAction::FilesOnVolume { volume_name, paths } => {
            let exit_code = handle_dump_files_on_volume(globals, &volume_name, &paths)?;
            process::exit(exit_code);
        }
        DumpAction::Volumes { path } => {
            let exit_code = handle_dump_volumes(globals, &path)?;
            process::exit(exit_code);
        }
        DumpAction::VolumeGroups { path } => {
            let exit_code = handle_dump_volume_groups(globals, &path)?;
            process::exit(exit_code);
        }
        DumpAction::Objectives { path } => {
            let exit_code = handle_dump_objectives(globals, &path)?;
            process::exit(exit_code);
        }
    }
}

fn handle_dump_inode(globals: &Globals, full: bool, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Dump inode command");

    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: true,
        compact: false,
        json: globals.output_json,
    };

    let exp = if full { "THIS" } else { "DUMP_INODE" };

    let hsexp = HSExp::new(exp.to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_iinfo(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Dump iinfo command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let hsexp = HSExp::new("INODE_INFO".to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_share(
    globals: &Globals,
    filter_volume: Option<&str>,
    paths: &[PathBuf],
) -> Result<i32> {
    vnprint(
        globals,
        &format!("Dump share command (filter: {:?})", filter_volume),
    );

    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: true,
        compact: false,
        json: globals.output_json,
    };

    let exp = if let Some(volume) = filter_volume {
        format!("dump_inode_on(storage_volume(\"{}\"))", volume)
    } else {
        "DUMP_INODE".to_string()
    };

    let hsexp = HSExp::new(exp);
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_misaligned(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Dump misaligned command");

    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let exp = "IS_FILE and overall_alignment!=alignment(\"aligned\")?dump_inode";

    let hsexp = HSExp::new(exp.to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_threat(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Dump threat command");

    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let exp = "IS_FILE and attributes.virus_scan==virus_scan_state(\"THREAT\")?dump_inode";

    let hsexp = HSExp::new(exp.to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_map_file_to_obj(
    globals: &Globals,
    bucket_name: &str,
    paths: &[PathBuf],
) -> Result<i32> {
    vnprint(
        globals,
        &format!("Dump map_file_to_obj command (bucket: {})", bucket_name),
    );

    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let exp = format!(
        "{{instances[|volume=storage_volume(\"{}\")],!ISNA(#A)?{{PATH,#A.PATH}}}}.#B",
        bucket_name
    );

    let hsexp = HSExp::new(exp);
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_files_on_volume(
    globals: &Globals,
    volume_name: &str,
    paths: &[PathBuf],
) -> Result<i32> {
    vnprint(
        globals,
        &format!("Dump files_on_volume command (volume: {})", volume_name),
    );

    let eval_args = expression::EvalArgs {
        recursive: true,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let exp = format!(
        "{{instances[|volume=storage_volume(\"{}\")],!ISNA(#A)?{{PATH}}}}.#B",
        volume_name
    );

    let hsexp = HSExp::new(exp);
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_volumes(globals: &Globals, path: &PathBuf) -> Result<i32> {
    vnprint(globals, "Dump volumes command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: true,
    };

    let hsexp = HSExp::new("STORAGE_VOLUMES".to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&[path.clone()], &command)?;

    // Parse JSON and filter out removed volumes
    if !globals.dry_run {
        let results = cmd.executor.execute(path, &command)?;
        let json_str = results.join("");
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
            if let Some(table) = json_value.get("STORAGE_VOLUMES_TABLE") {
                if let Some(rows) = table.as_array() {
                    let volumes: Vec<String> = rows
                        .iter()
                        .filter_map(|row| {
                            if let Some(status) = row.get("VOLUME_STATUS") {
                                if let Some(hammerscript) = status.get("HAMMERSCRIPT") {
                                    if hammerscript.as_str()
                                        != Some("STORAGE_VOLUME_STATUS('REMOVED')")
                                    {
                                        row.get("NAME")
                                            .and_then(|n| n.as_str())
                                            .map(|s| s.to_string())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                    if globals.output_json {
                        println!("{}", json_to_string(&volumes)?);
                    } else {
                        for vol in &volumes {
                            println!("{}", vol);
                        }
                    }
                }
            }
        }
    }

    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_volume_groups(globals: &Globals, path: &PathBuf) -> Result<i32> {
    vnprint(globals, "Dump volume_groups command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: true,
    };

    let hsexp = HSExp::new("VOLUME_GROUPS.NAME".to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&[path.clone()], &command)?;

    // Parse JSON and extract volume group names
    if !globals.dry_run {
        let results = cmd.executor.execute(path, &command)?;
        let json_str = results.join("");
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
            if let Some(table) = json_value.get("VOLUME_GROUPS_TABLE") {
                if let Some(rows) = table.as_array() {
                    let vgs: Vec<String> = rows
                        .iter()
                        .filter_map(|row| {
                            row.get("NAME")
                                .and_then(|n| n.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect();

                    if globals.output_json {
                        println!("{}", json_to_string(&vgs)?);
                    } else {
                        for vg in &vgs {
                            println!("{}", vg);
                        }
                    }
                }
            }
        }
    }

    flush_stdout();
    Ok(cmd.exit_status)
}

fn handle_dump_objectives(globals: &Globals, path: &PathBuf) -> Result<i32> {
    vnprint(globals, "Dump objectives command");

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: true,
    };

    let hsexp = HSExp::new("SMART_OBJECTIVES.NAME".to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&[path.clone()], &command)?;

    // Parse JSON and extract objective names (filter out deleted objectives)
    if !globals.dry_run {
        let results = cmd.executor.execute(path, &command)?;
        let json_str = results.join("");
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
            if let Some(table) = json_value.get("SMART_OBJECTIVES_TABLE") {
                if let Some(rows) = table.as_array() {
                    let objs: Vec<String> = rows
                        .iter()
                        .filter_map(|row| {
                            if let Some(name) = row.get("NAME") {
                                if let Some(name_str) = name.as_str() {
                                    // Filter out deleted objectives (start with __z_objective)
                                    if !name_str.starts_with("__z_objective") {
                                        Some(name_str.to_string())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                    if globals.output_json {
                        println!("{}", json_to_string(&objs)?);
                    } else {
                        for obj in &objs {
                            println!("{}", obj);
                        }
                    }
                }
            }
        }
    }

    flush_stdout();
    Ok(cmd.exit_status)
}

/// Helper function to get GNS participant site names
fn get_gns_participant_site_names(globals: &Globals, path: &PathBuf) -> Result<Vec<String>> {
    vnprint(
        globals,
        &format!("Getting GNS participant site names for {:?}", path),
    );

    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: true,
    };

    let hsexp = HSExp::new("THIS.PARTICIPANTS".to_string());
    let command = expression::gen_eval(&hsexp, &eval_args)?;

    let executor = GatewayExecutor::new(globals.dry_run, globals.verbose > 0, globals.debug);
    let results = executor.execute(path, &command)?;
    let json_str = results.join("");

    if globals.dry_run {
        // Return dummy site names for dry run
        return Ok(vec![
            "dry_run_test_site1".to_string(),
            "dry_run_test_site2".to_string(),
        ]);
    }

    // Parse JSON response
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
        if let Some(participants_table) = json_value.get("PARTICIPANTS_TABLE") {
            if let Some(rows) = participants_table.as_array() {
                let mut site_names = Vec::new();
                for row in rows {
                    if let Some(site_name) = row.get("SITE_NAME").and_then(|n| n.as_str()) {
                        site_names.push(site_name.to_string());
                    }
                }
                return Ok(site_names);
            }
        }
    }

    // If parsing fails, return an error instead of silently returning empty
    anyhow::bail!("Failed to parse GNS participant sites response")
}

fn handle_keep_on_site(globals: &Globals, args: KeepOnSiteArgs) -> Result<i32> {
    match args.action {
        KeepOnSiteAction::Available { paths } => {
            let exit_code = handle_keep_on_site_available(globals, &paths)?;
            process::exit(exit_code);
        }
        KeepOnSiteAction::List {
            recursive,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code =
                handle_keep_on_site_list(globals, &paths, recursive, local, inherited, object)?;
            process::exit(exit_code);
        }
        KeepOnSiteAction::Has {
            name,
            local,
            inherited,
            object,
            paths,
        } => {
            let exit_code =
                handle_keep_on_site_has(globals, &paths, &name, local, inherited, object)?;
            process::exit(exit_code);
        }
        KeepOnSiteAction::Delete {
            name,
            force,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code =
                handle_keep_on_site_delete(globals, &paths, &name, force, recursive, nonfiles)?;
            process::exit(exit_code);
        }
        KeepOnSiteAction::Add {
            name,
            recursive,
            nonfiles,
            paths,
        } => {
            let exit_code = handle_keep_on_site_add(globals, &paths, &name, recursive, nonfiles)?;
            process::exit(exit_code);
        }
    }
}

fn handle_keep_on_site_available(globals: &Globals, paths: &[PathBuf]) -> Result<i32> {
    vnprint(globals, "Keep-on-site available command");

    // Get participant site names from first path
    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("No path provided"))?;
    let site_names = get_gns_participant_site_names(globals, path)?;

    if globals.output_json {
        println!("{}", json_to_string(&site_names)?);
    } else {
        for site in &site_names {
            println!("{}", site);
        }
    }

    Ok(0)
}

fn handle_keep_on_site_list(
    globals: &Globals,
    paths: &[PathBuf],
    recursive: bool,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, "Keep-on-site list command");

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_list("keep_on_site", &eval_args, inheritance, false)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_keep_on_site_has(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    local: bool,
    inherited: bool,
    object: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Keep-on-site has command: {}", name));

    // Validate that the site name exists in participant sites
    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("No path provided"))?;
    let site_names = get_gns_participant_site_names(globals, path)?;

    if !site_names.contains(&name.to_string()) {
        anyhow::bail!("'{}' is not a valid site name", name);
    }

    let inheritance = determine_inheritance(local, inherited, object, false, false, false)?;
    let eval_args = expression::EvalArgs {
        recursive: false,
        nonfiles: false,
        raw: false,
        compact: false,
        json: globals.output_json,
    };

    let command = gen_read(
        "keep_on_site",
        ReadType::Has,
        name,
        None,
        &eval_args,
        inheritance,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_keep_on_site_delete(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    force: bool,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Keep-on-site delete command: {}", name));

    // Validate that the site name exists in participant sites
    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("No path provided"))?;
    let site_names = get_gns_participant_site_names(globals, path)?;

    if !site_names.contains(&name.to_string()) {
        anyhow::bail!("'{}' is not a valid site name", name);
    }

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let command = gen_delete("keep_on_site", Some("keep_on_sites"), name, None, &set_args, force)?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_keep_on_site_add(
    globals: &Globals,
    paths: &[PathBuf],
    name: &str,
    recursive: bool,
    nonfiles: bool,
) -> Result<i32> {
    vnprint(globals, &format!("Keep-on-site add command: {}", name));

    // Validate that the site name exists in participant sites
    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("No path provided"))?;
    let site_names = get_gns_participant_site_names(globals, path)?;

    if !site_names.contains(&name.to_string()) {
        anyhow::bail!("'{}' is not a valid site name", name);
    }

    let set_args = SetArgs {
        recursive,
        nonfiles,
    };

    let hsexp = HSExp::new("true".to_string()).with_string(true);
    let command = gen_update(
        "keep_on_site",
        UpdateType::Add,
        Some("keep_on_sites"),
        name,
        &hsexp,
        &set_args,
        false,
    )?;

    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(paths, &command)?;
    Ok(cmd.exit_status)
}

fn handle_eval(globals: &Globals, args: EvalArgs) -> Result<()> {
    vnprint(globals, "Eval command");
    dprint(globals, &format!("eval args: {:?}", args));

    if args.exp.is_none() && !args.exp_stdin && !args.interactive {
        bail!("Must provide expression (-e, -i, --interactive) to eval command");
    }

    let exp = if args.exp_stdin {
        let mut exp_str = String::new();
        std::io::stdin().read_line(&mut exp_str)?;
        exp_str.trim().to_string()
    } else if let Some(exp) = args.exp {
        exp
    } else {
        bail!("No expression provided");
    };

    let hsexp = HSExp::new(exp)
        .with_string(args.string)
        .with_input_json(args.input_json);

    let eval_args = expression::EvalArgs {
        recursive: args.recursive,
        nonfiles: args.nonfiles,
        raw: args.raw,
        compact: args.compact,
        json: globals.output_json,
    };

    let command = gen_eval(&hsexp, &eval_args)?;
    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&args.paths, &command)?;
    process::exit(cmd.exit_status);
}

fn handle_sum(globals: &Globals, args: SumArgs) -> Result<()> {
    vnprint(globals, "Sum command");
    dprint(globals, &format!("sum args: {:?}", args));

    let exp = args
        .exp
        .ok_or_else(|| anyhow::anyhow!("Must provide expression (-e) to sum command"))?;

    let hsexp = HSExp::new(exp);

    let sum_args = expression::SumArgs {
        raw: args.raw,
        compact: args.compact,
        nonfiles: args.nonfiles,
        json: globals.output_json,
    };

    let command = gen_sum(&hsexp, &sum_args)?;
    let mut cmd = ShadCmd::new(globals.clone());
    cmd.run(&args.paths, &command)?;
    process::exit(cmd.exit_status);
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway::{execute_on_paths, GatewayExecutor, MockGatewayExecutor};
    use std::path::Path;

    #[test]
    fn test_execute_on_paths_with_command_generator() {
        let executor = GatewayExecutor::new(false, true, false);
        let paths = vec![
            PathBuf::from("/tmp/test1"),
            PathBuf::from("/tmp/test2"),
        ];

        let results = execute_on_paths(&paths, |_| Ok("eval 1+1".to_string()), &executor).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].1.len() > 0);
        assert!(results[1].1.len() > 0);
    }

    #[test]
    fn test_mock_gateway_executor_unbound_expressions() {
        let executor = MockGatewayExecutor::new(false, false, true);

        let hsexp = HSExp::new("SIZE>33KB".to_string())
            .with_string(true)
            .with_unbound(true);

        let result = executor.execute(Path::new("/tmp/test"), &hsexp.to_string()).unwrap();
        assert!(result.len() > 0);
    }
}
