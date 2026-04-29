// src/expression.rs
// Expression handling infrastructure for Hammerspace CLI
// Based on hstk/hsscript.py

use anyhow::{bail, Result};

/// Unicode character mappings for special characters in filenames
pub const UCHARS: &[(char, &str)] = &[
    // Add character mappings as needed
    // Example: ('/', "\u{2215}"), // Use division slash instead of forward slash
];

/// Shadow descriptor prefix
pub const SHADESC: &str = "?.";

/// Hammerspace Expression
/// Represents an expression that can be evaluated by Hammerspace
#[derive(Debug, Clone)]
pub struct HSExp {
    pub exp: String,
    pub string: bool,
    pub input_json: bool,
    pub unbound: bool,
}

impl HSExp {
    /// Create a new HSExp
    pub fn new(exp: String) -> Self {
        Self {
            exp,
            string: false,
            input_json: false,
            unbound: false,
        }
    }

    /// Set string mode
    pub fn with_string(mut self, string: bool) -> Self {
        self.string = string;
        self
    }

    /// Set input_json mode
    pub fn with_input_json(mut self, input_json: bool) -> Self {
        self.input_json = input_json;
        self
    }

    /// Set unbound mode
    pub fn with_unbound(mut self, unbound: bool) -> Self {
        self.unbound = unbound;
        self
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        let mut ret = if self.unbound {
            format!("EXPRESSION({})", self.exp)
        } else {
            self.exp.clone()
        };

        if self.string {
            ret = format!("\"{}\"", ret);
        }

        ret
    }
}

impl std::fmt::Display for HSExp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Clean a string by remapping special characters to Unicode equivalents
pub fn clean_str(value: &str) -> String {
    let mut ret = value.to_string();
    for (c, u) in UCHARS {
        ret = ret.replace(*c, u);
    }
    format!("{}{}", SHADESC, ret)
}

/// Default arguments for eval commands
#[derive(Debug, Clone, Default)]
pub struct EvalArgs {
    pub recursive: bool,
    pub nonfiles: bool,
    pub raw: bool,
    pub compact: bool,
    pub json: bool,
}

/// Default arguments for sum commands
#[derive(Debug, Clone, Default)]
pub struct SumArgs {
    pub raw: bool,
    pub compact: bool,
    pub nonfiles: bool,
    pub json: bool,
}

/// Default arguments for set commands
#[derive(Debug, Clone, Default)]
pub struct SetArgs {
    pub recursive: bool,
    pub nonfiles: bool,
}

/// Inheritance options for metadata operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inheritance {
    None,
    Local,
    Inherited,
    Object,
    Active,
    Effective,
    Share,
}

/// Build eval command string based on options
pub fn build_eval(args: &EvalArgs) -> Result<String> {
    let mut ret = "eval".to_string();

    if args.compact && args.raw {
        bail!("Select only one of compact / raw");
    }

    if args.compact {
        ret += "_compact";
    } else if args.raw {
        ret += "_raw";
    }

    if args.nonfiles {
        ret += "_rec_nofiles";
    } else if args.recursive {
        ret += "_rec";
    }

    if args.json {
        ret += "_json";
    }

    Ok(ret)
}

/// Build sum command string based on options
pub fn build_sum(args: &SumArgs) -> Result<String> {
    let mut ret = "sum".to_string();

    if args.compact && args.raw {
        bail!("Select only one of compact / raw");
    }

    if args.compact {
        ret += "_compact";
    } else if args.raw {
        ret += "_raw";
    }

    if args.nonfiles {
        ret += "_nofiles";
    }

    if args.json {
        ret += "_json";
    }

    Ok(ret)
}

/// Build set command string based on options
pub fn build_set(args: &SetArgs) -> String {
    let mut ret = "set".to_string();

    if args.nonfiles {
        ret += "_rec_nofiles";
    } else if args.recursive {
        ret += "_rec";
    }

    ret
}

/// Build inheritance suffix based on inheritance option
pub fn build_inheritance(inheritance: Inheritance) -> Result<String> {
    match inheritance {
        Inheritance::None => Ok(String::new()),
        Inheritance::Local => Ok("_local".to_string()),
        Inheritance::Inherited => Ok("_inherited".to_string()),
        Inheritance::Object => Ok("_object".to_string()),
        Inheritance::Active => Ok("_active".to_string()),
        Inheritance::Effective => Ok("_effective".to_string()),
        Inheritance::Share => Ok("_share".to_string()),
    }
}

/// Validate that only one inheritance option is set
pub fn validate_inheritance(_inheritance: Inheritance) -> Result<()> {
    // Inheritance is an enum, so it can only be one value
    // This is a no-op but kept for API compatibility
    Ok(())
}

/// Generate list function command
pub fn gen_list(
    mdtype: &str,
    eval_args: &EvalArgs,
    inheritance: Inheritance,
    unbound: bool,
) -> Result<String> {
    validate_inheritance(inheritance)?;
    let eval_cmd = build_eval(eval_args)?;
    let inherit_suffix = build_inheritance(inheritance)?;
    let unbound_suffix = if unbound { "_unbound" } else { "" };

    let cmd = format!(
        "{} list_{}s{}{}",
        eval_cmd, mdtype, inherit_suffix, unbound_suffix
    );

    Ok(clean_str(&cmd))
}

/// Generate get/has function command
pub fn gen_read(
    mdtype: &str,
    read_type: ReadType,
    name: &str,
    value: Option<&HSExp>,
    eval_args: &EvalArgs,
    inheritance: Inheritance,
    unbound: bool,
) -> Result<String> {
    validate_inheritance(inheritance)?;

    if read_type == ReadType::Has && unbound {
        bail!("unbound only allowed on get, not on has");
    }

    let eval_cmd = build_eval(eval_args)?;
    let inherit_suffix = build_inheritance(inheritance)?;
    let unbound_suffix = if read_type == ReadType::Get && unbound {
        "_unbound"
    } else {
        ""
    };

    let mut cmd = format!(
        "{} {}_{}{}{}",
        eval_cmd,
        read_type.as_str(),
        mdtype,
        inherit_suffix,
        unbound_suffix
    );

    cmd += "(";
    cmd += &format!("\"{}\"", name);

    if let Some(val) = value {
        if val.input_json {
            cmd += &format!(", EXPRESSION_FROM_JSON({})", val);
        } else {
            cmd += &format!(", EXPRESSION_FROM_TEXT({})", val);
        }
    }

    cmd += ")";

    Ok(clean_str(&cmd))
}

/// Read operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadType {
    Get,
    Has,
}

impl ReadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadType::Get => "get",
            ReadType::Has => "has",
        }
    }
}

/// Update operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateType {
    Set,
    Add,
}

impl UpdateType {
    pub fn as_str(&self) -> &'static str {
        match self {
            UpdateType::Set => "set",
            UpdateType::Add => "add",
        }
    }
}

/// Generate set/add function command
pub fn gen_update(
    mdtype: &str,
    update_type: UpdateType,
    table: Option<&str>,
    name: &str,
    value: &HSExp,
    set_args: &SetArgs,
    unbound: bool,
) -> Result<String> {
    if update_type == UpdateType::Add && unbound {
        bail!("unbound only allowed on set, not on add");
    }

    let set_cmd = build_set(set_args);
    let mut cmd;

    if mdtype == "attribute" {
        // Special case for attributes
        cmd = format!("{} {} ", set_cmd, name);
        if value.input_json {
            cmd += "_json";
        }
        cmd += "=";
        cmd += &value.to_string();
    } else {
        let table =
            table.ok_or_else(|| anyhow::anyhow!("table required for non-attribute types"))?;
        cmd = format!("{} #{}={}_{}", set_cmd, table, update_type.as_str(), mdtype);
        cmd += "(";
        cmd += &format!("\"{}\"", name);

        if update_type == UpdateType::Set || mdtype == "objective" {
            let val_unbound = unbound || mdtype == "objective";
            let val = value.clone().with_unbound(val_unbound).with_string(true);

            if val.input_json {
                cmd += &format!(", EXPRESSION_FROM_JSON({})", val);
            } else {
                cmd += &format!(", EXPRESSION_FROM_TEXT({})", val);
            }
        }

        cmd += ")";
    }

    Ok(clean_str(&cmd))
}

/// Generate delete function command
pub fn gen_delete(
    mdtype: &str,
    table: Option<&str>,
    name: &str,
    value: Option<&HSExp>,
    set_args: &SetArgs,
    force: bool,
) -> Result<String> {
    let set_cmd = build_set(set_args);
    let mut cmd = format!("{} ", set_cmd);

    if mdtype == "attribute" {
        // Special case for attributes
        cmd += &format!("{}=#EMPTY", name);
    } else {
        let table =
            table.ok_or_else(|| anyhow::anyhow!("table required for non-attribute types"))?;
        let delete_type = if force { "delete_force" } else { "delete" };
        cmd += &format!("#{}={}_{}", table, delete_type, mdtype);
        cmd += "(";
        cmd += &format!("\"{}\"", name);

        if mdtype == "objective" {
            let default_val = HSExp::new("true".to_string());
            let val_ref = value.unwrap_or(&default_val);
            let val = val_ref.clone().with_unbound(true).with_string(true);

            if val.input_json {
                cmd += &format!(", EXPRESSION_FROM_JSON({})", val);
            } else {
                cmd += &format!(", EXPRESSION_FROM_TEXT({})", val);
            }
        }

        cmd += ")";
    }

    Ok(clean_str(&cmd))
}

/// Generate eval command with expression
pub fn gen_eval(exp: &HSExp, eval_args: &EvalArgs) -> Result<String> {
    let eval_cmd = build_eval(eval_args)?;

    let cmd = if exp.input_json {
        format!("{} EVAL(EXPRESSION_FROM_JSON('{}'))", eval_cmd, exp)
    } else {
        format!("{} {}", eval_cmd, exp)
    };

    Ok(clean_str(&cmd))
}

/// Generate sum command with expression
pub fn gen_sum(exp: &HSExp, sum_args: &SumArgs) -> Result<String> {
    let sum_cmd = build_sum(sum_args)?;

    let cmd = if exp.input_json {
        format!("{} EVAL(EXPRESSION_FROM_JSON('{}'))", sum_cmd, exp)
    } else {
        format!("{} {}", sum_cmd, exp)
    };

    Ok(clean_str(&cmd))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hsexp_basic() {
        let exp = HSExp::new("1+1".to_string());
        assert_eq!(exp.to_string(), "1+1");
    }

    #[test]
    fn test_hsexp_string() {
        let exp = HSExp::new("test".to_string()).with_string(true);
        assert_eq!(exp.to_string(), "\"test\"");
    }

    #[test]
    fn test_hsexp_unbound() {
        let exp = HSExp::new("SIZE>1GB".to_string()).with_unbound(true);
        assert_eq!(exp.to_string(), "EXPRESSION(SIZE>1GB)");
    }

    #[test]
    fn test_build_eval() {
        let args = EvalArgs {
            recursive: true,
            ..Default::default()
        };
        let cmd = build_eval(&args).unwrap();
        assert_eq!(cmd, "eval_rec");
    }

    #[test]
    fn test_build_sum() {
        let args = SumArgs {
            raw: true,
            ..Default::default()
        };
        let cmd = build_sum(&args).unwrap();
        assert_eq!(cmd, "sum_raw");
    }

    #[test]
    fn test_build_set() {
        let args = SetArgs {
            recursive: true,
            ..Default::default()
        };
        let cmd = build_set(&args);
        assert_eq!(cmd, "set_rec");
    }

    #[test]
    fn test_clean_str() {
        let result = clean_str("test");
        assert_eq!(result, "?.test");
    }

    #[test]
    fn test_gen_list() {
        let eval_args = EvalArgs::default();
        let cmd = gen_list("attribute", &eval_args, Inheritance::None, false).unwrap();
        assert_eq!(cmd, "?.eval list_attributes");
    }

    #[test]
    fn test_gen_read_get() {
        let eval_args = EvalArgs::default();
        let cmd = gen_read(
            "attribute",
            ReadType::Get,
            "myattr",
            None,
            &eval_args,
            Inheritance::None,
            false,
        )
        .unwrap();
        assert_eq!(cmd, "?.eval get_attribute(\"myattr\")");
    }

    #[test]
    fn test_gen_read_has() {
        let eval_args = EvalArgs::default();
        let cmd = gen_read(
            "tag",
            ReadType::Has,
            "mytag",
            None,
            &eval_args,
            Inheritance::None,
            false,
        )
        .unwrap();
        assert_eq!(cmd, "?.eval has_tag(\"mytag\")");
    }

    #[test]
    fn test_gen_update_set() {
        let set_args = SetArgs::default();
        let value = HSExp::new("value".to_string()).with_string(true);
        let cmd = gen_update(
            "tag",
            UpdateType::Set,
            Some("tags"),
            "mytag",
            &value,
            &set_args,
            false,
        )
        .unwrap();
        assert!(cmd.contains("set #tags=set_tag"));
        assert!(cmd.contains("\"mytag\""));
        assert!(cmd.contains("EXPRESSION_FROM_TEXT"));
    }

    #[test]
    fn test_gen_delete() {
        let set_args = SetArgs::default();
        let cmd = gen_delete("tag", Some("tags"), "mytag", None, &set_args, false).unwrap();
        assert_eq!(cmd, "?.set #tags=delete_tag(\"mytag\")");
    }

    #[test]
    fn test_gen_eval() {
        let eval_args = EvalArgs::default();
        let exp = HSExp::new("1+1".to_string());
        let cmd = gen_eval(&exp, &eval_args).unwrap();
        assert_eq!(cmd, "?.eval 1+1");
    }

    #[test]
    fn test_gen_sum() {
        let sum_args = SumArgs::default();
        let exp = HSExp::new("SUMS_TABLE{TYPE,1}".to_string());
        let cmd = gen_sum(&exp, &sum_args).unwrap();
        assert_eq!(cmd, "?.sum SUMS_TABLE{TYPE,1}");
    }

    #[test]
    fn test_compact_raw_error() {
        let args = EvalArgs {
            compact: true,
            raw: true,
            ..Default::default()
        };
        assert!(build_eval(&args).is_err());
    }
}
