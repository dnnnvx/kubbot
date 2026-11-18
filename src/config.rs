//! Environment configuration, read and validated once at startup.
//!
//! Every variable can also be given as `<NAME>_FILE` pointing at a file holding
//! the value (Docker/Kubernetes secrets); the file wins over the plain variable.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs};

use anyhow::Context;
use matrix_sdk::ruma::OwnedRoomId;

pub struct Config {
    pub homeserver: String,
    pub data_dir: PathBuf,
    pub store_passphrase: String,
    /// Only needed on first start, when there's no saved session yet.
    pub user: Option<String>,
    pub password: Option<String>,
    /// Keeps a copy of the cross-signing keys on the server when set.
    pub recovery_passphrase: Option<String>,
    pub health_addr: SocketAddr,
    pub tekton_dashboard_url: String,
    /// Where the PipelineRuns and their pods are.
    pub tekton_namespace: String,
    /// How many log lines of a failed CI step are posted.
    pub ci_log_lines: i64,
    pub tofu_namespace: String,
    /// The tofu-controller `Terraform` object whose plans are shown.
    pub tofu_terraform: String,
    /// Put in the `kubectl` command that applies a plan.
    pub kubectl_context: String,
    /// Where cluster commands are allowed and CI alerts go.
    pub ops_room: OwnedRoomId,
    /// How often the pending tofu plan is checked.
    pub tofu_poll: Duration,
    /// A CI run tracked this long is forgotten, in case its end was never seen.
    pub tekton_forget_after: Duration,
    /// A Kyverno failure or stuck pod that flaps is posted at most once per this.
    pub alert_repost_after: Duration,
    /// Waiting reasons that make a container stuck. An OOM kill always counts.
    pub pod_stuck_reasons: Vec<String>,
    /// Areas switched off: their commands, alerts and watches don't run.
    pub disabled: Vec<Feature>,
}

/// An area of the bot that can be switched off, with the cluster access it needs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Feature {
    /// `!ci` and CI run posts.
    Tekton,
    /// `!tofu` and pending plan posts.
    Tofu,
    /// `!kyverno`, policy failure posts and their part of `!problems`.
    Kyverno,
    /// Stuck pod posts and their part of `!problems`.
    Pods,
}

impl Feature {
    fn parse(name: &str) -> anyhow::Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "tekton" => Ok(Self::Tekton),
            "tofu" => Ok(Self::Tofu),
            "kyverno" => Ok(Self::Kyverno),
            "pods" => Ok(Self::Pods),
            _ => anyhow::bail!(
                "DISABLED_FEATURES: unknown `{name}`, use tekton, tofu, kyverno, pods"
            ),
        }
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let disabled: Vec<Feature> = list("DISABLED_FEATURES", "")?
            .iter()
            .map(|name| Feature::parse(name))
            .collect::<anyhow::Result<_>>()?;
        let tekton_dashboard_url = match disabled.contains(&Feature::Tekton) {
            true => String::new(),
            false => var("TEKTON_DASHBOARD_URL")?.context("TEKTON_DASHBOARD_URL is required")?,
        };
        Ok(Self {
            homeserver: var("MATRIX_HOMESERVER")?.context("MATRIX_HOMESERVER is required")?,
            data_dir: var("DATA_DIR")?.unwrap_or_else(|| "/data".into()).into(),
            store_passphrase: var("STORE_PASSPHRASE")?.context("STORE_PASSPHRASE is required")?,
            user: var("MATRIX_USER")?,
            password: var("MATRIX_PASSWORD")?,
            recovery_passphrase: var("RECOVERY_PASSPHRASE")?,
            health_addr: var("HEALTH_ADDR")?
                .as_deref()
                .unwrap_or("0.0.0.0:8080")
                .parse()
                .context("HEALTH_ADDR must be <ip>:<port>")?,
            tekton_dashboard_url,
            tekton_namespace: var("TEKTON_NAMESPACE")?.unwrap_or_else(|| "tekton-ci".into()),
            ci_log_lines: positive("CI_LOG_LINES", 30)?
                .try_into()
                .context("CI_LOG_LINES is too big")?,
            tofu_namespace: var("TOFU_NAMESPACE")?.unwrap_or_else(|| "flux-system".into()),
            tofu_terraform: var("TOFU_TERRAFORM")?.unwrap_or_else(|| "production".into()),
            kubectl_context: var("KUBECTL_CONTEXT")?.unwrap_or_else(|| "homelab".into()),
            ops_room: var("OPS_ROOM")?
                .context("OPS_ROOM is required")?
                .parse()
                .context("OPS_ROOM must be a room ID")?,
            tofu_poll: Duration::from_secs(60 * positive("TOFU_POLL_MINUTES", 30)?),
            tekton_forget_after: Duration::from_secs(3600 * positive("TEKTON_GC_HOURS", 24)?),
            alert_repost_after: Duration::from_secs(3600 * positive("ALERT_REPOST_HOURS", 24)?),
            pod_stuck_reasons: list(
                "POD_STUCK_REASONS",
                "CrashLoopBackOff,ImagePullBackOff,CreateContainerConfigError",
            )?,
            disabled,
        })
    }
}

fn positive(name: &str, default: u64) -> anyhow::Result<u64> {
    let Some(value) = var(name)? else {
        return Ok(default);
    };
    value
        .parse()
        .ok()
        .filter(|n| *n > 0)
        .with_context(|| format!("{name} must be a whole number above 0"))
}

/// A comma-separated list, blanks dropped.
fn list(name: &str, default: &str) -> anyhow::Result<Vec<String>> {
    let value = var(name)?;
    let items = value.as_deref().unwrap_or(default).split(',');
    Ok(items
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect())
}

fn var(name: &str) -> anyhow::Result<Option<String>> {
    let file_var = format!("{name}_FILE");
    if let Ok(path) = env::var(&file_var) {
        let value = fs::read_to_string(&path).with_context(|| format!("{file_var}={path}"))?;
        return Ok(Some(value.trim_end_matches(['\r', '\n']).to_owned()));
    }
    Ok(env::var(name).ok().filter(|v| !v.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_lists_and_drops_blanks() {
        let items = list("TEST_UNSET_LIST", " CrashLoopBackOff, ,ImagePullBackOff,").unwrap();
        assert_eq!(items, ["CrashLoopBackOff", "ImagePullBackOff"]);
    }

    #[test]
    fn parses_features() {
        assert_eq!(Feature::parse("Kyverno").unwrap(), Feature::Kyverno);
        assert!(Feature::parse("flux").is_err());
    }
}
