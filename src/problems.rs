//! What counts as wrong in the cluster, shared by the alerts and `!problems`.

use k8s_openapi::api::core::v1::{ContainerStatus, Pod};
use kube::api::{DynamicObject, ListParams};

use crate::cluster::Cluster;
use crate::commands::kyverno;
use crate::markdown::{MAX_LIST_BYTES, bullets};

/// Something wrong with an object, as `find` sees it.
pub struct Problem {
    /// Tells it apart from the object's other problems, e.g. the policy and rule.
    pub key: String,
    pub line: String,
}

/// Every problem in the cluster right now.
pub async fn current(cluster: &Cluster) -> anyhow::Result<Vec<Problem>> {
    let reports = cluster.list_policy_reports().await?;
    let pods = cluster.pods().list(&ListParams::default()).await?;
    let reasons = &cluster.pod_stuck_reasons;
    let failures = reports.iter().flat_map(policy_failures);
    let stuck = pods
        .items
        .iter()
        .flat_map(|pod| stuck_containers(pod, reasons));
    Ok(failures.chain(stuck).collect())
}

/// One message listing `lines` under `title`.
pub fn summary(title: &str, lines: &[String]) -> String {
    format!(
        "**{title}:** {}\n\n{}",
        lines.len(),
        bullets(lines, MAX_LIST_BYTES)
    )
}

/// The failing policies of a PolicyReport.
pub fn policy_failures(report: &DynamicObject) -> Vec<Problem> {
    kyverno::failures(report)
        .iter()
        .map(|failure| Problem {
            key: format!("{}/{}", failure.policy, failure.rule),
            line: format!("🛡️ **Kyverno:** {}", failure.line()),
        })
        .collect()
}

/// The containers of a pod that were OOM-killed or wait for one of `reasons`.
pub fn stuck_containers(pod: &Pod, reasons: &[String]) -> Vec<Problem> {
    let Some(status) = &pod.status else {
        return Vec::new();
    };
    let namespace = pod.metadata.namespace.as_deref().unwrap_or_default();
    let name = pod.metadata.name.as_deref().unwrap_or_default();
    let containers = status.container_statuses.iter().flatten();
    let init_containers = status.init_container_statuses.iter().flatten();
    containers
        .chain(init_containers)
        .filter_map(|container| {
            let reason = stuck_reason(container, reasons)?;
            let limit = match reason {
                "OOMKilled" => memory_limit(pod, &container.name)
                    .map(|limit| format!(", memory limit `{limit}`"))
                    .unwrap_or_default(),
                _ => String::new(),
            };
            Some(Problem {
                key: format!("{}/{reason}", container.name),
                line: format!(
                    "💥 **Pod:** `{namespace}/{name}` container `{}`: {reason}{limit}",
                    container.name
                ),
            })
        })
        .collect()
}

/// An OOM kill wins: it's the cause of the crash loop that usually follows.
fn stuck_reason<'a>(container: &'a ContainerStatus, reasons: &[String]) -> Option<&'a str> {
    let oom_killed = [&container.state, &container.last_state]
        .iter()
        .filter_map(|state| state.as_ref()?.terminated.as_ref()?.reason.as_deref())
        .any(|reason| reason == "OOMKilled");
    if oom_killed {
        return Some("OOMKilled");
    }
    let waiting = container
        .state
        .as_ref()?
        .waiting
        .as_ref()?
        .reason
        .as_deref()?;
    reasons
        .iter()
        .any(|reason| reason == waiting)
        .then_some(waiting)
}

fn memory_limit<'a>(pod: &'a Pod, container: &str) -> Option<&'a str> {
    let spec = pod.spec.as_ref()?;
    let init_containers = spec.init_containers.iter().flatten();
    let found = spec
        .containers
        .iter()
        .chain(init_containers)
        .find(|spec| spec.name == container)?;
    let limits = found.resources.as_ref()?.limits.as_ref()?;
    Some(&limits.get("memory")?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_stuck_containers() {
        let pod = |app: serde_json::Value, sidecar: serde_json::Value| -> Pod {
            serde_json::from_value(serde_json::json!({
                "metadata": { "name": "reports-1", "namespace": "kyverno", "uid": "p" },
                "spec": { "containers": [
                    { "name": "app", "resources": { "limits": { "memory": "128Mi" } } },
                    { "name": "sidecar" },
                ] },
                "status": { "containerStatuses": [
                    { "name": "app", "image": "", "imageID": "", "ready": false, "restartCount": 3, "lastState": app },
                    { "name": "sidecar", "image": "", "imageID": "", "ready": false, "restartCount": 0, "state": sidecar },
                ] },
            }))
            .unwrap()
        };
        let reasons = ["ImagePullBackOff".to_owned()];
        let lines = |pod: &Pod| -> Vec<String> {
            stuck_containers(pod, &reasons)
                .into_iter()
                .map(|problem| problem.line)
                .collect()
        };
        let oom = serde_json::json!({ "terminated": { "exitCode": 137, "reason": "OOMKilled" } });
        let waiting = |reason| serde_json::json!({ "waiting": { "reason": reason } });

        assert_eq!(
            lines(&pod(oom, waiting("ImagePullBackOff"))),
            [
                "💥 **Pod:** `kyverno/reports-1` container `app`: OOMKilled, memory limit `128Mi`",
                "💥 **Pod:** `kyverno/reports-1` container `sidecar`: ImagePullBackOff",
            ]
        );
        let healthy = serde_json::json!({ "terminated": { "exitCode": 0, "reason": "Completed" } });
        assert!(lines(&pod(healthy, waiting("ContainerCreating"))).is_empty());
    }
}
