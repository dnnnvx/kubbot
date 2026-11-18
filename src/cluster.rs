//! What the ops commands and alerts need from the Kubernetes cluster.

use std::fmt::Debug;

use anyhow::Context;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams, LogParams};
use matrix_sdk::ruma::{OwnedRoomId, RoomId};
use serde::de::DeserializeOwned;

use crate::config::Config;

/// Pods that can still be stuck: one that ran to completion can't. Failed ones stay, e.g. an OOM-killed Job.
pub const UNFINISHED_PODS: &str = "status.phase!=Succeeded";
/// Objects per page when listing, so a big cluster comes in pieces instead of one huge response.
const PAGE_SIZE: u32 = 500;

#[derive(Clone)]
pub struct Cluster {
    pub kube: kube::Client,
    pub dashboard_url: String,
    pub ops_room: OwnedRoomId,
    ci_namespace: String,
    ci_log_lines: i64,
    pub tofu_namespace: String,
    pub tofu_terraform: String,
    pub kubectl_context: String,
    pub pod_stuck_reasons: Vec<String>,
}

impl Cluster {
    pub async fn connect(config: &Config) -> anyhow::Result<Self> {
        Ok(Self {
            kube: kube::Client::try_default()
                .await
                .context("no Kubernetes config")?,
            dashboard_url: config.tekton_dashboard_url.clone(),
            ops_room: config.ops_room.clone(),
            ci_namespace: config.tekton_namespace.clone(),
            ci_log_lines: config.ci_log_lines,
            tofu_namespace: config.tofu_namespace.clone(),
            tofu_terraform: config.tofu_terraform.clone(),
            kubectl_context: config.kubectl_context.clone(),
            pod_stuck_reasons: config.pod_stuck_reasons.clone(),
        })
    }

    pub fn pipeline_runs(&self) -> Api<DynamicObject> {
        self.tekton("PipelineRun", "pipelineruns")
    }

    fn tekton(&self, kind: &str, plural: &str) -> Api<DynamicObject> {
        let gvk = GroupVersionKind::gvk("tekton.dev", "v1", kind);
        self.custom(&gvk, plural, &self.ci_namespace)
    }

    /// A custom resource, which has no typed API in k8s-openapi.
    pub fn custom(
        &self,
        gvk: &GroupVersionKind,
        plural: &str,
        namespace: &str,
    ) -> Api<DynamicObject> {
        let resource = ApiResource::from_gvk_with_plural(gvk, plural);
        Api::namespaced_with(self.kube.clone(), namespace, &resource)
    }

    /// Kyverno's results for namespaced resources, in every namespace.
    pub fn policy_reports(&self) -> Api<DynamicObject> {
        self.reports("PolicyReport", "policyreports")
    }

    /// Kyverno's results for cluster-scoped resources.
    pub fn cluster_policy_reports(&self) -> Api<DynamicObject> {
        self.reports("ClusterPolicyReport", "clusterpolicyreports")
    }

    /// What `find` sees in Kyverno's results, namespaced and cluster-scoped.
    pub async fn find_in_policy_reports<T>(
        &self,
        find: impl Fn(&DynamicObject) -> Vec<T>,
    ) -> kube::Result<Vec<T>> {
        let params = ListParams::default();
        let mut found = find_in(&self.policy_reports(), params.clone(), &find).await?;
        found.extend(find_in(&self.cluster_policy_reports(), params, &find).await?);
        Ok(found)
    }

    /// Every pod of the cluster.
    pub fn pods(&self) -> Api<Pod> {
        Api::all(self.kube.clone())
    }

    fn reports(&self, kind: &str, plural: &str) -> Api<DynamicObject> {
        let gvk = GroupVersionKind::gvk("wgpolicyk8s.io", "v1alpha2", kind);
        let resource = ApiResource::from_gvk_with_plural(&gvk, plural);
        Api::all_with(self.kube.clone(), &resource)
    }

    /// The last log lines of the step that failed the run.
    pub async fn failure_logs(&self, run: &DynamicObject) -> anyhow::Result<String> {
        let selector = format!("tekton.dev/pipelineRun={}", run_name(run));
        let task_runs = self
            .tekton("TaskRun", "taskruns")
            .list(&ListParams::default().labels(&selector))
            .await?;
        let step = task_runs
            .items
            .iter()
            .filter(|task_run| matches!(run_state(task_run), RunState::Failed(_)))
            .find_map(failed_step)
            .context("no failed step found")?;
        let params = LogParams {
            container: Some(step.container.to_owned()),
            tail_lines: Some(self.ci_log_lines),
            ..LogParams::default()
        };
        let pods = Api::<Pod>::namespaced(self.kube.clone(), &self.ci_namespace);
        Ok(pods.logs(step.pod, &params).await?)
    }

    pub fn ensure_ops_room(&self, room: &RoomId) -> anyhow::Result<()> {
        anyhow::ensure!(room == self.ops_room, "only available in the ops room");
        Ok(())
    }
}

/// What `find` sees in everything `api` lists. The API server pages the list:
/// each page is checked as it comes, so only what `find` returns is held.
pub async fn find_in<K, T>(
    api: &Api<K>,
    params: ListParams,
    find: impl Fn(&K) -> Vec<T>,
) -> kube::Result<Vec<T>>
where
    K: Clone + DeserializeOwned + Debug,
{
    let mut params = params.limit(PAGE_SIZE);
    let mut found = Vec::new();
    loop {
        let page = api.list(&params).await?;
        found.extend(page.items.iter().flat_map(&find));
        match page.metadata.continue_ {
            Some(token) if !token.is_empty() => params = params.continue_token(&token),
            _ => return Ok(found),
        }
    }
}

pub struct Condition<'a> {
    pub status: &'a str,
    pub reason: &'a str,
    pub message: &'a str,
}

/// The `status.conditions` entry of the given type.
pub fn condition<'a>(object: &'a DynamicObject, kind: &str) -> Option<Condition<'a>> {
    let conditions = object.data.pointer("/status/conditions")?.as_array()?;
    let found = conditions.iter().find(|entry| entry["type"] == kind)?;
    Some(Condition {
        status: found["status"].as_str().unwrap_or_default(),
        reason: found["reason"].as_str().unwrap_or_default(),
        message: found["message"].as_str().unwrap_or_default(),
    })
}

/// Where a PipelineRun or TaskRun is, from its `Succeeded` condition.
pub enum RunState<'a> {
    /// Also a run with no condition yet: it hasn't been picked up, but it isn't finished either.
    Running,
    Succeeded,
    /// Stopped on purpose, so it doesn't count as a failure. With the reason.
    Cancelled(&'a str),
    Failed(Condition<'a>),
}

pub fn run_state(run: &DynamicObject) -> RunState<'_> {
    let Some(succeeded) = condition(run, "Succeeded") else {
        return RunState::Running;
    };
    match (succeeded.status, succeeded.reason) {
        ("Unknown", _) => RunState::Running,
        ("True", _) => RunState::Succeeded,
        ("False", reason) if reason != "Cancelled" => RunState::Failed(succeeded),
        (_, reason) => RunState::Cancelled(reason),
    }
}

#[derive(Debug, PartialEq)]
struct FailedStep<'a> {
    pod: &'a str,
    container: &'a str,
}

/// A TaskRun's first step that exited non-zero.
fn failed_step(task_run: &DynamicObject) -> Option<FailedStep<'_>> {
    let status = &task_run.data["status"];
    let steps = status["steps"].as_array()?;
    let failed = steps.iter().find(|step| {
        step["terminated"]["exitCode"]
            .as_i64()
            .is_some_and(|code| code != 0)
    })?;
    Some(FailedStep {
        pod: status["podName"].as_str()?,
        container: failed["container"].as_str()?,
    })
}

pub fn run_name(run: &DynamicObject) -> &str {
    run.metadata.name.as_deref().unwrap_or_default()
}

/// Markdown link to the run in the Tekton dashboard.
pub fn run_link(dashboard_url: &str, run: &DynamicObject) -> String {
    let name = run_name(run);
    let namespace = run.metadata.namespace.as_deref().unwrap_or_default();
    let url = dashboard_url.trim_end_matches('/');
    format!("[{name}]({url}/#/namespaces/{namespace}/pipelineruns/{name})")
}

#[cfg(test)]
pub fn test_run(name: &str, status: &str, reason: &str) -> DynamicObject {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "tekton.dev/v1",
        "kind": "PipelineRun",
        "metadata": { "name": name, "namespace": "tekton-ci", "uid": name },
        "status": {
            "startTime": format!("2026-10-30T10:00:0{}Z", name.len() % 10),
            "completionTime": "2026-10-30T11:00:00Z",
            "conditions": [{
                "type": "Succeeded",
                "status": status,
                "reason": reason,
                "message": "Tasks Completed: 2 (Failed: 1, Cancelled 0), Skipped: 2",
            }],
        },
    }))
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_name(run: &DynamicObject) -> &'static str {
        match run_state(run) {
            RunState::Running => "running",
            RunState::Succeeded => "succeeded",
            RunState::Cancelled(_) => "cancelled",
            RunState::Failed(_) => "failed",
        }
    }

    #[test]
    fn classifies_runs() {
        let cases = [
            ("False", "Failed", "failed"),
            ("False", "PipelineRunTimeout", "failed"),
            ("False", "Cancelled", "cancelled"),
            ("Unknown", "Running", "running"),
            ("True", "Succeeded", "succeeded"),
        ];
        for (status, reason, expected) in cases {
            let run = test_run("a", status, reason);
            assert_eq!(state_name(&run), expected, "{status} {reason}");
        }

        let mut new = test_run("a", "Unknown", "Running");
        new.data = serde_json::json!({});
        assert_eq!(state_name(&new), "running", "no condition yet");
    }

    #[test]
    fn finds_the_failed_step() {
        let task_run: DynamicObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "tekton.dev/v1",
            "kind": "TaskRun",
            "metadata": { "name": "build-1-test" },
            "status": {
                "podName": "build-1-test-pod",
                "steps": [
                    { "container": "step-fetch", "terminated": { "exitCode": 0 } },
                    { "container": "step-test", "terminated": { "exitCode": 101 } },
                    { "container": "step-push", "terminated": { "exitCode": 1 } },
                ],
            },
        }))
        .unwrap();
        assert_eq!(
            failed_step(&task_run),
            Some(FailedStep {
                pod: "build-1-test-pod",
                container: "step-test"
            })
        );

        let mut running = task_run;
        running.data["status"]["steps"] =
            serde_json::json!([{ "container": "step-fetch", "running": {} }]);
        assert_eq!(failed_step(&running), None);
    }

    #[test]
    fn parses_hash_room_ids() {
        let room: OwnedRoomId = "!hcNOO-kFitB4fJ15jq8k5Ju5H56mGzLYXIIdbuhJvXM"
            .parse()
            .unwrap();
        assert_eq!(
            room.as_str(),
            "!hcNOO-kFitB4fJ15jq8k5Ju5H56mGzLYXIIdbuhJvXM"
        );
    }

    #[tokio::test]
    async fn lists_page_by_page() {
        use std::convert::Infallible;

        use kube::client::Body;
        use serde_json::json;

        // A fake API server with two pages, the second asked for with the first's token.
        let server = tower::service_fn(|request: http::Request<Body>| async move {
            let query = request.uri().query().unwrap_or_default().to_owned();
            assert!(query.contains("limit=500"), "{query}");
            let (names, next) = match query.contains("continue=page-2") {
                true => (vec!["c"], ""),
                false => (vec!["a", "b"], "page-2"),
            };
            let items: Vec<_> = names
                .iter()
                .map(|name| json!({ "metadata": { "name": name } }))
                .collect();
            let list = json!({ "metadata": { "continue": next }, "items": items });
            let body = Body::from(serde_json::to_vec(&list).unwrap());
            Ok::<_, Infallible>(http::Response::new(body))
        });
        let pods = Api::<Pod>::all(kube::Client::new(server, "default"));
        let found = find_in(&pods, ListParams::default(), |pod: &Pod| {
            let name = pod.metadata.name.clone().unwrap();
            if name == "b" { vec![] } else { vec![name] }
        })
        .await
        .unwrap();
        assert_eq!(found, ["a", "c"]);
    }

    #[test]
    fn links_to_the_dashboard() {
        assert_eq!(
            run_link(
                "https://tekton.example/",
                &test_run("build-1", "True", "Succeeded")
            ),
            "[build-1](https://tekton.example/#/namespaces/tekton-ci/pipelineruns/build-1)"
        );
    }
}
