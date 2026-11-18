use kube::api::DynamicObject;

use super::{Command, Ctx, Reply};
use crate::markdown::{MAX_LIST_BYTES, bullets};

pub const COMMAND: Command = Command {
    name: "kyverno",
    usage: "!kyverno",
    summary: "policies failing in Kyverno's reports",
    ops_only: true,
    run: |ctx| Box::pin(run(ctx)),
};

async fn run(ctx: Ctx) -> Reply {
    Ok(render(&ctx.cluster.list_policy_reports().await?))
}

fn render(reports: &[DynamicObject]) -> String {
    let lines: Vec<String> = reports
        .iter()
        .flat_map(failures)
        .map(|failure| failure.line())
        .collect();
    if lines.is_empty() {
        return "**Kyverno:** no failing policies.".to_owned();
    }
    format!(
        "**Kyverno:** {} failing\n\n{}",
        lines.len(),
        bullets(&lines, MAX_LIST_BYTES)
    )
}

/// A `fail` result of a PolicyReport, on the resource the report is about.
pub struct Failure<'a> {
    pub policy: &'a str,
    pub rule: &'a str,
    message: &'a str,
    report: &'a DynamicObject,
}

impl Failure<'_> {
    pub fn line(&self) -> String {
        let scope = &self.report.data["scope"];
        let field = |name: &str| scope[name].as_str().unwrap_or_default();
        let resource = match field("namespace") {
            "" => field("name").to_owned(),
            namespace => format!("{namespace}/{}", field("name")),
        };
        let policy = if self.rule == self.policy {
            self.policy.to_owned()
        } else {
            format!("{}/{}", self.policy, self.rule)
        };
        format!(
            "`{policy}` on {} `{resource}`: {}",
            field("kind"),
            self.message
        )
    }
}

pub fn failures(report: &DynamicObject) -> Vec<Failure<'_>> {
    let results = report.data["results"].as_array();
    results
        .into_iter()
        .flatten()
        .filter(|result| result["result"] == "fail")
        .map(|result| Failure {
            policy: result["policy"].as_str().unwrap_or_default(),
            rule: result["rule"].as_str().unwrap_or_default(),
            message: result["message"].as_str().unwrap_or_default(),
            report,
        })
        .collect()
}

#[cfg(test)]
pub fn test_report(uid: &str, results: &serde_json::Value) -> DynamicObject {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "wgpolicyk8s.io/v1alpha2",
        "kind": "PolicyReport",
        "metadata": { "name": uid, "namespace": "piraeus", "uid": uid },
        "scope": { "kind": "ReplicaSet", "namespace": "piraeus", "name": "linstor-54997" },
        "results": results,
    }))
    .unwrap()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn lists_failures_only() {
        let report = test_report(
            "a",
            &json!([
                { "policy": "signed", "rule": "signed", "result": "fail", "message": "no signatures" },
                { "policy": "labels", "rule": "team", "result": "fail", "message": "missing" },
                { "policy": "limits", "rule": "limits", "result": "pass", "message": "ok" },
            ]),
        );
        assert_eq!(
            render(&[report]),
            "**Kyverno:** 2 failing\n\n\
             - `signed` on ReplicaSet `piraeus/linstor-54997`: no signatures\n\
             - `labels/team` on ReplicaSet `piraeus/linstor-54997`: missing"
        );
        assert_eq!(
            render(&[test_report("b", &json!([]))]),
            "**Kyverno:** no failing policies."
        );
    }
}
