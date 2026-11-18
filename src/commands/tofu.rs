use std::fmt::Write;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::Api;
use kube::api::{DynamicObject, GroupVersionKind};

use super::{Command, Ctx, Reply};
use crate::cluster::{Cluster, condition};
use crate::config::Feature;
use crate::markdown::{code_block, head};

pub const COMMAND: Command = Command {
    name: "tofu",
    usage: "!tofu",
    summary: "pending tofu plan, and the command to apply it",
    ops_only: true,
    feature: Some(Feature::Tofu),
    run: |ctx| Box::pin(run(ctx)),
};

/// An event caps at 65 KB and holds the text twice, plain and as escaped HTML.
const MAX_PLAN_BYTES: usize = 20_000;

async fn run(ctx: Ctx) -> Reply {
    Plan::fetch(&ctx.cluster).await?.render()
}

/// The Terraform object, and the ConfigMap holding its readable plan if there is one.
pub struct Plan {
    terraform: DynamicObject,
    stored: Option<ConfigMap>,
    kubectl_context: String,
}

impl Plan {
    pub async fn fetch(cluster: &Cluster) -> anyhow::Result<Self> {
        let gvk = GroupVersionKind::gvk("infra.contrib.fluxcd.io", "v1alpha2", "Terraform");
        let (namespace, name) = (&cluster.tofu_namespace, &cluster.tofu_terraform);
        let terraform = cluster
            .custom(&gvk, "terraforms", namespace)
            .get(name)
            .await?;

        let stored = Api::<ConfigMap>::namespaced(cluster.kube.clone(), namespace)
            .get_opt(&plan_config_map(name))
            .await?;
        let kubectl_context = cluster.kubectl_context.clone();
        Ok(Self {
            terraform,
            stored,
            kubectl_context,
        })
    }

    /// A non-empty field of the Terraform object's `status.plan`.
    fn plan_status(&self, field: &str) -> Option<&str> {
        let value = self.terraform.data["status"]["plan"][field].as_str();
        value.filter(|v| !v.is_empty())
    }

    fn pending(&self) -> Option<&str> {
        self.plan_status("pending")
    }

    /// The revision of the stored plan text.
    fn saved(&self) -> Option<&str> {
        let annotations = self.stored.as_ref()?.metadata.annotations.as_ref()?;
        annotations.get("savedPlan").map(String::as_str)
    }

    /// The pending plan, once its readable text is stored and [`Plan::render`] can show it.
    pub fn stored_pending(&self) -> Option<&str> {
        self.pending()
            .filter(|pending| self.saved() == Some(*pending))
    }

    pub fn render(&self) -> Reply {
        let metadata = &self.terraform.metadata;
        let name = metadata.name.as_deref().unwrap_or_default();
        let namespace = metadata.namespace.as_deref().unwrap_or_default();
        let mut reply = format!("**Tofu** `{name}`");
        if let Some(ready) = condition(&self.terraform, "Ready") {
            write!(reply, ": ready `{}` (`{}`)", ready.status, ready.reason)?;
        }
        if let Some(applied) = self.plan_status("lastApplied") {
            write!(reply, "\n\nLast applied: `{applied}`")?;
        }
        let Some(pending) = self.pending() else {
            reply.push_str("\n\nNo pending plan.");
            return Ok(reply);
        };
        write!(reply, "\n\n**Pending:** `{pending}`")?;
        let kubectl = format!("kubectl --context {} -n {namespace}", self.kubectl_context);

        let saved = self.saved();
        let text = self
            .stored
            .as_ref()
            .and_then(|config_map| config_map.data.as_ref()?.get("tfplan"));
        match text {
            // A stored plan from another revision isn't what the command below would apply.
            Some(text) if saved == Some(pending) => {
                write!(reply, "\n\n{}", code_block(head(text, MAX_PLAN_BYTES)))?;
                if text.len() > MAX_PLAN_BYTES {
                    let full = format!(
                        "{kubectl} get configmap {} -o jsonpath='{{.data.tfplan}}'",
                        plan_config_map(name)
                    );
                    write!(
                        reply,
                        "\n\nPlan cut short, the full one:\n\n{}",
                        code_block(&full)
                    )?;
                }
            }
            Some(_) => write!(
                reply,
                "\n\n⚠️ The stored plan is `{}`, not the pending one, so it isn't shown.",
                saved.unwrap_or("unknown")
            )?,
            None => reply.push_str("\n\n⚠️ The plan isn't stored yet, so it isn't shown."),
        }

        let patch = format!(r#"{{"spec":{{"approvePlan":"{pending}"}}}}"#);
        let apply = format!(
            "{kubectl} patch terraform {name} --type merge --field-manager tofu-approve -p '{patch}'"
        );
        write!(reply, "\n\nApply with:\n\n{}", code_block(&apply))?;
        Ok(reply)
    }
}

/// tofu-controller's name for the ConfigMap holding the plan of the `default` workspace.
fn plan_config_map(terraform: &str) -> String {
    format!("tfplan-default-{terraform}")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn plan(pending: &str, stored: Option<ConfigMap>) -> Plan {
        let terraform = serde_json::from_value(json!({
            "apiVersion": "infra.contrib.fluxcd.io/v1alpha2",
            "kind": "Terraform",
            "metadata": { "name": "production", "namespace": "flux-system" },
            "status": {
                "plan": { "pending": pending, "lastApplied": "plan-main-old" },
                "conditions": [{ "type": "Ready", "status": "Unknown", "reason": "Planned" }],
            },
        }))
        .unwrap();
        let kubectl_context = "homelab".to_owned();
        Plan {
            terraform,
            stored,
            kubectl_context,
        }
    }

    fn stored(saved: &str, text: &str) -> Option<ConfigMap> {
        let config_map = serde_json::from_value(json!({
            "metadata": { "name": "tfplan-default-production", "annotations": { "savedPlan": saved } },
            "data": { "tfplan": text },
        }));
        Some(config_map.unwrap())
    }

    #[test]
    fn shows_the_pending_plan_and_how_to_apply_it() {
        let stored = stored("plan-main-new", "Plan: 1 to add");
        let reply = plan("plan-main-new", stored).render().unwrap();
        assert!(reply.contains("ready `Unknown` (`Planned`)"));
        assert!(reply.contains("Last applied: `plan-main-old`"));
        assert!(reply.contains("**Pending:** `plan-main-new`"));
        assert!(reply.contains("```\nPlan: 1 to add\n```"));
        assert!(reply.contains(
            r#"kubectl --context homelab -n flux-system patch terraform production --type merge --field-manager tofu-approve -p '{"spec":{"approvePlan":"plan-main-new"}}'"#
        ));
    }

    #[test]
    fn nothing_to_apply_without_a_pending_plan() {
        let reply = plan("", stored("plan-main-old", "old plan"))
            .render()
            .unwrap();
        assert!(reply.contains("No pending plan."));
        assert!(!reply.contains("old plan") && !reply.contains("kubectl"));
    }

    #[test]
    fn hides_a_stored_plan_of_another_revision() {
        let stored = stored("plan-main-other", "other plan");
        let reply = plan("plan-main-new", stored).render().unwrap();
        assert!(reply.contains("⚠️ The stored plan is `plan-main-other`"));
        assert!(!reply.contains("other plan"));

        let reply = plan("plan-main-new", None).render().unwrap();
        assert!(reply.contains("⚠️ The plan isn't stored yet"));
    }

    #[test]
    fn pending_counts_as_stored_once_its_text_is() {
        let new = plan("plan-main-new", stored("plan-main-new", "new plan"));
        assert_eq!(new.stored_pending(), Some("plan-main-new"));
        let other = plan("plan-main-new", stored("plan-main-other", "other plan"));
        assert_eq!(other.stored_pending(), None);
        assert_eq!(plan("plan-main-new", None).stored_pending(), None);
        let applied = plan("", stored("plan-main-new", "new plan"));
        assert_eq!(applied.stored_pending(), None);
    }

    #[test]
    fn cuts_long_plans_on_a_character_boundary() {
        let stored = stored("plan-main-new", &"é".repeat(MAX_PLAN_BYTES));
        let reply = plan("plan-main-new", stored).render().unwrap();
        assert!(reply.contains(
            "kubectl --context homelab -n flux-system get configmap tfplan-default-production \
             -o jsonpath='{.data.tfplan}'"
        ));
        assert!(reply.len() < MAX_PLAN_BYTES + 1000);
    }
}
