# tekton-matrix-bot

Matrix bot for the homelab, built on [matrix-rust-sdk](https://github.com/matrix-org/matrix-rust-sdk) (E2EE capable).
It joins rooms it's invited to and answers `!commands` (`!help` lists them) as replies.

## Configuration

| Env | Required | Meaning |
|-----|----------|---------|
| `MATRIX_HOMESERVER` | yes | e.g. `http://tuwunel.matrix.svc.cluster.local:8008` |
| `STORE_PASSPHRASE` | yes | encrypts the SQLite store, including the saved session |
| `MATRIX_USER`, `MATRIX_PASSWORD` | first start only | used to log in when there's no saved session |
| `RECOVERY_PASSPHRASE` | no | keeps the cross-signing keys on the server, encrypted under it, so a new device can sign itself |
| `DATA_DIR` | no (`/data`) | holds `store/` |
| `HEALTH_ADDR` | no (`0.0.0.0:8080`) | where the health probes are served |
| `TEKTON_DASHBOARD_URL` | yes | e.g. `https://tekton.local.casasulgorgo.net`, for links to CI runs |
| `OPS_ROOM` | yes | room ID, the only room where `!ci`, `!tofu` and `!kyverno` answer and where CI runs, tofu plans, policy failures and stuck pods are posted; use an encrypted, invite-only room |
| `TEKTON_NAMESPACE` | no (`tekton-ci`) | where the PipelineRuns and their pods are |
| `CI_LOG_LINES` | no (`30`) | how many log lines of a failed CI step are posted |
| `TOFU_NAMESPACE` | no (`flux-system`) | where the tofu-controller `Terraform` object is |
| `TOFU_TERRAFORM` | no (`production`) | the `Terraform` object whose plans `!tofu` shows and drift alerts post |
| `KUBECTL_CONTEXT` | no (`homelab`) | kubeconfig context in the `kubectl` command shown to apply a plan |
| `TOFU_POLL_MINUTES` | no (`30`) | how often the pending tofu plan is checked |
| `TEKTON_GC_HOURS` | no (`24`) | a CI run still running after this long is forgotten, in case its end was missed |
| `ALERT_REPOST_HOURS` | no (`24`) | a Kyverno failure or stuck pod that keeps coming back is posted at most once per this |
| `POD_STUCK_REASONS` | no (`CrashLoopBackOff,ImagePullBackOff,CreateContainerConfigError`) | comma-separated container waiting reasons that get a pod posted; an OOM kill always does |
| `RUST_LOG` | no | tracing filter, default `info,matrix_sdk=warn` |

Any variable can instead be given as `<NAME>_FILE`, pointing at a file holding the value (for mounted secrets).

Everything lives in `store/`: the crypto state and the session (access token, device ID), sealed under `STORE_PASSPHRASE`.
Keep it on a persistent volume. Wiping it means a new login, i.e. a new device, which can't decrypt messages the old one received.

## Device verification

The bot sets up cross-signing by itself and signs its device, so clients don't warn about its messages. The account must not already have cross-signing keys, for example from logging in to it with another client: the bot can't sign itself with keys it doesn't hold, and clients keep warning.

The private keys live in `store/`, so a new device can't sign itself and the warning returns. To avoid that, set `RECOVERY_PASSPHRASE`. On start, the bot stores the keys on the server encrypted under it, along with a server-side backup of room keys, and a later new device fetches them back. Keep it stable and treat it like a password: whoever has it and access to the account can sign devices as the bot. A recovery key or passphrase set up from another client works too, as long as secret storage holds the keys.

## Cluster access

The bot talks to Kubernetes with the in-cluster service account, or the local kubeconfig when run outside the cluster, and only reads:

- `!ci` lists PipelineRuns in `TEKTON_NAMESPACE`, linking to the Tekton dashboard.
- Each run's start is posted to `OPS_ROOM`, and the message is edited into the result when it ends; since clients usually don't notify on edits, a failure also gets a reply with the last log lines of the failed step. Which runs have a start message is kept in `store/`, so a run that outlives the bot, like the bot's own release, still gets its message edited after a restart. Runs that start and end while the bot is down aren't posted.
- `!tofu` shows the pending plan of the `TOFU_TERRAFORM` Terraform in `TOFU_NAMESPACE`, and the `kubectl` command to approve it. Applying is left to whoever runs that command. Every `TOFU_POLL_MINUTES`, a pending plan is posted the same way, as a reminder of the drift, once its readable text is stored.
- `!kyverno` lists the `fail` results in Kyverno's PolicyReports and ClusterPolicyReports, which cover audit-mode policies and background scans. Each failure is also posted when it first shows up in a report: a background scan rewriting the same results stays quiet, a fixed failure that comes back is posted again. Failures already there the first time the bot starts aren't posted; after that, what it has seen is kept in `store/`, so a restart or upgrade reposts nothing and anything that broke while the bot was down is posted when it's back, and one that keeps flapping is posted at most once per `ALERT_REPOST_HOURS`.
- Containers that are OOM-killed, or waiting for one of `POD_STUCK_REASONS`, are posted the same way, in any namespace, an OOM kill with the container's memory limit.

The service account needs, in `TEKTON_NAMESPACE`: `get`, `list`, `watch` on `pipelineruns`, `list` on `taskruns`, `get` on `pods/log`; in `TOFU_NAMESPACE`: `get` on `terraforms` and `configmaps`; cluster-wide: `list`, `watch` on `pods`, and on `policyreports` and `clusterpolicyreports` (`wgpolicyk8s.io`).
Failure logs are posted as they are: a step that prints a secret puts it in the room's history.

## Health probes

- `GET /health/live`: 503 once syncing has been failing for 5 minutes, so Kubernetes restarts a bot stuck on an unrecoverable error. Always 200 before the first sync.
- `GET /health/ready`: 200 once the first sync is done and the last one succeeded within 2 minutes.

A failed sync is logged and retried; HTTP requests are retried up to 5 times on network errors.

```yaml
livenessProbe:
  httpGet: { path: /health/live, port: 8080 }
  periodSeconds: 30
readinessProbe:
  httpGet: { path: /health/ready, port: 8080 }
  periodSeconds: 10
```

## Run locally

```bash
mkdir -p data
DATA_DIR=./data MATRIX_HOMESERVER=https://matrix.casasulgorgo.net \
  STORE_PASSPHRASE=… MATRIX_USER=… MATRIX_PASSWORD=… cargo run
```

`cargo fmt`, `clippy` and the tests run in the pre-commit hook (`.githooks/pre-commit`), not in the image build.
The nix dev shell enables it; otherwise run `git config core.hooksPath .githooks`.

## Layout

| File | Role |
|------|------|
| `src/main.rs` | startup and sync loop |
| `src/config.rs` | environment configuration |
| `src/session.rs` | login and session persistence |
| `src/health.rs` | liveness and readiness probes |
| `src/handlers.rs` | Matrix event handlers (invites, messages) |
| `src/cluster.rs` | Kubernetes access shared by commands and alerts |
| `src/alerts.rs` | posts CI runs, pending tofu plans, Kyverno policy failures and stuck pods to the ops room |
| `src/commands/` | command registry, parsing and dispatch, one module per command |

## Adding a command

Create `src/commands/<name>.rs`, modelled on `ping.rs`:

```rust
use super::{Command, Ctx, Reply};

pub const COMMAND: Command = Command {
    name: "echo",
    usage: "!echo <text>",
    summary: "repeat the text back",
    run: |ctx| Box::pin(run(ctx)),
};

async fn run(ctx: Ctx) -> Reply {
    anyhow::ensure!(!ctx.args.is_empty(), "usage: `!echo <text>`");
    Ok(ctx.args)
}
```

Then declare it with `mod <name>;` in `src/commands/mod.rs` and add it to `ALL`; `!help` picks it up automatically.
The command gets the client, room, sender, raw arguments and cluster access through `Ctx`; an `Err` is logged and reported to the sender.
Each command runs in its own task, so slow ones don't hold up the bot.
