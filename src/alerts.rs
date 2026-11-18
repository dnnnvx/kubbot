//! Posts CI runs, pending tofu plans, Kyverno policy failures and stuck pods to the ops room.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::pin::pin;
use std::time::{Duration, Instant, SystemTime};
use std::{future, mem};

use futures_util::{Stream, StreamExt};
use k8s_openapi::serde::de::DeserializeOwned;
use kube::api::DynamicObject;
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Resource};
use matrix_sdk::Client;
use matrix_sdk::ruma::events::relation::Reply;
use matrix_sdk::ruma::events::room::message::{
    Relation, ReplacementMetadata, RoomMessageEventContent,
};
use matrix_sdk::ruma::{OwnedEventId, OwnedRoomId, RoomId};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::cluster::{Cluster, RunState, run_link, run_name, run_state};
use crate::commands::tofu::Plan;
use crate::markdown::{code_block, tail};
use crate::problems::Problem;

/// Enough for the error, short enough to read in a chat. An event caps at 65 KB.
const MAX_LOG_BYTES: usize = 4_000;

/// Where the tracked runs are kept across restarts, in the bot's store.
const RUNNING_KEY: &str = "tekton.running";

/// Watches forever. The watcher re-lists and retries by itself after an error.
/// A run's start is posted, then edited into its result when it ends; a failure
/// also gets a reply with the logs.
pub async fn watch_ci(client: Client, cluster: Cluster, forget_after: Duration) {
    let mut events = pin!(events(cluster.pipeline_runs()));
    let mut running = Running::load(&client).await;
    while let Some(event) = events.next().await {
        running.forget_older_than(forget_after);
        match running.change(&event) {
            None => continue,
            Some(Change::Started(run)) => {
                let text = message(&cluster.dashboard_url, run, None);
                let sent = post(
                    &client,
                    &cluster.ops_room,
                    RoomMessageEventContent::text_markdown(text),
                )
                .await;
                running.track(run, sent);
            }
            Some(Change::Ended(run, started)) => post_end(&client, &cluster, run, started).await,
        }
        running.save(&client).await;
    }
}

/// Edits the start message into the run's result, and replies to it with the
/// logs of a failure.
async fn post_end(
    client: &Client,
    cluster: &Cluster,
    run: &DynamicObject,
    started: Option<OwnedEventId>,
) {
    let (url, room) = (&cluster.dashboard_url, &cluster.ops_room);
    let failure = match run_state(run) {
        RunState::Failed(_) => {
            let logs = cluster.failure_logs(run).await.inspect_err(|err| {
                warn!(run = run_name(run), "no failure logs: {err:#}");
            });
            Some(message(url, run, logs.ok().as_deref()))
        }
        _ => None,
    };
    let Some(started) = started else {
        // The start wasn't posted when sending failed: the result gets a message of its own.
        let text = failure.unwrap_or_else(|| message(url, run, None));
        post(client, room, RoomMessageEventContent::text_markdown(text)).await;
        return;
    };
    let edit = RoomMessageEventContent::text_markdown(message(url, run, None))
        .make_replacement(ReplacementMetadata::new(started.clone(), None));
    post(client, room, edit).await;
    // Clients usually don't notify on edits, but do on a new message.
    if let Some(failure) = failure {
        let mut reply = RoomMessageEventContent::text_markdown(failure);
        reply.relates_to = Some(Relation::Reply(Reply::with_event_id(started)));
        post(client, room, reply).await;
    }
}

/// Checks forever, the first time right away. A pending plan is posted at every
/// check, as a reminder of the drift, once its text is stored.
pub async fn poll_tofu(client: Client, cluster: Cluster, every: Duration) {
    let mut ticks = tokio::time::interval(every);
    loop {
        ticks.tick().await;
        let plan = match Plan::fetch(&cluster).await {
            Ok(plan) => plan,
            Err(err) => {
                warn!("checking the tofu plan failed: {err:#}");
                continue;
            }
        };
        if plan.stored_pending().is_none() {
            continue;
        }
        let text = match plan.render() {
            Ok(text) => text,
            Err(err) => {
                error!("rendering the tofu plan failed: {err:#}");
                continue;
            }
        };
        let content = RoomMessageEventContent::text_markdown(format!("🟠 Drift\n\n{text}"));
        post(&client, &cluster.ops_room, content).await;
    }
}

/// A watch's events, forever. The watcher re-lists and retries by itself; errors are only logged.
fn events<K>(api: Api<K>) -> impl Stream<Item = watcher::Event<K>>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + 'static,
{
    let url = api.resource_url().to_owned();
    watcher::watcher(api, watcher::Config::default())
        .default_backoff()
        .filter_map(move |event| {
            future::ready(
                event
                    .inspect_err(|err| warn!("watching {url} failed: {err:#}"))
                    .ok(),
            )
        })
}

/// Watches forever, posting each problem `find` sees on an object once, when
/// it shows up. Problems already there when the bot starts stay quiet, and one
/// that flaps, e.g. a flaky signature check or a crash loop, is posted at most
/// once per `repost_after`.
pub async fn watch_problems<K>(
    client: Client,
    api: Api<K>,
    ops_room: OwnedRoomId,
    repost_after: Duration,
    find: impl Fn(&K) -> Vec<Problem>,
) where
    K: Resource + Clone + DeserializeOwned + Debug + Send + 'static,
{
    let key = format!("tekton.problems{}", api.resource_url());
    let mut problems = Problems::load(&client, &key).await;
    let mut events = pin!(events(api));
    while let Some(event) = events.next().await {
        problems.forget_posted_older_than(repost_after);
        for line in problems.change(&event, &find) {
            post(
                &client,
                &ops_room,
                RoomMessageEventContent::text_markdown(line),
            )
            .await;
        }
        if mem::take(&mut problems.changed) {
            save(&client, &key, &problems).await;
        }
    }
}

/// Sends to the ops room. The event ID is what a later edit refers to.
async fn post(
    client: &Client,
    room_id: &RoomId,
    content: RoomMessageEventContent,
) -> Option<OwnedEventId> {
    let Some(room) = client.get_room(room_id) else {
        warn!(room = %room_id, "not in the ops room, dropping alert");
        return None;
    };
    match room.send(content).await {
        Ok(sent) => Some(sent.response.event_id),
        Err(err) => {
            error!(room = %room_id, "send failed: {err:#}");
            None
        }
    }
}

/// The runs whose start was posted and whose end wasn't yet, by UID. Kept
/// across restarts, so a run that outlives the bot, e.g. the bot's own
/// release, gets its start message edited instead of a second one.
/// shortcut: a run that starts and ends while the bot is down or disconnected is never posted.
#[derive(Default)]
struct Running(HashMap<String, Tracked>);

struct Tracked {
    /// `None` when posting the start failed.
    started: Option<OwnedEventId>,
    since: Instant,
}

enum Change<'a> {
    Started(&'a DynamicObject),
    /// With the start message to edit.
    Ended(&'a DynamicObject, Option<OwnedEventId>),
}

impl Running {
    async fn load(client: &Client) -> Self {
        let started: HashMap<String, OwnedEventId> =
            load(client, RUNNING_KEY).await.unwrap_or_default();
        // The age restarts: a run deleted while the bot was down is forgotten a full `forget_after` later.
        let since = Instant::now();
        let tracked = started.into_iter().map(|(uid, started)| {
            let started = Some(started);
            (uid, Tracked { started, since })
        });
        Self(tracked.collect())
    }

    /// Only runs with a start message: one whose start failed to post is posted again after a restart.
    async fn save(&self, client: &Client) {
        let started: HashMap<&str, &OwnedEventId> = self
            .0
            .iter()
            .filter_map(|(uid, tracked)| Some((uid.as_str(), tracked.started.as_ref()?)))
            .collect();
        save(client, RUNNING_KEY, &started).await;
    }

    fn change<'a>(&mut self, event: &'a watcher::Event<DynamicObject>) -> Option<Change<'a>> {
        let run = match event {
            watcher::Event::Init | watcher::Event::InitDone => return None,
            watcher::Event::Delete(run) => {
                self.0.remove(run.metadata.uid.as_deref()?);
                return None;
            }
            // Runs already active when the bot starts are posted as started too.
            watcher::Event::InitApply(run) | watcher::Event::Apply(run) => run,
        };
        let uid = run.metadata.uid.as_deref()?;
        if matches!(run_state(run), RunState::Running) {
            (!self.0.contains_key(uid)).then_some(Change::Started(run))
        } else {
            // An untracked finished run was posted already, or ended before the bot saw it start.
            let tracked = self.0.remove(uid)?;
            Some(Change::Ended(run, tracked.started))
        }
    }

    fn track(&mut self, run: &DynamicObject, started: Option<OwnedEventId>) {
        if let Some(uid) = run.metadata.uid.clone() {
            let since = Instant::now();
            self.0.insert(uid, Tracked { started, since });
        }
    }

    /// No run lasts this long: its end was missed, e.g. it was deleted while the watch was down.
    fn forget_older_than(&mut self, age: Duration) {
        self.0.retain(|_, tracked| tracked.since.elapsed() < age);
    }
}

/// The problems of each object, to post only the ones it didn't have before:
/// objects are rewritten often, e.g. Kyverno's reports at every scan. Kept
/// across restarts, so what broke while the bot was down is posted, and a
/// restart neither reposts nor forgets what was posted lately.
#[derive(Default, Serialize, Deserialize)]
struct Problems {
    /// The keys of each object's problems, by UID. Objects without any aren't
    /// kept, so healthy ones cost nothing and their updates never save.
    keys: HashMap<String, HashSet<String>>,
    /// When each problem was first posted, by UID then key, so a flapping one stays quiet.
    posted: HashMap<String, HashMap<String, SystemTime>>,
    /// Set once a listing is done, or something was saved before: until then, problems are only recorded.
    #[serde(skip)]
    listed: bool,
    /// The objects of the listing in progress: the ones missing from it were deleted meanwhile.
    #[serde(skip)]
    listing: HashSet<String>,
    /// Something to save since the last save.
    #[serde(skip)]
    changed: bool,
}

impl Problems {
    /// Nothing saved means a first start: what's already broken stays quiet.
    async fn load(client: &Client, key: &str) -> Self {
        let Some(saved) = load::<Self>(client, key).await else {
            return Self::default();
        };
        Self {
            listed: true,
            ..saved
        }
    }

    /// The lines to post: problems new on an object, once listed, not posted lately.
    fn change<K: Resource>(
        &mut self,
        event: &watcher::Event<K>,
        find: impl Fn(&K) -> Vec<Problem>,
    ) -> Vec<String> {
        let object = match event {
            watcher::Event::Init => {
                self.listing.clear();
                return Vec::new();
            }
            watcher::Event::InitDone => {
                let listing = mem::take(&mut self.listing);
                let before = self.keys.len();
                self.keys.retain(|uid, _| listing.contains(uid));
                self.changed |= self.keys.len() != before;
                self.listed = true;
                return Vec::new();
            }
            watcher::Event::Delete(object) => {
                if let Some(uid) = object.meta().uid.as_deref() {
                    self.changed |= self.keys.remove(uid).is_some();
                }
                return Vec::new();
            }
            // A re-list, after a restart or a watch error, diffs too, catching what broke meanwhile.
            watcher::Event::InitApply(object) | watcher::Event::Apply(object) => object,
        };
        let Some(uid) = object.meta().uid.clone() else {
            return Vec::new();
        };
        if matches!(event, watcher::Event::InitApply(_)) {
            self.listing.insert(uid.clone());
        }
        let problems = find(object);
        let now: HashSet<String> = problems.iter().map(|problem| problem.key.clone()).collect();
        let before = self.keys.remove(&uid).unwrap_or_default();
        self.changed |= before != now;
        if !now.is_empty() {
            self.keys.insert(uid.clone(), now);
        }
        if !self.listed {
            return Vec::new();
        }
        let posted = self.posted.entry(uid).or_default();
        let lines: Vec<String> = problems
            .into_iter()
            .filter(|problem| !before.contains(&problem.key))
            // Keeps the first post's time, so a steady flapper is reposted once per window.
            .filter(|problem| match posted.entry(problem.key.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(SystemTime::now());
                    true
                }
                Entry::Occupied(_) => false,
            })
            .map(|problem| problem.line)
            .collect();
        self.changed |= !lines.is_empty();
        lines
    }

    fn forget_posted_older_than(&mut self, age: Duration) {
        let count = |posted: &HashMap<String, HashMap<String, SystemTime>>| {
            posted.values().map(HashMap::len).sum::<usize>()
        };
        let before = count(&self.posted);
        for posted in self.posted.values_mut() {
            // A clock set back makes `elapsed` fail: kept, as if just posted.
            posted.retain(|_, at| at.elapsed().map_or(true, |elapsed| elapsed < age));
        }
        self.posted.retain(|_, posted| !posted.is_empty());
        self.changed |= count(&self.posted) != before;
    }
}

/// A value kept in the bot's store across restarts. Missing or unreadable, e.g. from an older version, is `None`.
async fn load<T: DeserializeOwned>(client: &Client, key: &str) -> Option<T> {
    let loaded = async {
        let bytes = client
            .state_store()
            .get_custom_value(key.as_bytes())
            .await?;
        anyhow::Ok(
            bytes
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()?,
        )
    };
    loaded.await.unwrap_or_else(|err| {
        warn!(key, "loading from the store failed: {err:#}");
        None
    })
}

async fn save<T: Serialize>(client: &Client, key: &str, value: &T) {
    let bytes = serde_json::to_vec(value).expect("maps with string keys serialize");
    let saved = client
        .state_store()
        .set_custom_value(key.as_bytes(), bytes)
        .await;
    if let Err(err) = saved {
        warn!(key, "saving to the store failed: {err:#}");
    }
}

fn message(dashboard_url: &str, run: &DynamicObject, logs: Option<&str>) -> String {
    let link = run_link(dashboard_url, run);
    let failed = match run_state(run) {
        RunState::Running => return format!("🟡 **CI running:** {link}"),
        RunState::Succeeded => return format!("✅ **CI succeeded:** {link}"),
        RunState::Cancelled(reason) => return format!("⏹️ **CI cancelled:** {link} (`{reason}`)"),
        RunState::Failed(failed) => failed,
    };
    let mut text = format!(
        "🔴 **CI failed:** {link} (`{}`) {}",
        failed.reason, failed.message
    );
    if let Some(logs) = logs {
        // The end of the logs is where the error is.
        text.push_str("\n\n");
        text.push_str(&code_block(tail(logs, MAX_LOG_BYTES).trim_end()));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::test_run;
    use crate::problems;

    fn event_id() -> OwnedEventId {
        "$start:example.org".parse().unwrap()
    }

    #[test]
    fn posts_the_start_then_the_end_once() {
        let mut running = Running::default();
        let started = || test_run("a", "Unknown", "Running");
        let done = || test_run("a", "True", "Succeeded");

        // Before the bot started: a finished run stays quiet, an active one is posted.
        let old = watcher::Event::InitApply(test_run("old", "False", "Failed"));
        assert!(running.change(&old).is_none());
        let event = watcher::Event::InitApply(started());
        let Some(Change::Started(run)) = running.change(&event) else {
            panic!("start not posted");
        };
        running.track(run, Some(event_id()));
        assert!(running.change(&watcher::Event::InitDone).is_none());

        assert!(running.change(&watcher::Event::Apply(started())).is_none());
        let event = watcher::Event::Apply(done());
        let Some(Change::Ended(_, Some(id))) = running.change(&event) else {
            panic!("end not posted as an edit");
        };
        assert_eq!(id, event_id());
        assert!(running.change(&watcher::Event::Apply(done())).is_none());
        assert!(running.0.is_empty(), "ended runs are forgotten");
    }

    #[tokio::test]
    async fn keeps_tracked_runs_across_restarts() {
        let client = Client::builder()
            .homeserver_url("http://localhost")
            .build()
            .await
            .unwrap();
        let mut running = Running::load(&client).await;
        running.track(&test_run("a", "Unknown", "Running"), Some(event_id()));
        running.track(&test_run("b", "Unknown", "Running"), None);
        running.save(&client).await;

        // After a restart: still running is quiet, the end edits the old start message.
        let mut running = Running::load(&client).await;
        let event = watcher::Event::InitApply(test_run("a", "Unknown", "Running"));
        assert!(running.change(&event).is_none());
        let event = watcher::Event::Apply(test_run("a", "True", "Succeeded"));
        let Some(Change::Ended(_, Some(id))) = running.change(&event) else {
            panic!("end not posted as an edit");
        };
        assert_eq!(id, event_id());
        assert!(running.0.is_empty(), "a run whose start failed isn't kept");

        // Alerts: nothing saved is a first start and stays quiet, anything saved diffs.
        assert!(!Problems::load(&client, "problems").await.listed);
        save(&client, "problems", &Problems::default()).await;
        assert!(Problems::load(&client, "problems").await.listed);
    }

    #[test]
    fn forgets_deleted_and_stale_runs() {
        let mut running = Running::default();
        let run = test_run("a", "Unknown", "Running");
        running.track(&run, None);
        assert!(
            running
                .change(&watcher::Event::Delete(run.clone()))
                .is_none()
        );
        assert!(running.0.is_empty());

        running.track(&run, None);
        running.forget_older_than(Duration::from_secs(3600));
        assert_eq!(running.0.len(), 1);
        running.forget_older_than(Duration::ZERO);
        assert!(running.0.is_empty());
    }

    #[test]
    fn posts_each_policy_failure_once() {
        use serde_json::json;

        use crate::commands::kyverno::test_report;

        let fail = |policy: &str| json!({ "policy": policy, "rule": policy, "result": "fail", "message": "no" });
        let mut problems = Problems::default();
        let mut change = |event| problems.change(&event, problems::policy_failures);

        // Failing before the bot started: quiet.
        let report = |results| test_report("a", &json!(results));
        assert!(change(watcher::Event::InitApply(report(vec![fail("signed")]))).is_empty());
        assert!(change(watcher::Event::InitDone).is_empty());

        // A background scan rewrites the same results: quiet. A new failure: posted.
        assert!(change(watcher::Event::Apply(report(vec![fail("signed")]))).is_empty());
        let posted = change(watcher::Event::Apply(report(vec![
            fail("signed"),
            fail("labels"),
        ])));
        assert_eq!(posted.len(), 1);
        assert!(posted[0].starts_with("🛡️ **Kyverno:** `labels` on ReplicaSet"));

        // Fixed then broken again: posted again. A re-list catches what failed meanwhile.
        change(watcher::Event::Apply(report(vec![])));
        assert_eq!(
            change(watcher::Event::InitApply(report(vec![fail("signed")]))).len(),
            1
        );

        // Flapping again soon after: quiet, until the last post is forgotten.
        change(watcher::Event::Apply(report(vec![])));
        assert!(change(watcher::Event::Apply(report(vec![fail("signed")]))).is_empty());
        change(watcher::Event::Apply(report(vec![])));
        problems.forget_posted_older_than(Duration::ZERO);
        let again = watcher::Event::Apply(report(vec![fail("signed")]));
        assert_eq!(problems.change(&again, problems::policy_failures).len(), 1);

        let deleted = watcher::Event::Delete(report(vec![]));
        problems.change(&deleted, problems::policy_failures);
        assert!(problems.keys.is_empty());
    }

    #[test]
    fn diffs_against_what_was_saved_before_a_restart() {
        use serde_json::json;

        use crate::commands::kyverno::test_report;

        let fail = |policy: &str| json!({ "policy": policy, "rule": policy, "result": "fail", "message": "no" });
        let report = |uid, results| test_report(uid, &json!(results));
        let mut problems = Problems {
            listed: true,
            ..Problems::default()
        };
        let event = watcher::Event::Apply(report("a", vec![fail("signed")]));
        assert_eq!(problems.change(&event, problems::policy_failures).len(), 1);
        assert!(problems.changed, "a post is saved");
        let healthy = watcher::Event::Apply(report("b", vec![]));
        problems.changed = false;
        problems.change(&healthy, problems::policy_failures);
        assert!(!problems.changed, "a healthy object isn't saved");
        assert_eq!(problems.keys.len(), 1);

        // Restarted: the saved state comes back as if listed.
        let saved = serde_json::to_vec(&problems).unwrap();
        let mut problems = Problems {
            listed: true,
            ..serde_json::from_slice(&saved).unwrap()
        };
        let mut change = |event| problems.change(&event, problems::policy_failures);
        assert!(change(watcher::Event::Init).is_empty());
        // Still failing: quiet. Broke while the bot was down: posted.
        assert!(change(watcher::Event::InitApply(report("a", vec![fail("signed")]))).is_empty());
        assert_eq!(
            change(watcher::Event::InitApply(report("c", vec![fail("labels")]))).len(),
            1
        );
        assert!(change(watcher::Event::InitDone).is_empty());

        // Deleted while the bot was down: dropped once the listing is done.
        let event = watcher::Event::InitApply(report("a", vec![fail("signed")]));
        assert!(
            problems
                .change(&watcher::Event::Init, problems::policy_failures)
                .is_empty()
        );
        problems.change(&event, problems::policy_failures);
        problems.change(&watcher::Event::InitDone, problems::policy_failures);
        assert_eq!(problems.keys.keys().collect::<Vec<_>>(), ["a"]);
    }

    #[test]
    fn formats_each_state() {
        let link = |n: &str| format!("[{n}](https://t/#/namespaces/tekton-ci/pipelineruns/{n})");
        let text =
            |status, reason, logs| message("https://t", &test_run("b", status, reason), logs);
        assert_eq!(
            text("Unknown", "Running", None),
            format!("🟡 **CI running:** {}", link("b"))
        );
        assert_eq!(
            text("True", "Succeeded", None),
            format!("✅ **CI succeeded:** {}", link("b"))
        );
        assert_eq!(
            text("False", "Cancelled", None),
            format!("⏹️ **CI cancelled:** {} (`Cancelled`)", link("b"))
        );
        assert_eq!(
            text("False", "Failed", Some("compiling\nerror[E0308]\n")),
            format!(
                "🔴 **CI failed:** {} (`Failed`) Tasks Completed: 2 (Failed: 1, Cancelled 0), Skipped: 2\
                 \n\n```\ncompiling\nerror[E0308]\n```",
                link("b")
            )
        );
    }

    #[test]
    fn keeps_the_end_of_long_logs() {
        let logs = format!("{}the error", "é".repeat(MAX_LOG_BYTES));
        let text = message("https://t", &test_run("b", "False", "Failed"), Some(&logs));
        assert!(text.ends_with("the error\n```"));
        assert!(text.len() < MAX_LOG_BYTES + 500);
    }
}
