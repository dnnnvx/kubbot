use super::{Command, Ctx, Reply};
use crate::problems;

pub const COMMAND: Command = Command {
    name: "problems",
    usage: "!problems",
    summary: "everything the alerts watch for that's wrong right now",
    ops_only: true,
    feature: None,
    run: |ctx| Box::pin(run(ctx)),
};

async fn run(ctx: Ctx) -> Reply {
    let lines: Vec<String> = problems::current(&ctx.cluster)
        .await?
        .into_iter()
        .map(|problem| problem.line)
        .collect();
    if lines.is_empty() {
        return Ok("**Problems:** none.".to_owned());
    }
    Ok(problems::summary("Problems", &lines))
}
