use std::io;

use rs_git_status2arrow_ipc_stream::{GitDir, GitRepo, GitStatus, status2arrow_ipc_stream_writer};

fn main() -> Result<(), io::Error> {
    let repo = GitDir(".").discover()?;
    let git_repo = GitRepo(repo);
    let status = git_repo.status(gix::progress::Discard)?;
    let mut stdout = io::stdout();

    let items: Vec<_> = GitStatus(status).iter()?.collect::<Result<_, _>>()?;

    status2arrow_ipc_stream_writer(&items, &mut stdout)?;

    Ok(())
}
