# Working in this repo

## Git rules

- **Do NOT commit or push after every prompt.** Finishing a task does not mean
  shipping it. Leave changes in the working tree unless told otherwise.
- Only `git commit` / `git push` when explicitly asked (e.g. "push this",
  "commit that"). One request to push covers that request only — it is not
  standing permission for future work.
- When asked to push, push to the branch the user names; if none is named, ask
  or use the current branch — never create branches unprompted.

## Build / test

- Build: `cargo build --release --features binary`
- Tests: `cargo test --release --lib`
- Format with `cargo fmt` before any commit (repo uses rustfmt.toml).

## Environment notes

- This is the staging checkout; prod espo runs separately from `~/espo`
  (look only, never modify).
- `src/bin/*` is gitignored except explicitly whitelisted bins — diagnostic
  binaries stay local.
- `prodb/` is the local database; never commit it, treat its contents as
  disposable staging state unless told otherwise.
