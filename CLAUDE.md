# Working in this repo

## Git rules

- **Do NOT commit or push after every prompt.** Finishing a task does not mean
  shipping it. Leave changes in the working tree unless told otherwise.
- Only `git commit` / `git push` when explicitly asked (e.g. "push this",
  "commit that"). One request to push covers that request only — it is not
  standing permission for future work.
- When asked to push, push to the branch the user names; if none is named, ask
  or use the current branch — never create branches unprompted.

## Indexer changes

- **New index work starts at the current tip and only moves forward.** An index
  upgrade must leave every existing keyspace readable and untouched, and must not
  need historical data to be regenerated to be correct. A new series simply has
  no points before the block it was deployed at — that is fine and expected.
- **Never write a backfill, a migration, or anything that rewrites or invalidates
  an already-indexed keyspace without asking first.** Backfills stall the indexer
  (they run inside `index_block`, so no blocks advance until they finish), they
  are slow on a real database, and one per index upgrade would leave this repo
  unmaintainable. If a change seems to need one, stop and ask before writing it.
- The same goes for anything that forces a reindex, changes the meaning of an
  existing key, or deletes/rewrites history. Ask, do not assume.
- **Never delete keys in prod.** If indexed data turns out to be wrong, bump the
  namespace (`v1` → `v2`), write the corrected data there, point the readers at
  it, and leave the old keys in place, stale and orphaned. Writing new keys is
  safe; deleting is not.
- When a backfill *is* explicitly requested: it must be idempotent, it must
  only write to namespaces nothing else reads, it must be guarded by a marker
  key written only on completion, and its per-height cost must be bounded
  (range scans with a limit, never whole-namespace reads in a loop).

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
