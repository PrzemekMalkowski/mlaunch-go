# Changelog

## v0.2.1 — 2026-11-09

### Highlights

- **macOS support** (Intel and Apple Silicon) with MongoDB Community Server.
- **New default location**: sandboxes and downloads now live under
  `~/mongodb-sandboxes` instead of the current directory (see *Upgrade notes*).
- **Live progress display** for `init`, `start` and `stop`.

### Added

- macOS hosts are detected and served MongoDB Community tarballs from
  `fastdl.mongodb.org/osx/` (`mongodb-macos-x86_64-…`, `mongodb-macos-arm64-…`;
  releases up to 4.0 via the older `osx-ssl` name). Process discovery uses the
  stock `lsof` and `ps` tools, as macOS has no `/proc`.
- Release archives for `darwin_amd64` and `darwin_arm64`.
- Step-by-step progress output: each stage is one line with a spinner and a
  detail (download size, percentage and rate; "member 2/3"; …), finished with
  a status marker and the elapsed time. When stdout is not a terminal,
  `TERM=dumb`, `NO_COLOR` is set or `--no-color` is given, the same
  information is printed as plain lines.
- `init` ends with a summary block: `Sandbox ready! Connect via: …` followed
  by the environment, directory, processes, state file, auth and search details.
- `start` restarts nodes in groups (config servers, mongod, mongos, mongot),
  reports how many were started and how many were already running, and prints
  the connection string for the environment.
- `stop` shows which processes it is still waiting for.
- Every new mongod gets a 0.5 GB WiredTiger cache
  (`storage.wiredTiger.engineConfig.cacheSizeGB` in its `mongod.conf`)
  instead of the single-instance default of 50% of RAM minus 1 GB, so
  multi-node sandboxes no longer overcommit memory. Edit the conf file or
  pass `-- --wiredTigerCacheSizeGB N` to change it.

### Changed

- `--dir` now defaults to `~/mongodb-sandboxes/sandboxes` (was `./sandboxes`)
  and `--download-dir` to `~/mongodb-sandboxes/downloads` (was `./downloads`),
  so every command finds the same environments regardless of the working
  directory.
- `show-repo`: the HOST BUILD column now also applies to Percona Server and
  Percona Search rows (`-` on macOS, where they are not published).
- `--distro percona` and `--percona-search` on macOS fail early with an
  explanatory error; `--binary-dir` still accepts any build.
- `stop` on an environment with nothing running reports "Nothing to stop"
  instead of "stopped 0 process(es)"; if processes outlive the 60 s wait, it
  now names them and reports how many of the total stopped.
- Progress and status messages are written to stdout.

### Fixed

- `destroy` and `list` no longer kill or count unrelated processes for
  orphan environments (directories without a state file). Previously any
  process whose command line contained the environment path matched — for
  example the shell running `mlaunch-go destroy --dir …`, `sudo`, or
  `tail -f …/mongod.log`. Now only mongod, mongos and mongot processes
  started with `--config <file inside the environment>` match.
- Archives whose top-level directory differs from the archive name (the
  macOS arm64 tarballs unpack into `mongodb-macos-aarch64--<version>/`) are
  no longer re-extracted on every `init`.

### Upgrade notes

- Environments created by v0.2.0 stay where they were. Their config files
  and state files hold absolute paths, so do not move them. Manage them by
  passing the old location, e.g. `mlaunch-go list --dir ./sandboxes`, or
  destroy and recreate them.
- To reuse already downloaded tarballs, pass `--download-dir ./downloads` or
  copy (not move) them to `~/mongodb-sandboxes/downloads`. Environments
  created by v0.2.0 record the absolute path of the binaries they were
  started with.
- Existing environments keep their `mongod.conf`, so they do not get the
  0.5 GB cache limit.

## v0.2.0 — 2026-08-26

- Percona Search for MongoDB (mongot) support: `init --percona-search` runs
  one mongot per replica set or shard next to Percona Server for MongoDB 8.3+,
  so `$search` / `$vectorSearch` work in the sandbox.
- `show-repo --distro search` lists Percona Search releases.

## v0.1.0 — 2026-07-17

- Initial release.
