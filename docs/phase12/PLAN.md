# Phase 12 — implementation plan (written after the mandatory inspection, before any change)

## Inspection findings that shape the plan
- No Git repository (as in all earlier phases) → pre-change snapshots: status file, source tarball and SHA-256 of
  all 17 artifact/dataset files were saved to the session scratchpad before editing.
- Docker Desktop was stopped; started it — the live `fraudstream` stack came back healthy with its volumes.
- Docker VM: 16 CPUs, 7.6 GiB. Live stack limits add up to ~6.9 GiB (Spark 3 GiB, steady ~2.6 GiB). A second full
  stack does **not** fit next to it → isolated experiments need the live stack stopped (reversible, volumes kept).
- The compose network name is hard-coded (`name: fraudstream`): a second compose project would join the **live**
  network and collide on DNS names (`kafka`, `postgres`, …). Isolation needs an override that renames the network.
- The existing disruptive container tests run against the **live** stack (default project + `.env`), which Phase 12
  forbids → make the target configurable (project / env file / ports) and guard disruptive tests on the live project.
- No backup tooling exists (§8 "no backups configured"); Phase 11C backups were ad-hoc `pg_dump`s.
- Candidate security issues seen in code: uvicorn `--forwarded-allow-ips *` + nginx appending to a client-supplied
  `X-Forwarded-For` (login rate-limit key may be spoofable); `limit`/`minutes` query params without bounds on
  `/drift/history`, `/simulation/runs`, `/analytics/risk-distribution`; optional `expected_updated_at`.
- Reports are generated synchronously in a threadpool endpoint of a single uvicorn worker (GIL contention possible).

## Work packages (order)
1. **Isolated environment** — compose project `fraudstream-p12`: override file (own network), own env file with fresh
   random secrets and other host ports, own named volumes; same images; artifacts/datasets bind-mounted read-only.
   Optional `ARTIFACTS_HOST_DIR` / `DATASETS_HOST_DIR` (defaults = current paths) so a restored copy can be mounted.
2. **Backup / restore** (`scripts/backup.py`, `scripts/restore.py`): pg_dump in an exported snapshot with row counts
   from the same snapshot, SQLite online backup + integrity check, artifacts/datasets with SHA-256, optional volume
   archives (uploads, Kafka, Spark checkpoints — consistent only when stopped), manifest + checksums, `.partial`
   directory renamed only on success, never overwrites, no `.env`. Restore only into a non-live project/empty DB,
   compare counts/checksums with the manifest. **Proof:** back up live → restore into the isolated stack → verify →
   start the application on the restored data.
3. **Benchmarks** (`benchmarks/`): API (concurrency levels, p50/p95/p99, errors, sizes), ingestion via API and via
   Kafka at several rates, stage-separated streaming latency (producer ack / Kafka→Spark→Kafka / scored→PostgreSQL /
   end-to-end), backlog recovery, report generation (6 types × sizes, CSV/PDF, API responsiveness during generation),
   resource sampler (`docker stats`, volume growth). All on the isolated stack; machine config recorded.
4. **Failure tests** on the isolated stack: existing disruptive suite (Kafka, Spark, PostgreSQL, full stack) + new:
   Redis outage, Kafka stop during continuous production (acknowledged records), backlog recovery, timed full-stack
   recovery.
5. **Security review + fixes** (with tests): proxy-header trust, unbounded params, input-validation probes, secrets
   scan, container inspection, config-version requirement (backward-compatibility decision first).
6. **Report retention** (CLI, dry-run by default, audited, isolated test) and **PDF CJK** fallback font investigation.
7. Regression suites, docs (`docs/phase12/*`, `benchmarks/BENCHMARKS.md`), `DEVELOPMENT_STATUS.md`, final checks
   (artifacts byte-identical, live counts not reduced, live stack healthy).
