# Lance Reconciler

The reconciler polls the job store, claims conversion jobs, and runs them with a
bounded worker pool. It is the only process that changes jobs from queued work
into completed or failed work.

## Job lifecycle

1. A new job starts in `queuing`.
2. The reconciler atomically claims it, changes it to `running`, increments its
   attempt number, and assigns a lease expiration time.
3. While conversion runs, the worker renews the lease and checkpoints progress.
4. A successful conversion changes the job to `succeeded`.
5. A conversion error records the attempt number, timestamp, and reason. The job
   returns to `queuing` while retries remain, otherwise it becomes `failed`.

The maximum is 16 attempts. Errors are retained as attempt history even if a
later retry succeeds.

```text
 +---------+       +---------+       +---------+
 | CREATED | ----> | QUEUING | ----> | RUNNING |
 +---------+       +---------+       +----+----+
                    claim: attempt++      |
                    and assign lease      |
                                          +-- heartbeat
                                          |     |
                                          |     +--> RUNNING
                                          |          same attempt; lease renewed
                                          |
                                          +-- conversion succeeds
                                          |     |
                                          |     +--> SUCCEEDED (terminal)
                                          |
                                          +-- conversion fails
                                          |     |
                                          |     +-- attempts 1-15 --> QUEUING
                                          |     +-- attempt 16 ----> FAILED (terminal)
                                          |
                                          +-- lease expires
                                                |
                                                +-- attempts 1-15 --> RUNNING
                                                |                    next attempt;
                                                |                    new worker
                                                |
                                                +-- attempt 16 ----> FAILED (terminal)
```

Every update to a running job must match its current attempt and unexpired
lease. A worker from an older attempt is therefore unable to update the job.

## Leases and reclaiming

A job can be claimed when it is:

- `queuing` with fewer than 16 attempts; or
- `running` with an expired lease and fewer than 16 attempts.

Claiming loads the job by destination URI, then updates that row. Reclaiming an
expired running job records `lease expired before completion` for the abandoned
attempt. If the selected job is already on its final attempt, it is marked
`failed` with `lease expired on final attempt` instead of being returned to a
worker.

All progress, completion, and failure updates are fenced by destination URI,
attempt number, running status, and an unexpired lease. A stale worker therefore
cannot update a job after another worker has reclaimed it.

Retries restart conversion in overwrite mode. Durable fragment-level resume is
not implemented yet.

## Heartbeats and progress

The lease duration must be at least five times the lease-renewal interval. The
progress checkpoint interval must not exceed the renewal interval. Lease
renewals also include the latest progress snapshot.

The worker's `tokio::select!` only polls a pinned conversion future and
cancellation-safe interval ticks. Job-store writes run after selection, so a
competing timer or completed conversion cannot cancel a write midway.

## Worker pool

Each polling cycle:

1. Collects completed worker tasks.
2. Computes capacity from `WORKER_COUNT`.
3. Atomically claims up to the available capacity.
4. Starts one task per claimed job.

A conversion failure is recorded on the job and does not stop the reconciler.
An unexpected worker-task or job-store error is returned from the reconciler so
the process supervisor can restart it.

## Configuration

Configuration is read only from environment variables:

- `DATABASE_URL` — default `postgres://127.0.0.1:5432/lance_jobs`. SQLite URLs
  require building with `--features sqlite`
- `DATABASE_MAX_CONNECTIONS` — PostgreSQL pool size, default `8`. Ignored for
  SQLite
- `WORKER_COUNT` — default `256`
- `POLL_INTERVAL_MS` — default `1000`
- `LEASE_DURATION_SECS` — default `900`
- `LEASE_RENEW_INTERVAL_SECS` — default `180`
- `PROGRESS_INTERVAL_SECS` — default `30`
- `TARGET_LANCE_FILE_SIZE_MIB` — default `512`
- `BLOB_INLINE_THRESHOLD_MIB` — default `2`
- `BLOB_DEDICATED_THRESHOLD_MIB` — default `32`

Run it from the workspace root:

```shell
DATABASE_URL=postgres://127.0.0.1:5432/lance_jobs cargo run -p lance-reconciler
```
