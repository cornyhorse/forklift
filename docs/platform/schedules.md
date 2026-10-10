# Schedules

A schedule runs a dataset on a timer: a cron expression, read in an IANA time zone. The
gateway's dispatcher (`forklift-web dispatch`, the `dispatcher` service in Docker Compose)
queues each run when it is due, and the workers run it like any other run of the dataset.

Only datasets that read from a source connection (an object in an `s3` connection, or a `sql`
connection) can be scheduled. A dataset that reads uploaded files needs a new file for each run,
so it is refused ("reads uploaded files, so it cannot run on a schedule").

Everyone who can see datasets (`datasets:read`) sees schedules. Authors and admins
(`datasets:write`) add, change, enable, disable and delete them; adding or enabling one also
needs `jobs:run`, because the schedule runs the dataset, so an API token without `jobs:run`
can stop schedules but not start them.

In the UI, a dataset's page has a **Schedules** section (with a preview of the next runs as you
type), and **Schedules** in the main navigation lists every schedule with its next run and what
became of its last one.

## Cron expressions

Five fields, separated by spaces:

```
┌ minute        0-59
│ ┌ hour          0-23
│ │ ┌ day of month  1-31
│ │ │ ┌ month         1-12 or jan-dec
│ │ │ │ ┌ day of week   0-7 or sun-sat (0 and 7 are Sunday)
│ │ │ │ │
30 2 * * *     02:30 every day
```

| Write | Means |
|---|---|
| `*` | every value |
| `5`, `mon`, `jul` | one value (names in any letter case) |
| `1-5`, `mon-fri` | a range |
| `1,15,30-35` | a list of values and ranges |
| `*/15`, `8-18/2` | every 15th value, every 2nd from 8 to 18 (a step needs `*` or a range: write `5-59/15`, not `5/15`) |
| `@hourly`, `@daily` (`@midnight`), `@weekly`, `@monthly`, `@yearly` (`@annually`) | `0 * * * *`, `0 0 * * *`, `0 0 * * 0`, `0 0 1 * *`, `0 0 1 1 *` |

When both day fields are restricted (neither starts with `*`), a day matches if **either** does,
as in Vixie cron: `0 0 1 * mon` runs on the 1st of each month and on every Monday. A field that
starts with `*` is not restricted, so `0 0 */2 * mon` runs on Mondays that fall on an odd day.

An expression that can never run, such as `0 0 31 2 *` (February 31st), is refused. Errors
name the field and what is wrong: `minute field '61': must be 0-59.`

`POST /api/v1/schedules/preview` with `{"cron": "...", "timezone": "..."}` answers with the next
five runs (or the error) without saving anything.

## Time zones and daylight saving time

The times are wall-clock times in the schedule's time zone (`UTC` unless you name another IANA
zone, such as `Europe/Berlin` or `America/New_York`). Around daylight saving time changes:

- A time the clocks skip (spring forward: in Berlin, 02:00 becomes 03:00) runs **once, at the
  first instant after the gap**: a schedule for 02:30 runs at 03:00 that night, and slots at
  02:00, 02:15, 02:30 and 02:45 all become one run at 03:00.
- A time the clocks pass twice (fall back: in Berlin, 03:00 becomes 02:00) runs **once, at its
  first occurrence**: a schedule for 02:30 runs at 02:30 summer time, not again an hour later.
  An hourly schedule therefore has one two-hour gap that night.

The API reports times in UTC (`next_run_at`, `next_runs`, `last_run_at`); the UI shows them in
the schedule's time zone.

## What the dispatcher does

The dispatcher checks every few seconds (every 10 in Compose) for enabled schedules whose
`next_run_at` has come. For each one it queues a run of the dataset and moves `next_run_at` to
the first slot after now. Every run it queues has the idempotency key
`schedule:<schedule id>:<slot time>`, so one slot is one job, also when several dispatchers run
(each schedule is claimed with `SELECT ... FOR UPDATE SKIP LOCKED`).

What became of the latest slot is the schedule's `last_outcome`, with `last_run_at` (the slot's
time), `last_message` and `last_job_id`:

| Outcome | When |
|---|---|
| `queued` | The run was queued. The message is empty for a run on time, and says so for a late one that caught up. |
| `skipped_overlap` | A run this schedule started earlier is still queued or running: this slot is skipped, so runs of one schedule never overlap. |
| `missed` | The dispatcher was not running when the slot came, and it is now too late to catch up (below). |
| `failed_to_enqueue` | The run could not be queued, for example because the dataset now reads uploads or a limit refuses it; the message says why. The schedule stays enabled and tries again at its next slot. |

### After downtime

A slot the dispatcher finds less than a minute after its time is on time. When the dispatcher
was not running for longer, it queues **at most one** catch-up run, for the most recent missed
slot, and only if that slot is at most `schedule_catch_up_seconds` old (an installation setting:
default 3600, at most 604800; 0 turns catching up off). Otherwise the outcome is `missed` and the
schedule waits for its next slot. Slots that pass while a schedule is disabled are not caught
up: enabling it, or changing its expression or time zone, counts from now.

## Jobs a schedule started

A scheduled run has no requester: `requested_by_id` is null, because the gateway asked, not a
person. `schedule_id` names the schedule and `scheduled_for` the slot; the job page says
"Started by the schedule ..." and the job lists say "a schedule". When a schedule is deleted,
its jobs stay, with `scheduled_for` still set and `schedule_id` null.

Authors and admins may cancel scheduled runs (other jobs are cancelled by whoever requested
them, or an admin). A [webhook](webhooks.md) with the `dataset` scope hears about a dataset's
scheduled runs; an `own_jobs` webhook does not, since nobody requested them.

A dataset that has run cannot be deleted, so deleting a dataset removes only schedules that
never queued a run.

## The API

| Endpoint | |
|---|---|
| `GET /api/v1/datasets/{id}/schedules` | A dataset's schedules |
| `POST /api/v1/datasets/{id}/schedules` | Add one: `{"cron": "30 2 * * *", "timezone": "Europe/Berlin", "enabled": true}` |
| `GET /api/v1/schedules` | Every schedule, the next to run first; filters `enabled` and `dataset_id` |
| `GET`, `PATCH`, `DELETE /api/v1/schedules/{id}` | One schedule; `PATCH` changes `cron`, `timezone` and `enabled` |
| `POST /api/v1/schedules/preview` | The next five runs of an expression in a time zone |

Changes are recorded in the audit log (`schedule.create`, `schedule.update`, `schedule.delete`).

## Running the dispatcher

```bash
forklift-web dispatch                                   # one pass
forklift-web dispatch --every=10 --heartbeat=/tmp/dispatcher.alive
```

Each pass runs its steps in turn (queueing scheduled runs, sending webhook deliveries). A step
that fails is logged and runs again on the next pass; it stops neither the other steps nor the
loop. `--heartbeat` touches the file after each pass in which every step succeeded, so a health
check on its age (Compose: under a minute) also notices a step that keeps failing. A single pass
(no `--every`) exits with an error when a step failed. More than one dispatcher may run.
