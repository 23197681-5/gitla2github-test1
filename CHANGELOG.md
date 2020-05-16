# 0.3 (WIP)

 - major breaking refactor of job queues
   - database structure changed heavily. instead of a single `violet_jobs`
     table, there's one table per job queue.
   - job queues are now declared via a class, instead of registering manually
     to the job manager. `sched.create_job_queue` => `sched.register_job_queue`
   - job queues can declare non-jsonb parameters via their table's columns.
   - APIs that were on `JobManager` are now on the job queue class itself.
   - `custom_start_event` is dropped in favor of `setup` / `handle`
     classmethods
   - `sched.push_queue("your_job_queue", ...)` => `YourJobQueue.push(...)`
   - `sched.fetch_queue_job_status(job_id)` => `YourJobQueue.fetch_job_status(job_id)`
   - `sched.wait_job(job_id, ...)` => `YourJobQueue.wait_job(job_id, ...)`
   - Note: `sched.wait_job_start` **did not change**

# 0.2.3

 - add custom start events
    - so that jobs can do state initialization THEN signal everyone waiting
      on them

# 0.2.2

 - add JobManager.wait_job_start

# 0.2.1

```sql
DROP TABLE violet_jobs
```

another experiment. trying to use job\_id as UUID for proper ordering.

# 0.2.0

if you still want to maintain `violet_jobs` intact right now:

```sql
ALTER TABLE violet_jobs ADD COLUMN name text UNIQUE;
```

(not recommended, will likely break things.
recommended to drop the table and start over)

 - change of job ids from uuids to [flakeids](https://gitlab.com/elixire/hail)
   - job ids as they are right now were removed, they are automatically set to a new flake id
   - job ids are now sorteable!
   - users of violet can set names for jobs (default uuid4)

# 0.1.0

first proper release
