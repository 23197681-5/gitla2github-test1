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
