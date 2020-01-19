create table if not exists violet_jobs (
    primary_job_id text primary key,
    secondary_job_id text UNIQUE,
    queue text default null,
    state bigint default 0,
    fail_mode text default 'log_error',
    errors text default '',
    args jsonb default '{}',
    inserted_at timestamp without time zone default (now() at time zone 'utc'),
    scheduled_at timestamp without time zone default (now() at time zone 'utc'),
    taken_at timestamp without time zone default null,
    internal_state jsonb default '{}'
);
