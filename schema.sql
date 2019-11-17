create table if not exists violet_jobs (
    job_id text primary key,
    queue text default null,
    state bigint default 0,
    fail_mode text default 'log_error',
    errors text default '',
    args jsonb default '{}',
    inserted_at timestamp without time zone default (now() at time zone 'utc'),
);
