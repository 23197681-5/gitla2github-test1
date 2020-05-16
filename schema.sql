create table if not exists YOUR_QUEUE_NAME_HERE (
    job_id uuid primary key,
    name text unique,

    state bigint default 0,
    errors text default '',
    inserted_at timestamp without time zone default (now() at time zone 'utc'),
    scheduled_at timestamp without time zone default (now() at time zone 'utc'),
    taken_at timestamp without time zone default null,
    internal_state jsonb default '{}',

    args jsonb default '{}'
);

create table if not exists example_queue (
    job_id uuid primary key,
    name text unique,

    state bigint default 0,
    errors text default '',
    inserted_at timestamp without time zone default (now() at time zone 'utc'),
    scheduled_at timestamp without time zone default (now() at time zone 'utc'),
    taken_at timestamp without time zone default null,
    internal_state jsonb default '{}',

    number_a bigint not null,
    number_b bigint not null
);
