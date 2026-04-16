-- Run this in Supabase → SQL Editor before deploying

create table instructor_changes (
  id                  uuid        default gen_random_uuid() primary key,
  batch               text        not null,
  module              text        not null,
  prev_instructor     text        not null,
  incoming_instructor text        not null,
  first_class         date,
  last_class          date,
  synced_at           timestamptz default now(),
  acknowledged        boolean     default false
);

-- Unique constraint used for upsert (prevents duplicates on re-sync)
alter table instructor_changes
  add constraint instructor_changes_unique
  unique (batch, module, prev_instructor, incoming_instructor, first_class);

-- Optional: allow the anon key to read (if you want public read access)
-- alter table instructor_changes enable row level security;
-- create policy "read all" on instructor_changes for select using (true);
