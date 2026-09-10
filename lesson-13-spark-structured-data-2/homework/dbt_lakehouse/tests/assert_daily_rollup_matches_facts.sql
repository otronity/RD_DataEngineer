-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
-- TODO: замініть заглушку (зараз тест проходить вхолосту).
with daily_commits as (
    select sum(commits) as total_commits
    from {{ ref('fact_repo_activity_daily') }}
),
actual_commits as (
    select count(*) as total_commits
    from {{ ref('fact_commit') }}
)
select *
from daily_commits d
cross join actual_commits a
where d.total_commits != a.total_commits
