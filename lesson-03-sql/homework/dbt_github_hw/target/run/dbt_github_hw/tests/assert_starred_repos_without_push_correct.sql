
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Перевірка task 5: жоден репозиторій зі списку не повинен мати PushEvent.
-- Якщо хоч один має — anti-join побудований неправильно.
-- Має повертати 0 рядків.
SELECT s.repo_name
FROM "warehouse"."main"."starred_repos_without_push" s
WHERE EXISTS (
    SELECT 1 FROM "warehouse"."main"."stg_events" e
    WHERE e.repo_name = s.repo_name
      AND e.event_type = 'PushEvent'
)
  
  
      
    ) dbt_internal_test