-- report_category_week_naive: НАВМИСНО неоптимізований звіт (дано як є, НЕ редагувати).
-- Join до calendar відбувається по РЯДКОВО-форматованій даті: strftime(event_date) = strftime(day).
-- Через перетворення ключа join DuckDB не може ані пропагувати фільтр (c.iso_week = 2),
-- ані виконати partition pruning → скан усіх 14 партицій.
SELECT
    c.iso_week,
    cat.category,
    count(*) AS events
FROM "warehouse"."main"."stg_events" e
JOIN "warehouse"."main"."calendar" c
    ON strftime(e.event_date, '%Y-%m-%d') = strftime(c.day, '%Y-%m-%d')
JOIN "warehouse"."main"."event_categories" cat
    ON e.event_type = cat.event_type
WHERE c.iso_week = 2
GROUP BY c.iso_week, cat.category
ORDER BY cat.category