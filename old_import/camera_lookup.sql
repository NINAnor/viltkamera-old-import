-- Generate camera_lookup.csv from the old GPU modeler database
--
-- Run:
--   psql -q -h ningpu01.nina.no -p 5433 -U rCmahaNwWYXgsQWZxXLqCeJvLLeatthK \
--     -d viltkamera_admin -f camera_lookup.sql > camera_lookup.csv
--
-- For each camera, generates both name→camera_id AND camera_id→camera_id
-- (self-mapping) so numeric IDs like "296" are always resolvable.
-- camera_id is NULL (empty in CSV) for rejected entries:
--   1. Test/copy patterns (case-insensitive regex)
--   2. Duplicate names (name maps to multiple camera_ids)
--   3. Ambiguous numeric names - BOTH sides of the conflict are rejected:
--      - If camera 3230 has name="4", reject both "4" AND "3230"
--      - This prevents misinterpretation when camera_id in Parquet is ambiguous

COPY (
    WITH camera_list AS (
        SELECT DISTINCT camera_id, name
        FROM wild_cameras_location
        WHERE name IS NOT NULL
    ),
    -- Find cameras with ambiguous numeric names (name is digits but != camera_id)
    ambiguous_cameras AS (
        SELECT
            camera_id,
            name::int AS ambiguous_name_as_id
        FROM camera_list
        WHERE
            name ~ '^\d+$'
            AND name::int != camera_id
            AND name NOT ILIKE '%test%'
            AND name NOT ILIKE '%teste%'
            AND name NOT ILIKE '%copy%'
    ),
    -- Collect all camera_ids involved in ambiguity (both sides)
    ambiguous_ids AS (
        SELECT camera_id AS ambiguous_id FROM ambiguous_cameras
        UNION
        SELECT ambiguous_name_as_id AS ambiguous_id FROM ambiguous_cameras
    ),
    -- Name mappings (with exclusions for duplicates and ambiguous numeric names)
    name_mappings AS (
        SELECT
            name,
            COUNT(DISTINCT camera_id) AS id_count,
            MIN(camera_id) AS camera_id
        FROM camera_list
        GROUP BY name
    ),
    valid_name_mappings AS (
        SELECT
            name,
            CASE
                WHEN name ~* '(test|teste|copy)' THEN NULL
                WHEN id_count > 1 THEN NULL
                WHEN name ~ '^\d+$' AND name::int IN (SELECT ambiguous_id FROM ambiguous_ids) THEN NULL
                WHEN camera_id IN (SELECT ambiguous_id FROM ambiguous_ids) THEN NULL
                ELSE camera_id
            END AS camera_id
        FROM name_mappings
    ),
    -- Self-mappings (marked as NULL if camera_id is involved in ambiguity)
    self_mappings AS (
        SELECT DISTINCT
            camera_id::text AS name,
            CASE
                WHEN camera_id IN (SELECT ambiguous_id FROM ambiguous_ids) THEN NULL
                ELSE camera_id
            END AS camera_id
        FROM camera_list
    ),
    -- Combine both mappings (including NULL entries for rejected lookups)
    all_mappings AS (
        SELECT name, camera_id FROM self_mappings
        UNION
        SELECT name, camera_id FROM valid_name_mappings
    )
    SELECT name, camera_id
    FROM all_mappings
    ORDER BY
        CASE WHEN camera_id IS NULL THEN 1 ELSE 0 END,
        camera_id,
        name
) TO STDOUT WITH CSV HEADER NULL '';
