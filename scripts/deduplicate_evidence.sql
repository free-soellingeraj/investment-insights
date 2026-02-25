-- One-time cleanup: remove duplicate evidence rows across scoring runs.
-- For each (company_id, evidence_type, target_dimension, keyword, patent_number),
-- keep only the row from the latest scoring run.
--
-- Run with: psql $DATABASE_URL -f scripts/deduplicate_evidence.sql
--
-- Preview what will be deleted:
--   SELECT count(*) FROM evidence WHERE id NOT IN ( <inner query> );

BEGIN;

DELETE FROM evidence
WHERE id NOT IN (
    SELECT DISTINCT ON (
        company_id,
        evidence_type,
        target_dimension,
        COALESCE(payload->>'keyword', ''),
        COALESCE(payload->>'patent_number', '')
    ) id
    FROM evidence
    ORDER BY
        company_id,
        evidence_type,
        target_dimension,
        COALESCE(payload->>'keyword', ''),
        COALESCE(payload->>'patent_number', ''),
        pipeline_run_id DESC NULLS LAST
);

-- Show how many rows remain
SELECT count(*) AS remaining_evidence_rows FROM evidence;

COMMIT;
