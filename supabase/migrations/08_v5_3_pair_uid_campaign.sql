-- ════════════════════════════════════════════════════════════════════════════
-- Migration 08: v5_3 campaign with stable visual classification keys
-- ════════════════════════════════════════════════════════════════════════════
-- Operational cut-over:
--   - Archive the v3 id_par-based campaign tables.
--   - Recreate active tables for v5_3.
--   - Store classifications by stable item_uid:
--       pairs  -> pair_uid = min(TARGETID):max(TARGETID)
--       groups -> stable_system_id when available
--
-- Run this only when the public app is ready to use the v5_3 catalog.
-- The v3 data remain in *_v3_archive tables.
-- ════════════════════════════════════════════════════════════════════════════

BEGIN;

CREATE OR REPLACE FUNCTION _is_valid_device_id(p_device_id text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_device_id ~ '^[A-Z0-9_]{3,20}$';
$$;

CREATE OR REPLACE FUNCTION _is_valid_classification(p_classification text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_classification IN ('FP', 'Pair', 'PM', 'GROUP', 'PP');
$$;

CREATE OR REPLACE FUNCTION _is_valid_item_type(p_item_type text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_item_type IN ('pair', 'group');
$$;

-- Archive active v3 tables once. If the archive already exists, keep it.
DO $$
BEGIN
  IF to_regclass('public.clasificaciones_v3_archive') IS NULL
     AND to_regclass('public.clasificaciones') IS NOT NULL THEN
    ALTER TABLE public.clasificaciones RENAME TO clasificaciones_v3_archive;
  END IF;

  IF to_regclass('public.partitions_v3_archive') IS NULL
     AND to_regclass('public.partitions') IS NOT NULL THEN
    ALTER TABLE public.partitions RENAME TO partitions_v3_archive;
  END IF;
END $$;

-- Active v5_3 classifications table.
CREATE TABLE IF NOT EXISTS public.clasificaciones (
  id               bigserial PRIMARY KEY,
  device_id        text        NOT NULL,
  item_type        text        NOT NULL CHECK (item_type IN ('pair', 'group')),
  item_uid         text        NOT NULL,
  pair_uid         text,
  stable_system_id text,
  id_par_v5        integer,
  classification   text        NOT NULL CHECK (classification IN ('FP', 'Pair', 'PM', 'GROUP', 'PP')),
  source           text        NOT NULL DEFAULT 'v5_3_app',
  exported_at      timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (device_id, item_type, item_uid)
);

CREATE INDEX IF NOT EXISTS clasificaciones_item_uid_idx
  ON public.clasificaciones(item_type, item_uid);

CREATE INDEX IF NOT EXISTS clasificaciones_pair_uid_idx
  ON public.clasificaciones(pair_uid)
  WHERE pair_uid IS NOT NULL;

-- Active v5_3 partitions table. Existing assignment RPCs use this shape.
CREATE TABLE IF NOT EXISTS public.partitions (
  device_id        text PRIMARY KEY,
  calib_seed       integer NOT NULL,
  work_start       integer NOT NULL,
  work_end         integer NOT NULL,
  group_work_start integer,
  group_work_end   integer,
  registered_at    timestamptz NOT NULL DEFAULT now(),
  calib_v          integer NOT NULL DEFAULT 2,
  work_start_v2    integer,
  work_end_v2      integer,
  n_v1             integer,
  n_v2             integer,
  n_groups         integer
);

ALTER TABLE public.clasificaciones ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.partitions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS public_write ON public.clasificaciones;
DROP POLICY IF EXISTS public_read ON public.partitions;
DROP POLICY IF EXISTS auto_register ON public.partitions;

-- Public access goes through SECURITY DEFINER RPCs.
REVOKE SELECT, INSERT, UPDATE, DELETE ON public.clasificaciones FROM anon;
REVOKE SELECT, INSERT, UPDATE, DELETE ON public.partitions FROM anon;

-- v5_3 public API.
CREATE OR REPLACE FUNCTION get_device_classifications(p_device_id text)
RETURNS TABLE (
  item_type        text,
  item_uid         text,
  pair_uid         text,
  stable_system_id text,
  id_par_v5        integer,
  classification   text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT _is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;

  RETURN QUERY
  SELECT c.item_type, c.item_uid, c.pair_uid, c.stable_system_id,
         c.id_par_v5, c.classification
    FROM clasificaciones c
   WHERE c.device_id = p_device_id
   ORDER BY c.item_type, c.item_uid;
END;
$$;

CREATE OR REPLACE FUNCTION upsert_classification(
  p_device_id        text,
  p_item_type        text,
  p_item_uid         text,
  p_classification   text,
  p_pair_uid         text DEFAULT NULL,
  p_stable_system_id text DEFAULT NULL,
  p_id_par_v5        integer DEFAULT NULL,
  p_exported_at      timestamptz DEFAULT now(),
  p_source           text DEFAULT 'v5_3_app'
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT _is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;
  IF NOT _is_valid_item_type(p_item_type) THEN
    RAISE EXCEPTION 'Invalid item_type';
  END IF;
  IF p_item_uid IS NULL OR length(p_item_uid) < 3 OR length(p_item_uid) > 128 THEN
    RAISE EXCEPTION 'Invalid item_uid';
  END IF;
  IF NOT _is_valid_classification(p_classification) THEN
    RAISE EXCEPTION 'Invalid classification';
  END IF;
  IF p_item_type = 'pair' AND (p_pair_uid IS NULL OR p_pair_uid <> p_item_uid) THEN
    RAISE EXCEPTION 'pair classifications require pair_uid = item_uid';
  END IF;
  IF p_item_type = 'group' AND p_stable_system_id IS NULL THEN
    RAISE EXCEPTION 'group classifications require stable_system_id';
  END IF;

  INSERT INTO clasificaciones(
    device_id, item_type, item_uid, pair_uid, stable_system_id,
    id_par_v5, classification, source, exported_at
  )
  VALUES (
    p_device_id, p_item_type, p_item_uid, p_pair_uid, p_stable_system_id,
    p_id_par_v5, p_classification, COALESCE(p_source, 'v5_3_app'),
    COALESCE(p_exported_at, now())
  )
  ON CONFLICT (device_id, item_type, item_uid)
  DO UPDATE SET
    pair_uid         = EXCLUDED.pair_uid,
    stable_system_id = EXCLUDED.stable_system_id,
    id_par_v5        = EXCLUDED.id_par_v5,
    classification   = EXCLUDED.classification,
    source           = EXCLUDED.source,
    exported_at      = EXCLUDED.exported_at;
END;
$$;

CREATE OR REPLACE FUNCTION upsert_classifications(p_rows jsonb)
RETURNS int
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_count int;
BEGIN
  IF jsonb_typeof(p_rows) <> 'array' THEN
    RAISE EXCEPTION 'p_rows must be a JSON array';
  END IF;
  IF jsonb_array_length(p_rows) > 3000 THEN
    RAISE EXCEPTION 'Too many rows';
  END IF;

  CREATE TEMP TABLE _rows_to_upsert (
    device_id        text,
    item_type        text,
    item_uid         text,
    pair_uid         text,
    stable_system_id text,
    id_par_v5        integer,
    classification   text,
    source           text,
    exported_at      timestamptz
  ) ON COMMIT DROP;

  INSERT INTO _rows_to_upsert(
    device_id, item_type, item_uid, pair_uid, stable_system_id,
    id_par_v5, classification, source, exported_at
  )
  SELECT device_id, item_type, item_uid, pair_uid, stable_system_id,
         id_par_v5, classification, source, exported_at
    FROM jsonb_to_recordset(p_rows)
         AS x(
           device_id text, item_type text, item_uid text, pair_uid text,
           stable_system_id text, id_par_v5 integer, classification text,
           source text, exported_at timestamptz
         );

  IF EXISTS (
    SELECT 1 FROM _rows_to_upsert
     WHERE NOT _is_valid_device_id(device_id)
        OR NOT _is_valid_item_type(item_type)
        OR item_uid IS NULL OR length(item_uid) < 3 OR length(item_uid) > 128
        OR NOT _is_valid_classification(classification)
        OR (item_type = 'pair' AND (pair_uid IS NULL OR pair_uid <> item_uid))
        OR (item_type = 'group' AND stable_system_id IS NULL)
  ) THEN
    RAISE EXCEPTION 'Invalid row in p_rows';
  END IF;

  INSERT INTO clasificaciones(
    device_id, item_type, item_uid, pair_uid, stable_system_id,
    id_par_v5, classification, source, exported_at
  )
  SELECT device_id, item_type, item_uid, pair_uid, stable_system_id,
         id_par_v5, classification, COALESCE(source, 'v5_3_app'),
         COALESCE(exported_at, now())
    FROM _rows_to_upsert
  ON CONFLICT (device_id, item_type, item_uid)
  DO UPDATE SET
    pair_uid         = EXCLUDED.pair_uid,
    stable_system_id = EXCLUDED.stable_system_id,
    id_par_v5        = EXCLUDED.id_par_v5,
    classification   = EXCLUDED.classification,
    source           = EXCLUDED.source,
    exported_at      = EXCLUDED.exported_at;

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION delete_classification(
  p_device_id text,
  p_item_type text,
  p_item_uid  text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT _is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;
  IF NOT _is_valid_item_type(p_item_type) THEN
    RAISE EXCEPTION 'Invalid item_type';
  END IF;
  IF p_item_uid IS NULL OR length(p_item_uid) < 3 OR length(p_item_uid) > 128 THEN
    RAISE EXCEPTION 'Invalid item_uid';
  END IF;

  DELETE FROM clasificaciones
   WHERE device_id = p_device_id
     AND item_type = p_item_type
     AND item_uid = p_item_uid;
END;
$$;

GRANT EXECUTE ON FUNCTION get_device_classifications(text) TO anon;
GRANT EXECUTE ON FUNCTION upsert_classification(text, text, text, text, text, text, integer, timestamptz, text) TO anon;
GRANT EXECUTE ON FUNCTION upsert_classifications(jsonb) TO anon;
GRANT EXECUTE ON FUNCTION delete_classification(text, text, text) TO anon;

-- Keep partition assignment RPCs executable; they operate on the recreated
-- public.partitions table.
GRANT EXECUTE ON FUNCTION assign_partition(text, int, int, int, int, int) TO anon;
GRANT EXECUTE ON FUNCTION assign_partition_mixed(text, int, int, int, int, int, int, int) TO anon;
GRANT EXECUTE ON FUNCTION claim_v2_slice(text, int, int, int, int) TO anon;

COMMIT;

