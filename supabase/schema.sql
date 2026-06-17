-- Current Galaxy Pair Inspector Supabase schema.
-- This file describes the active app contract as a baseline schema.

BEGIN;

CREATE OR REPLACE FUNCTION public._is_valid_device_id(p_device_id text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_device_id ~ '^[A-Z0-9_]{3,20}$';
$$;

CREATE OR REPLACE FUNCTION public._is_valid_classification(p_classification text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_classification IN ('FP', 'Pair', 'PM', 'GROUP', 'PP');
$$;

CREATE OR REPLACE FUNCTION public._is_valid_item_type(p_item_type text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_item_type IN ('pair', 'group');
$$;

CREATE TABLE IF NOT EXISTS public.clasificaciones (
  id               bigserial PRIMARY KEY,
  device_id        text        NOT NULL,
  item_type        text        NOT NULL CHECK (item_type IN ('pair', 'group')),
  item_uid         text        NOT NULL,
  pair_uid         text,
  stable_system_id text,
  id_par_v5        integer,
  classification   text        NOT NULL CHECK (classification IN ('FP', 'Pair', 'PM', 'GROUP', 'PP')),
  source           text        NOT NULL DEFAULT 'app',
  exported_at      timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (device_id, item_type, item_uid)
);

CREATE INDEX IF NOT EXISTS clasificaciones_item_uid_idx
  ON public.clasificaciones(item_type, item_uid);

CREATE INDEX IF NOT EXISTS clasificaciones_pair_uid_idx
  ON public.clasificaciones(pair_uid)
  WHERE pair_uid IS NOT NULL;

CREATE INDEX IF NOT EXISTS clasificaciones_device_idx
  ON public.clasificaciones(device_id);

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

DROP POLICY IF EXISTS classifications_public_read ON public.clasificaciones;
DROP POLICY IF EXISTS partitions_public_read ON public.partitions;

CREATE POLICY classifications_public_read
  ON public.clasificaciones
  FOR SELECT
  USING (true);

CREATE POLICY partitions_public_read
  ON public.partitions
  FOR SELECT
  USING (true);

GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON public.clasificaciones TO anon;
GRANT SELECT ON public.partitions TO anon;
GRANT USAGE, SELECT ON SEQUENCE public.clasificaciones_id_seq TO anon;

CREATE OR REPLACE FUNCTION public.get_device_classifications(p_device_id text)
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
  IF NOT public._is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;

  RETURN QUERY
  SELECT c.item_type, c.item_uid, c.pair_uid, c.stable_system_id,
         c.id_par_v5, c.classification
    FROM public.clasificaciones c
   WHERE c.device_id = p_device_id
   ORDER BY c.item_type, c.item_uid;
END;
$$;

CREATE OR REPLACE FUNCTION public.upsert_classification(
  p_device_id        text,
  p_item_type        text,
  p_item_uid         text,
  p_classification   text,
  p_pair_uid         text DEFAULT NULL,
  p_stable_system_id text DEFAULT NULL,
  p_id_par_v5        integer DEFAULT NULL,
  p_exported_at      timestamptz DEFAULT now(),
  p_source           text DEFAULT 'app'
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT public._is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;
  IF NOT public._is_valid_item_type(p_item_type) THEN
    RAISE EXCEPTION 'Invalid item_type';
  END IF;
  IF p_item_uid IS NULL OR length(p_item_uid) < 3 OR length(p_item_uid) > 128 THEN
    RAISE EXCEPTION 'Invalid item_uid';
  END IF;
  IF NOT public._is_valid_classification(p_classification) THEN
    RAISE EXCEPTION 'Invalid classification';
  END IF;
  IF p_item_type = 'pair' AND (p_pair_uid IS NULL OR p_pair_uid <> p_item_uid) THEN
    RAISE EXCEPTION 'pair classifications require pair_uid = item_uid';
  END IF;
  IF p_item_type = 'group' AND p_stable_system_id IS NULL THEN
    RAISE EXCEPTION 'group classifications require stable_system_id';
  END IF;

  INSERT INTO public.clasificaciones(
    device_id, item_type, item_uid, pair_uid, stable_system_id,
    id_par_v5, classification, source, exported_at
  )
  VALUES (
    p_device_id, p_item_type, p_item_uid, p_pair_uid, p_stable_system_id,
    p_id_par_v5, p_classification, COALESCE(p_source, 'app'),
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

CREATE OR REPLACE FUNCTION public.upsert_classifications(p_rows jsonb)
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
     WHERE NOT public._is_valid_device_id(device_id)
        OR NOT public._is_valid_item_type(item_type)
        OR item_uid IS NULL OR length(item_uid) < 3 OR length(item_uid) > 128
        OR NOT public._is_valid_classification(classification)
        OR (item_type = 'pair' AND (pair_uid IS NULL OR pair_uid <> item_uid))
        OR (item_type = 'group' AND stable_system_id IS NULL)
  ) THEN
    RAISE EXCEPTION 'Invalid row in p_rows';
  END IF;

  INSERT INTO public.clasificaciones(
    device_id, item_type, item_uid, pair_uid, stable_system_id,
    id_par_v5, classification, source, exported_at
  )
  SELECT device_id, item_type, item_uid, pair_uid, stable_system_id,
         id_par_v5, classification, COALESCE(source, 'app'),
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

CREATE OR REPLACE FUNCTION public.delete_classification(
  p_device_id text,
  p_item_type text,
  p_item_uid  text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT public._is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;
  IF NOT public._is_valid_item_type(p_item_type) THEN
    RAISE EXCEPTION 'Invalid item_type';
  END IF;
  IF p_item_uid IS NULL OR length(p_item_uid) < 3 OR length(p_item_uid) > 128 THEN
    RAISE EXCEPTION 'Invalid item_uid';
  END IF;

  DELETE FROM public.clasificaciones
   WHERE device_id = p_device_id
     AND item_type = p_item_type
     AND item_uid = p_item_uid;
END;
$$;

CREATE OR REPLACE FUNCTION public.assign_partition_mixed(
  p_device_id        text,
  p_calib_seed       int,
  p_block_size       int,
  p_calib_size       int,
  p_group_block_size int,
  p_group_calib_size int,
  p_v1_max_idx       int,
  p_v2_max_idx       int
) RETURNS public.partitions
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_existing public.partitions;
  v_last_v1 int;
  v_last_v2 int;
  v_last_group int;
  v_q1 int;
  v_q2 int;
BEGIN
  IF NOT public._is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;

  SELECT * INTO v_existing
    FROM public.partitions
   WHERE device_id = p_device_id;
  IF FOUND THEN
    RETURN v_existing;
  END IF;

  v_q1 := round(p_block_size * 0.50);
  v_q2 := p_block_size - v_q1;

  SELECT COALESCE(max(work_end), p_calib_size)
    INTO v_last_v1
    FROM public.partitions
   WHERE work_start < p_v1_max_idx;
  v_last_v1 := greatest(p_calib_size, least(v_last_v1, p_v1_max_idx));

  SELECT COALESCE(max(COALESCE(work_end_v2, work_end)), p_v1_max_idx)
    INTO v_last_v2
    FROM public.partitions
   WHERE COALESCE(work_start_v2, work_start) >= p_v1_max_idx;
  v_last_v2 := greatest(p_v1_max_idx, least(v_last_v2, p_v2_max_idx));

  SELECT COALESCE(max(group_work_end), p_group_calib_size)
    INTO v_last_group
    FROM public.partitions;

  INSERT INTO public.partitions(
    device_id, calib_seed, work_start, work_end,
    group_work_start, group_work_end,
    calib_v, work_start_v2, work_end_v2,
    n_v1, n_v2
  )
  VALUES (
    p_device_id, p_calib_seed, v_last_v1, least(v_last_v1 + v_q1, p_v1_max_idx),
    v_last_group, v_last_group + p_group_block_size,
    2, v_last_v2, least(v_last_v2 + v_q2, p_v2_max_idx),
    p_v1_max_idx, p_v2_max_idx - p_v1_max_idx
  )
  RETURNING * INTO v_existing;

  RETURN v_existing;
END;
$$;

CREATE OR REPLACE FUNCTION public.assign_partition(
  p_device_id        text,
  p_calib_seed       int,
  p_block_size       int,
  p_calib_size       int,
  p_group_block_size int DEFAULT 100,
  p_group_calib_size int DEFAULT 80
) RETURNS public.partitions
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  RETURN public.assign_partition_mixed(
    p_device_id, p_calib_seed, p_block_size, p_calib_size,
    p_group_block_size, p_group_calib_size,
    p_calib_size + p_block_size, p_calib_size + p_block_size
  );
END;
$$;

GRANT EXECUTE ON FUNCTION public.get_device_classifications(text) TO anon;
GRANT EXECUTE ON FUNCTION public.upsert_classification(text, text, text, text, text, text, integer, timestamptz, text) TO anon;
GRANT EXECUTE ON FUNCTION public.upsert_classifications(jsonb) TO anon;
GRANT EXECUTE ON FUNCTION public.delete_classification(text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION public.assign_partition_mixed(text, int, int, int, int, int, int, int) TO anon;
GRANT EXECUTE ON FUNCTION public.assign_partition(text, int, int, int, int, int) TO anon;

COMMIT;
