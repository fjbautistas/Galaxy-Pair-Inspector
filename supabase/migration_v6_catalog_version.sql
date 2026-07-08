-- ════════════════════════════════════════════════════════════════════════════
-- Migración: namespacing de votos y particiones por `catalog_version` (opción A).
--
-- Objetivo: que TODOS arranquen la campaña v6 en el total completo (1500) sin
-- perder nada. El histórico se conserva etiquetado como 'v5_3'; la campaña v6
-- vive en su propio espacio. Un mismo par puede tener voto en v5_3 y en v6.
--
-- Correr UNA sola vez en el SQL Editor de Supabase (todo en una transacción).
-- Después de correrla, regenerar y desplegar GalPairs.html (la app manda 'v6').
-- ════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. clasificaciones: columna + backfill histórico + UNIQUE con versión ─────
-- El DEFAULT 'v5_3' hace el backfill de todas las filas existentes.
ALTER TABLE public.clasificaciones
  ADD COLUMN IF NOT EXISTS catalog_version text NOT NULL DEFAULT 'v5_3';

-- La UNIQUE pasa a incluir la versión: mismo (device,item) en distintos catálogos
-- son filas independientes → no se pisan, no se borra el histórico.
-- Elimina cualquier UNIQUE previa de la tabla (sin asumir su nombre autogenerado).
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT con.conname
      FROM pg_constraint con
      JOIN pg_class rel ON rel.oid = con.conrelid
      JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
     WHERE nsp.nspname = 'public'
       AND rel.relname = 'clasificaciones'
       AND con.contype = 'u'
  LOOP
    EXECUTE format('ALTER TABLE public.clasificaciones DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

ALTER TABLE public.clasificaciones
  ADD CONSTRAINT clasificaciones_device_item_version_key
  UNIQUE (device_id, item_type, item_uid, catalog_version);

-- ── 2. partitions: columna + PK compuesta (device_id, catalog_version) ────────
-- Cada device re-dibuja partición fresca por versión; el empaquetado de bloques
-- solo mira la versión actual (no cuenta particiones viejas de v5_3).
ALTER TABLE public.partitions
  ADD COLUMN IF NOT EXISTS catalog_version text NOT NULL DEFAULT 'v5_3';

-- Elimina TODAS las constraints PK/UNIQUE de la tabla (sin asumir sus nombres): así
-- se quita la unicidad vieja sobre device_id solo. Evita "multiple primary keys".
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT con.conname
      FROM pg_constraint con
      JOIN pg_class rel ON rel.oid = con.conrelid
      JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
     WHERE nsp.nspname = 'public'
       AND rel.relname = 'partitions'
       AND con.contype IN ('p', 'u')
  LOOP
    EXECUTE format('ALTER TABLE public.partitions DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

-- Nombre NUEVO para la PK compuesta: el nombre 'partitions_pkey' ya lo ocupa la PK
-- de otra tabla (partitions_v3_archive) y los nombres de índice son únicos por schema.
ALTER TABLE public.partitions
  ADD CONSTRAINT partitions_device_version_pkey PRIMARY KEY (device_id, catalog_version);

-- ── 3. get_device_classifications: filtra por versión ────────────────────────
DROP FUNCTION IF EXISTS public.get_device_classifications(text);

CREATE OR REPLACE FUNCTION public.get_device_classifications(
  p_device_id       text,
  p_catalog_version text DEFAULT 'v5_3'
)
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
     AND c.catalog_version = p_catalog_version
   ORDER BY c.item_type, c.item_uid;
END;
$$;

-- ── 4. upsert_classification: guarda con versión ─────────────────────────────
DROP FUNCTION IF EXISTS public.upsert_classification(
  text, text, text, text, text, text, integer, timestamptz, text);

CREATE OR REPLACE FUNCTION public.upsert_classification(
  p_device_id        text,
  p_item_type        text,
  p_item_uid         text,
  p_classification   text,
  p_pair_uid         text DEFAULT NULL,
  p_stable_system_id text DEFAULT NULL,
  p_id_par_v5        integer DEFAULT NULL,
  p_catalog_version  text DEFAULT 'v5_3',
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
    id_par_v5, classification, catalog_version, source, exported_at
  )
  VALUES (
    p_device_id, p_item_type, p_item_uid, p_pair_uid, p_stable_system_id,
    p_id_par_v5, p_classification, COALESCE(p_catalog_version, 'v5_3'),
    COALESCE(p_source, 'app'), COALESCE(p_exported_at, now())
  )
  ON CONFLICT (device_id, item_type, item_uid, catalog_version)
  DO UPDATE SET
    pair_uid         = EXCLUDED.pair_uid,
    stable_system_id = EXCLUDED.stable_system_id,
    id_par_v5        = EXCLUDED.id_par_v5,
    classification   = EXCLUDED.classification,
    source           = EXCLUDED.source,
    exported_at      = EXCLUDED.exported_at;
END;
$$;

-- ── 5. upsert_classifications (batch): idem, lee catalog_version del JSON ─────
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
    catalog_version  text,
    source           text,
    exported_at      timestamptz
  ) ON COMMIT DROP;

  INSERT INTO _rows_to_upsert(
    device_id, item_type, item_uid, pair_uid, stable_system_id,
    id_par_v5, classification, catalog_version, source, exported_at
  )
  SELECT device_id, item_type, item_uid, pair_uid, stable_system_id,
         id_par_v5, classification, COALESCE(catalog_version, 'v5_3'),
         source, exported_at
    FROM jsonb_to_recordset(p_rows)
         AS x(
           device_id text, item_type text, item_uid text, pair_uid text,
           stable_system_id text, id_par_v5 integer, classification text,
           catalog_version text, source text, exported_at timestamptz
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
    id_par_v5, classification, catalog_version, source, exported_at
  )
  SELECT device_id, item_type, item_uid, pair_uid, stable_system_id,
         id_par_v5, classification, catalog_version, COALESCE(source, 'app'),
         COALESCE(exported_at, now())
    FROM _rows_to_upsert
  ON CONFLICT (device_id, item_type, item_uid, catalog_version)
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

-- ── 6. delete_classification: apunta a la fila de la versión ─────────────────
DROP FUNCTION IF EXISTS public.delete_classification(text, text, text);

CREATE OR REPLACE FUNCTION public.delete_classification(
  p_device_id       text,
  p_item_type       text,
  p_item_uid        text,
  p_catalog_version text DEFAULT 'v5_3'
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
     AND item_uid = p_item_uid
     AND catalog_version = p_catalog_version;
END;
$$;

-- ── 7. assign_partition_mixed: partición y empaquetado por versión ───────────
DROP FUNCTION IF EXISTS public.assign_partition_mixed(
  text, int, int, int, int, int, int, int);

CREATE OR REPLACE FUNCTION public.assign_partition_mixed(
  p_device_id        text,
  p_calib_seed       int,
  p_block_size       int,
  p_calib_size       int,
  p_group_block_size int,
  p_group_calib_size int,
  p_v1_max_idx       int,
  p_v2_max_idx       int,
  p_catalog_version  text DEFAULT 'v5_3'
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
   WHERE device_id = p_device_id
     AND catalog_version = p_catalog_version;
  IF FOUND THEN
    RETURN v_existing;
  END IF;

  v_q1 := round(p_block_size * 0.50);
  v_q2 := p_block_size - v_q1;

  SELECT COALESCE(max(work_end), p_calib_size)
    INTO v_last_v1
    FROM public.partitions
   WHERE catalog_version = p_catalog_version
     AND work_start < p_v1_max_idx;
  v_last_v1 := greatest(p_calib_size, least(v_last_v1, p_v1_max_idx));

  SELECT COALESCE(max(COALESCE(work_end_v2, work_end)), p_v1_max_idx)
    INTO v_last_v2
    FROM public.partitions
   WHERE catalog_version = p_catalog_version
     AND COALESCE(work_start_v2, work_start) >= p_v1_max_idx;
  v_last_v2 := greatest(p_v1_max_idx, least(v_last_v2, p_v2_max_idx));

  SELECT COALESCE(max(group_work_end), p_group_calib_size)
    INTO v_last_group
    FROM public.partitions
   WHERE catalog_version = p_catalog_version;

  INSERT INTO public.partitions(
    device_id, catalog_version, calib_seed, work_start, work_end,
    group_work_start, group_work_end,
    calib_v, work_start_v2, work_end_v2,
    n_v1, n_v2
  )
  VALUES (
    p_device_id, p_catalog_version, p_calib_seed,
    v_last_v1, least(v_last_v1 + v_q1, p_v1_max_idx),
    v_last_group, v_last_group + p_group_block_size,
    2, v_last_v2, least(v_last_v2 + v_q2, p_v2_max_idx),
    p_v1_max_idx, p_v2_max_idx - p_v1_max_idx
  )
  RETURNING * INTO v_existing;

  RETURN v_existing;
END;
$$;

-- ── 8. assign_partition (wrapper): pasa la versión ───────────────────────────
DROP FUNCTION IF EXISTS public.assign_partition(text, int, int, int, int, int);

CREATE OR REPLACE FUNCTION public.assign_partition(
  p_device_id        text,
  p_calib_seed       int,
  p_block_size       int,
  p_calib_size       int,
  p_group_block_size int DEFAULT 100,
  p_group_calib_size int DEFAULT 80,
  p_catalog_version  text DEFAULT 'v5_3'
) RETURNS public.partitions
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  RETURN public.assign_partition_mixed(
    p_device_id, p_calib_seed, p_block_size, p_calib_size,
    p_group_block_size, p_group_calib_size,
    p_calib_size + p_block_size, p_calib_size + p_block_size,
    p_catalog_version
  );
END;
$$;

-- ── 9. GRANTs para las nuevas firmas ─────────────────────────────────────────
GRANT EXECUTE ON FUNCTION public.get_device_classifications(text, text) TO anon;
GRANT EXECUTE ON FUNCTION public.upsert_classification(
  text, text, text, text, text, text, integer, text, timestamptz, text) TO anon;
GRANT EXECUTE ON FUNCTION public.upsert_classifications(jsonb) TO anon;
GRANT EXECUTE ON FUNCTION public.delete_classification(text, text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION public.assign_partition_mixed(
  text, int, int, int, int, int, int, int, text) TO anon;
GRANT EXECUTE ON FUNCTION public.assign_partition(
  text, int, int, int, int, int, text) TO anon;

COMMIT;

-- ── Verificación rápida (correr aparte, opcional) ────────────────────────────
-- SELECT catalog_version, count(*) FROM public.clasificaciones GROUP BY 1;
-- SELECT catalog_version, count(*) FROM public.partitions      GROUP BY 1;
