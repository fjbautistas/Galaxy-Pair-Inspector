-- ════════════════════════════════════════════════════════════════════════════
-- Migration 05: rebalance a 1450 items por usuario
-- ════════════════════════════════════════════════════════════════════════════
-- Objetivo mobile:
--   120 pares calibración base
-- + 150 pares calibración suplementaria
-- +  80 grupos calibración
-- +1000 pares de trabajo
-- + 100 grupos de trabajo
-- =1450 items por usuario.
--
-- Política:
--   • Mantener solo los usuarios activos indicados por auditoría.
--   • Para activos existentes, conservar su bloque histórico recortado a los
--     primeros 1000 pares para no reemplazar masivamente lo ya visto.
--   • Para nuevos usuarios, asignar trabajo 50/50:
--       500 pares rp<20 kpc + 500 pares 20<=rp<50 kpc.
--   • Grupos de trabajo: 100 por usuario, sin solapamiento mientras haya pool.
-- ════════════════════════════════════════════════════════════════════════════

-- ─── 1) Retirar particiones inactivas/no auditadas ───────────────────────
WITH active_devices(device_id, ord) AS (
  VALUES
    ('DESKTOP', 1),
    ('EZ0TD',  2),
    ('2BO6V',  3),
    ('QD8RH',  4),
    ('N1POT',  5),
    ('FAFPT',  6),
    ('HVYPD',  7),
    ('YKUGT',  8),
    ('MF4CG',  9),
    ('2M2PR', 10),
    ('N9JBB', 11),
    ('0N0KF', 12),
    ('QB7B3', 13)
)
DELETE FROM partitions p
 WHERE NOT EXISTS (
   SELECT 1 FROM active_devices a WHERE a.device_id = p.device_id
 );


-- ─── 2) Rebalancear activos: 1000 pares históricos + 100 grupos ──────────
WITH active_devices(device_id, ord) AS (
  VALUES
    ('DESKTOP', 1),
    ('EZ0TD',  2),
    ('2BO6V',  3),
    ('QD8RH',  4),
    ('N1POT',  5),
    ('FAFPT',  6),
    ('HVYPD',  7),
    ('YKUGT',  8),
    ('MF4CG',  9),
    ('2M2PR', 10),
    ('N9JBB', 11),
    ('0N0KF', 12),
    ('QB7B3', 13)
)
UPDATE partitions p
   SET work_end         = LEAST(p.work_start + 1000, p.work_end),
       work_start_v2    = NULL,
       work_end_v2      = NULL,
       group_work_start = 80 + ((a.ord - 1) * 100),
       group_work_end   = 80 + (a.ord * 100)
  FROM active_devices a
 WHERE p.device_id = a.device_id;

-- Algunas bases tienen columnas informativas n_v1/n_v2/n_groups creadas
-- manualmente. Si existen, mantenerlas consistentes para la auditoría visual.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_v1'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_v1 = work_end - work_start WHERE work_start_v2 IS NULL AND work_end_v2 IS NULL';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_v2'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_v2 = NULL WHERE work_start_v2 IS NULL AND work_end_v2 IS NULL';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_groups'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_groups = group_work_end - group_work_start WHERE group_work_start IS NOT NULL AND group_work_end IS NOT NULL';
  END IF;
END;
$$;


-- ─── 3) assign_partition_mixed: nuevos usuarios 1000 pares 50/50 ─────────
DROP FUNCTION IF EXISTS assign_partition_mixed(text, int, int, int, int, int, int, int);

CREATE FUNCTION assign_partition_mixed(
  p_device_id        text,
  p_calib_seed       int,
  p_block_size       int,
  p_calib_size       int,
  p_group_block_size int,
  p_group_calib_size int,
  p_v1_max_idx       int,
  p_v2_max_idx       int
) RETURNS TABLE (
  device_id          text,
  calib_seed         int,
  work_start         int,
  work_end           int,
  group_work_start   int,
  group_work_end     int,
  calib_v            int,
  work_start_v2      int,
  work_end_v2        int,
  registered_at      timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_row              partitions%ROWTYPE;
  v_q_v1             int := ROUND(p_block_size * 0.50)::int;
  v_q_v2             int := p_block_size - ROUND(p_block_size * 0.50)::int;
  v_v1_start         int;
  v_v1_end           int;
  v_v2_start         int;
  v_v2_end           int;
  v_group_start      int;
  v_group_end        int;
BEGIN
  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF FOUND THEN
    RETURN QUERY SELECT v_row.device_id, v_row.calib_seed,
                        v_row.work_start, v_row.work_end,
                        v_row.group_work_start, v_row.group_work_end,
                        v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
                        v_row.registered_at;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('assign_partition_mixed'));

  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF FOUND THEN
    RETURN QUERY SELECT v_row.device_id, v_row.calib_seed,
                        v_row.work_start, v_row.work_end,
                        v_row.group_work_start, v_row.group_work_end,
                        v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
                        v_row.registered_at;
    RETURN;
  END IF;

  -- Buscar el primer hueco libre en v1. Esto reutiliza espacios dejados por
  -- particiones inactivas eliminadas, en vez de saltar al MAX(work_end).
  v_v1_start := p_calib_size;
  LOOP
    v_v1_end := v_v1_start + v_q_v1;
    IF v_v1_end > p_v1_max_idx THEN
      RAISE EXCEPTION 'No quedan suficientes pares rp<20 para asignar % items v1', v_q_v1;
    END IF;

    EXIT WHEN NOT EXISTS (
      SELECT 1
        FROM partitions p
       WHERE p.work_start < p_v1_max_idx
         AND int4range(v_v1_start, v_v1_end, '[)')
             && int4range(p.work_start, LEAST(p.work_end, p_v1_max_idx), '[)')
    );

    v_v1_start := v_v1_start + v_q_v1;
  END LOOP;

  -- Buscar el primer hueco libre en v2, considerando slices v2 explícitos y
  -- bloques históricos que ya estaban completamente en rp>=20.
  v_v2_start := p_v1_max_idx;
  LOOP
    v_v2_end := v_v2_start + v_q_v2;
    IF v_v2_end > p_v2_max_idx THEN
      RAISE EXCEPTION 'No quedan suficientes pares rp>=20 para asignar % items v2', v_q_v2;
    END IF;

    EXIT WHEN NOT EXISTS (
      SELECT 1
       FROM partitions p
       WHERE (
          p.work_start_v2 IS NOT NULL
          AND p.work_end_v2 IS NOT NULL
          AND int4range(v_v2_start, v_v2_end, '[)')
              && int4range(p.work_start_v2, p.work_end_v2, '[)')
       ) OR (
          p.work_start >= p_v1_max_idx
          AND int4range(v_v2_start, v_v2_end, '[)')
              && int4range(p.work_start, p.work_end, '[)')
       )
    );

    v_v2_start := v_v2_start + v_q_v2;
  END LOOP;

  SELECT COALESCE(MAX(p.group_work_end), p_group_calib_size)
    INTO v_group_start
    FROM partitions p
   WHERE p.group_work_end IS NOT NULL;
  v_group_end := v_group_start + p_group_block_size;

  INSERT INTO partitions (
    device_id, calib_seed,
    work_start, work_end,
    group_work_start, group_work_end,
    calib_v, work_start_v2, work_end_v2
  ) VALUES (
    p_device_id, p_calib_seed,
    v_v1_start, v_v1_end,
    v_group_start, v_group_end,
    1, v_v2_start, v_v2_end
  ) RETURNING * INTO v_row;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_v1'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_v1 = $1 WHERE device_id = $2'
      USING v_q_v1, p_device_id;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_v2'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_v2 = $1 WHERE device_id = $2'
      USING v_q_v2, p_device_id;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_groups'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_groups = $1 WHERE device_id = $2'
      USING p_group_block_size, p_device_id;
  END IF;

  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;

  RETURN QUERY SELECT v_row.device_id, v_row.calib_seed,
                      v_row.work_start, v_row.work_end,
                      v_row.group_work_start, v_row.group_work_end,
                      v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
                      v_row.registered_at;
END;
$$;


-- ─── 4) claim_v2_slice: no alterar activos ya recortados a 1000 ──────────
DROP FUNCTION IF EXISTS claim_v2_slice(text, int, int, int, int);

CREATE FUNCTION claim_v2_slice(
  p_device_id      text,
  p_target_total   int,
  p_n_classif_v1   int,
  p_v1_max_idx     int,
  p_v2_max_idx     int
) RETURNS TABLE (
  device_id        text,
  calib_v          int,
  work_start       int,
  work_end         int,
  work_start_v2    int,
  work_end_v2      int
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_row        partitions%ROWTYPE;
  v_q_v1       int := ROUND(p_target_total * 0.50)::int;
  v_q_v2       int := p_target_total - ROUND(p_target_total * 0.50)::int;
  v_v2_start   int;
  v_v2_end     int;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('claim_v2_slice'));

  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'claim_v2_slice: device % no tiene partition', p_device_id;
  END IF;

  IF v_row.calib_v = 2 THEN
    RETURN QUERY SELECT v_row.device_id, v_row.calib_v,
                        v_row.work_start, v_row.work_end,
                        v_row.work_start_v2, v_row.work_end_v2;
    RETURN;
  END IF;

  -- Nuevo esquema: si ya trae slice v2, solo cerrar calibración suplementaria.
  IF v_row.work_start_v2 IS NOT NULL AND v_row.work_end_v2 IS NOT NULL THEN
    UPDATE partitions
       SET calib_v = 2
     WHERE partitions.device_id = p_device_id
     RETURNING * INTO v_row;

    RETURN QUERY SELECT v_row.device_id, v_row.calib_v,
                        v_row.work_start, v_row.work_end,
                        v_row.work_start_v2, v_row.work_end_v2;
    RETURN;
  END IF;

  -- Activos rebalanceados: conservar bloque histórico recortado a 1000 pares.
  IF (v_row.work_end - v_row.work_start) <= p_target_total THEN
    UPDATE partitions
       SET calib_v = 2
     WHERE partitions.device_id = p_device_id
     RETURNING * INTO v_row;

    RETURN QUERY SELECT v_row.device_id, v_row.calib_v,
                        v_row.work_start, v_row.work_end,
                        v_row.work_start_v2, v_row.work_end_v2;
    RETURN;
  END IF;

  -- Fallback para particiones legacy que no hayan pasado por el rebalance.
  UPDATE partitions
     SET work_end = LEAST(work_start + v_q_v1, p_v1_max_idx)
   WHERE partitions.device_id = p_device_id
   RETURNING * INTO v_row;

  SELECT GREATEST(
           COALESCE(MAX(p.work_end_v2), p_v1_max_idx),
           COALESCE(MAX(CASE WHEN p.work_start >= p_v1_max_idx THEN p.work_end END), p_v1_max_idx),
           p_v1_max_idx
         )
    INTO v_v2_start
    FROM partitions p;

  v_v2_end := v_v2_start + v_q_v2;
  IF v_v2_end > p_v2_max_idx THEN
    RAISE EXCEPTION 'No quedan suficientes pares rp>=20 para claim_v2_slice';
  END IF;

  UPDATE partitions
     SET calib_v       = 2,
         work_start_v2 = v_v2_start,
         work_end_v2   = v_v2_end
   WHERE partitions.device_id = p_device_id
   RETURNING * INTO v_row;

  RETURN QUERY SELECT v_row.device_id, v_row.calib_v,
                      v_row.work_start, v_row.work_end,
                      v_row.work_start_v2, v_row.work_end_v2;
END;
$$;


-- ─── 5) Permisos ─────────────────────────────────────────────────────────
GRANT EXECUTE ON FUNCTION assign_partition_mixed(text, int, int, int, int, int, int, int) TO anon;
GRANT EXECUTE ON FUNCTION claim_v2_slice(text, int, int, int, int) TO anon;


-- ─── 6) Verificación sugerida ────────────────────────────────────────────
-- SELECT device_id, calib_v, work_start, work_end,
--        work_start_v2, work_end_v2,
--        group_work_start, group_work_end,
--        (work_end - work_start) AS n_pairs_primary,
--        COALESCE(work_end_v2 - work_start_v2, 0) AS n_pairs_v2,
--        (group_work_end - group_work_start) AS n_groups
--   FROM partitions
--   ORDER BY registered_at;
