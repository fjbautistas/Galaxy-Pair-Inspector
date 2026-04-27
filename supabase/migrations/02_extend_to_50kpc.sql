-- ════════════════════════════════════════════════════════════════════════════
-- Migration 02: extender catálogo a rp<50 kpc, two-slice work block (v1+v2)
-- ════════════════════════════════════════════════════════════════════════════
-- Aplica esto en Supabase SQL Editor. Es idempotente: se puede correr varias
-- veces sin efectos secundarios.
--
-- Diseño:
--   • La columna `calib_v` indica si el dispositivo todavía debe la calibración
--     suplementaria (1 = legacy, 120 pares; 2 = ya completó las 270).
--   • `work_start_v2 / work_end_v2` son un segundo slice contiguo en
--     rp ∈ [20, 50). En el flujo mixto nuevo se asignan desde el registro;
--     en partitions legacy se rellenan al terminar el suplementario #150.
--   • `assign_partition_mixed` entrega bloques 65% rp<20 + 35% rp∈[20,50).
--     `assign_partition` mantiene su contrato legacy como fallback.
--   • `claim_v2_slice` es atómico (advisory lock) y maneja dos casos:
--       A. Usuario sano (slice v1 dentro del rango [120, 31976), tamaño>0):
--          extiende su trabajo con un slice v2 tal que
--          `unclassified_v1 + len(v2) = 3000`.
--       B. Usuario roto (slice v1 fuera de rango o tamaño 0) — incluye los
--          4 dispositivos vacíos por el bug histórico y cualquier usuario
--          nuevo: resetea `work_start/work_end` a un slice limpio de 3000
--          dentro de la zona v2.
-- ════════════════════════════════════════════════════════════════════════════

-- ─── 1) Columnas nuevas ─────────────────────────────────────────────────────
ALTER TABLE partitions
  ADD COLUMN IF NOT EXISTS calib_v       int DEFAULT 1,
  ADD COLUMN IF NOT EXISTS work_start_v2 int,
  ADD COLUMN IF NOT EXISTS work_end_v2   int;

COMMENT ON COLUMN partitions.calib_v       IS '1 = legacy (120 pares calib); 2 = completó calibración suplementaria (270 pares)';
COMMENT ON COLUMN partitions.work_start_v2 IS 'Inicio del slice v2 en basePairs (rp∈[20,50]); asignado al registro mixto o vía claim_v2_slice';
COMMENT ON COLUMN partitions.work_end_v2   IS 'Fin del slice v2; asignado al registro mixto o vía claim_v2_slice';

-- Garantizar que pre-existentes tengan calib_v=1 explícito
UPDATE partitions SET calib_v = 1 WHERE calib_v IS NULL;


-- ─── 2) assign_partition (drop + create para refrescar la firma RETURN) ─────
-- Nota: la tabla partitions NO tiene columna `id` — su PK efectiva es device_id.
DROP FUNCTION IF EXISTS assign_partition(text, int, int, int, int, int);

CREATE FUNCTION assign_partition(
  p_device_id        text,
  p_calib_seed       int,
  p_block_size       int,
  p_calib_size       int,
  p_group_block_size int,
  p_group_calib_size int
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
  v_row partitions%ROWTYPE;
  v_max_end       int;
  v_max_grp_end   int;
BEGIN
  -- Si ya existe, devolver tal cual
  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF FOUND THEN
    RETURN QUERY
      SELECT v_row.device_id, v_row.calib_seed,
             v_row.work_start, v_row.work_end,
             v_row.group_work_start, v_row.group_work_end,
             v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
             v_row.registered_at;
    RETURN;
  END IF;

  -- Lock global para evitar race conditions en la asignación
  PERFORM pg_advisory_xact_lock(hashtext('assign_partition'));

  -- Re-check tras el lock
  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF FOUND THEN
    RETURN QUERY
      SELECT v_row.device_id, v_row.calib_seed,
             v_row.work_start, v_row.work_end,
             v_row.group_work_start, v_row.group_work_end,
             v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
             v_row.registered_at;
    RETURN;
  END IF;

  -- Calcular topes (incluyendo cualquier work_end_v2 ya asignado)
  SELECT GREATEST(
           COALESCE(MAX(p.work_end),    p_calib_size),
           COALESCE(MAX(p.work_end_v2), p_calib_size)
         )
    INTO v_max_end
    FROM partitions p;
  SELECT COALESCE(MAX(p.group_work_end), p_group_calib_size)
    INTO v_max_grp_end
    FROM partitions p;

  -- Insertar nuevo registro. calib_v=1 → el frontend lo guiará a hacer
  -- la calibración suplementaria primero. work_start_v2/end_v2 quedan NULL
  -- y se rellenarán cuando llame a claim_v2_slice.
  INSERT INTO partitions (
    device_id, calib_seed,
    work_start, work_end,
    group_work_start, group_work_end,
    calib_v, work_start_v2, work_end_v2
  ) VALUES (
    p_device_id, p_calib_seed,
    v_max_end, v_max_end + p_block_size,
    v_max_grp_end, v_max_grp_end + p_group_block_size,
    1, NULL, NULL
  ) RETURNING * INTO v_row;

  RETURN QUERY
    SELECT v_row.device_id, v_row.calib_seed,
           v_row.work_start, v_row.work_end,
           v_row.group_work_start, v_row.group_work_end,
           v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
           v_row.registered_at;
END;
$$;

-- ─── 2b) assign_partition_mixed (NUEVO) ────────────────────────────────────
-- Asigna desde el inicio un bloque mixto de pares:
--   65% en la zona v1 (rp<20 kpc) y 35% en la zona v2 (20<=rp<50 kpc).
-- Mantiene el contrato de salida de assign_partition para que el frontend pueda
-- usar una u otra RPC con fallback.
DROP FUNCTION IF EXISTS assign_partition_mixed(text, int, int, int, int, int, int, int);

CREATE FUNCTION assign_partition_mixed(
  p_device_id        text,
  p_calib_seed       int,
  p_block_size       int,
  p_calib_size       int,
  p_group_block_size int,
  p_group_calib_size int,
  p_v1_max_idx       int DEFAULT 31976,
  p_v2_max_idx       int DEFAULT 79156
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
  v_row             partitions%ROWTYPE;
  v_v1_quota        int := ROUND(p_block_size * 0.65)::int;
  v_v2_quota        int := p_block_size - ROUND(p_block_size * 0.65)::int;
  v_v1_start        int;
  v_v1_end          int;
  v_v2_start        int;
  v_v2_end          int;
  v_max_grp_end     int;
BEGIN
  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF FOUND THEN
    RETURN QUERY
      SELECT v_row.device_id, v_row.calib_seed,
             v_row.work_start, v_row.work_end,
             v_row.group_work_start, v_row.group_work_end,
             v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
             v_row.registered_at;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('assign_partition_mixed'));

  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF FOUND THEN
    RETURN QUERY
      SELECT v_row.device_id, v_row.calib_seed,
             v_row.work_start, v_row.work_end,
             v_row.group_work_start, v_row.group_work_end,
             v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
             v_row.registered_at;
    RETURN;
  END IF;

  SELECT GREATEST(
           COALESCE(MAX(LEAST(p.work_end, p_v1_max_idx)), p_calib_size),
           p_calib_size
         )
    INTO v_v1_start
    FROM partitions p
   WHERE p.work_start < p_v1_max_idx;

  SELECT GREATEST(
           COALESCE((SELECT MAX(p.work_end_v2)
                       FROM partitions p
                      WHERE p.work_end_v2 IS NOT NULL), p_v1_max_idx),
           COALESCE((SELECT MAX(p.work_end)
                       FROM partitions p
                      WHERE p.work_start >= p_v1_max_idx), p_v1_max_idx),
           p_v1_max_idx
         )
    INTO v_v2_start;

  SELECT COALESCE(MAX(p.group_work_end), p_group_calib_size)
    INTO v_max_grp_end
    FROM partitions p;

  v_v1_end := LEAST(v_v1_start + v_v1_quota, p_v1_max_idx);
  v_v2_end := LEAST(v_v2_start + v_v2_quota, p_v2_max_idx);

  -- Si la zona v1 ya está agotada, completar en v2 para mantener ~3000 pares.
  IF v_v1_end <= v_v1_start THEN
    v_v2_end := LEAST(v_v2_start + p_block_size, p_v2_max_idx);
  END IF;

  INSERT INTO partitions (
    device_id, calib_seed,
    work_start, work_end,
    group_work_start, group_work_end,
    calib_v, work_start_v2, work_end_v2
  ) VALUES (
    p_device_id, p_calib_seed,
    v_v1_start, v_v1_end,
    v_max_grp_end, v_max_grp_end + p_group_block_size,
    1, v_v2_start, v_v2_end
  ) RETURNING * INTO v_row;

  RETURN QUERY
    SELECT v_row.device_id, v_row.calib_seed,
           v_row.work_start, v_row.work_end,
           v_row.group_work_start, v_row.group_work_end,
           v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
           v_row.registered_at;
END;
$$;


-- ─── 3) claim_v2_slice (NUEVO) ──────────────────────────────────────────────
-- Llamado por el frontend cuando un dispositivo termina las 150 calibraciones
-- suplementarias. Devuelve la partition completa actualizada.
--
-- Argumentos:
--   p_device_id     : alias del dispositivo
--   p_target_total  : tamaño deseado del work block total (default 3000)
--   p_n_classif_v1  : pares ya clasificados por este device DENTRO de su slice
--                     v1 actual (lo calcula el frontend porque conoce el catálogo).
--                     Solo se usa en CASO A (sano).
--   p_v1_max_idx    : tope superior de la zona v1 (= n_pairs_v1 del catálogo)
--   p_v2_max_idx    : tope superior global (= total_pairs)

DROP FUNCTION IF EXISTS claim_v2_slice(text, int, int, int, int);

CREATE FUNCTION claim_v2_slice(
  p_device_id     text,
  p_target_total  int DEFAULT 3000,
  p_n_classif_v1  int DEFAULT 0,
  p_v1_max_idx    int DEFAULT 31976,
  p_v2_max_idx    int DEFAULT 79156
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
  v_row         partitions%ROWTYPE;
  v_v1_len      int;
  v_v1_in_range bool;
  v_v1_quota    int := ROUND(p_target_total * 0.65)::int;
  v_v2_quota    int := p_target_total - ROUND(p_target_total * 0.65)::int;
  v_v1_start    int;
  v_v1_end      int;
  v_v2_start    int;
  v_v2_end      int;
  v_zone_top    int;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('claim_v2_slice'));

  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'claim_v2_slice: device % no tiene partition', p_device_id;
  END IF;

  -- Idempotencia: si ya tiene calib_v=2, devolver lo que ya está
  IF v_row.calib_v = 2 THEN
    RETURN QUERY
      SELECT v_row.device_id, v_row.calib_v,
             v_row.work_start, v_row.work_end,
             v_row.work_start_v2, v_row.work_end_v2;
    RETURN;
  END IF;

  -- Si el dispositivo ya tiene slice v2 asignado por assign_partition_mixed,
  -- completar la calibración solo cambia calib_v.
  IF v_row.work_start_v2 IS NOT NULL AND v_row.work_end_v2 IS NOT NULL THEN
    UPDATE partitions
       SET calib_v = 2
     WHERE partitions.device_id = p_device_id
     RETURNING * INTO v_row;

    RETURN QUERY
      SELECT v_row.device_id, v_row.calib_v,
             v_row.work_start, v_row.work_end,
             v_row.work_start_v2, v_row.work_end_v2;
    RETURN;
  END IF;

  v_v1_len := v_row.work_end - v_row.work_start;
  v_v1_in_range := (v_row.work_end <= p_v1_max_idx) AND (v_v1_len > 0);

  -- Tope ocupado en la zona v2 (combinando work_end_v2 de sanos + work_end de rotos en zona v2)
  SELECT GREATEST(
           COALESCE((SELECT MAX(p.work_end_v2)
                       FROM partitions p
                      WHERE p.work_end_v2 IS NOT NULL), p_v1_max_idx),
           COALESCE((SELECT MAX(p.work_end)
                       FROM partitions p
                      WHERE p.work_start >= p_v1_max_idx), p_v1_max_idx),
           p_v1_max_idx
         )
    INTO v_zone_top;

  IF v_v1_in_range THEN
    -- ─── CASO A: usuario sano legacy → normalizar a bloque mixto 65/35 ─────
    v_v2_start := v_zone_top;
    v_v2_end   := LEAST(v_zone_top + v_v2_quota, p_v2_max_idx);

    UPDATE partitions
       SET calib_v       = 2,
           work_end      = LEAST(v_row.work_start + v_v1_quota, p_v1_max_idx),
           work_start_v2 = v_v2_start,
           work_end_v2   = v_v2_end
     WHERE partitions.device_id = p_device_id
     RETURNING * INTO v_row;
  ELSE
    -- ─── CASO B: usuario roto / creado en v2 → nuevo bloque mixto ──────────
    SELECT GREATEST(
             COALESCE(MAX(LEAST(p.work_end, p_v1_max_idx)), 120),
             120
           )
      INTO v_v1_start
      FROM partitions p
     WHERE p.work_start < p_v1_max_idx;
    v_v1_end   := LEAST(v_v1_start + v_v1_quota, p_v1_max_idx);
    v_v2_start := v_zone_top;
    v_v2_end   := LEAST(v_zone_top + v_v2_quota, p_v2_max_idx);

    IF v_v1_end <= v_v1_start THEN
      v_v2_end := LEAST(v_v2_start + p_target_total, p_v2_max_idx);
    END IF;

    UPDATE partitions
       SET calib_v       = 2,
           work_start    = v_v1_start,
           work_end      = v_v1_end,
           work_start_v2 = v_v2_start,
           work_end_v2   = v_v2_end
     WHERE partitions.device_id = p_device_id
     RETURNING * INTO v_row;
  END IF;

  RETURN QUERY
    SELECT v_row.device_id, v_row.calib_v,
           v_row.work_start, v_row.work_end,
           v_row.work_start_v2, v_row.work_end_v2;
END;
$$;


-- ─── 4) Permisos: que anon pueda llamar las RPC ─────────────────────────────
GRANT EXECUTE ON FUNCTION assign_partition(text, int, int, int, int, int) TO anon;
GRANT EXECUTE ON FUNCTION assign_partition_mixed(text, int, int, int, int, int, int, int) TO anon;
GRANT EXECUTE ON FUNCTION claim_v2_slice(text, int, int, int, int)        TO anon;


-- ─── 5) Verificación rápida (correr aparte, no es parte de la migración) ────
-- SELECT device_id, calib_v, work_start, work_end, work_start_v2, work_end_v2
--   FROM partitions
--   ORDER BY device_id;
