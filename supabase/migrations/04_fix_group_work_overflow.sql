-- ════════════════════════════════════════════════════════════════════════════
-- Migration 04: corregir overflow de group_work_start/end
-- ════════════════════════════════════════════════════════════════════════════
-- Aplica esto en Supabase SQL Editor. Idempotente.
--
-- Bug: assign_partition asignaba group_work_start = MAX(group_work_end) +
-- group_block_size (500) por usuario, pero el catálogo de grupos solo tiene
-- ~3800 entradas únicas. A partir del usuario ~7, group_work_start excede
-- la longitud real del array → el frontend hacía slice fuera de rango y
-- workGroups quedaba vacío → cero grupos en el work block.
--
-- Fix:
--   1. assign_partition deja group_work_start = group_work_end = NULL.
--      El frontend usa fallback: seededShuffle(baseGroups.slice(CALIB_GROUPS), seed)
--      que siempre tiene los ~3800 grupos disponibles, deterministicamente
--      por dispositivo.
--   2. UPDATE one-shot: NULL group_work_start/end donde queda fuera de rango.
--      Como SQL no conoce baseGroups.length, usamos un threshold conservador:
--      si group_work_end > 4000 (>~3800 grupos disponibles), null.
-- ════════════════════════════════════════════════════════════════════════════

-- ─── 1) assign_partition: no asignar bloque de grupos posicional ─────────
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
  v_row           partitions%ROWTYPE;
  v_v1_max_idx    CONSTANT int := 31976;
  v_max_v1_end    int;
  v_max_v2_end    int;
  v_work_start    int;
  v_work_end      int;
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

  PERFORM pg_advisory_xact_lock(hashtext('assign_partition'));

  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id;
  IF FOUND THEN
    RETURN QUERY SELECT v_row.device_id, v_row.calib_seed,
                        v_row.work_start, v_row.work_end,
                        v_row.group_work_start, v_row.group_work_end,
                        v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
                        v_row.registered_at;
    RETURN;
  END IF;

  -- Tope v1
  SELECT COALESCE(MAX(p.work_end), p_calib_size)
    INTO v_max_v1_end
    FROM partitions p
   WHERE p.work_end <= v_v1_max_idx;
  IF v_max_v1_end < p_calib_size THEN
    v_max_v1_end := p_calib_size;
  END IF;

  -- Tope v2
  SELECT GREATEST(
           COALESCE((SELECT MAX(p.work_end_v2)
                       FROM partitions p
                      WHERE p.work_end_v2 IS NOT NULL), v_v1_max_idx),
           COALESCE((SELECT MAX(p.work_end)
                       FROM partitions p
                      WHERE p.work_start >= v_v1_max_idx), v_v1_max_idx),
           v_v1_max_idx
         )
    INTO v_max_v2_end;

  IF v_max_v1_end + p_block_size <= v_v1_max_idx THEN
    v_work_start := v_max_v1_end;
    v_work_end   := v_max_v1_end + p_block_size;
  ELSE
    v_work_start := v_max_v2_end;
    v_work_end   := v_max_v2_end + p_block_size;
  END IF;

  -- group_work_start/end quedan NULL: el frontend usa shuffle determinístico
  -- por dispositivo (cada usuario ve 500+ grupos diferentes pero del catálogo común).
  INSERT INTO partitions (
    device_id, calib_seed,
    work_start, work_end,
    group_work_start, group_work_end,
    calib_v, work_start_v2, work_end_v2
  ) VALUES (
    p_device_id, p_calib_seed,
    v_work_start, v_work_end,
    NULL, NULL,
    1, NULL, NULL
  ) RETURNING * INTO v_row;

  RETURN QUERY SELECT v_row.device_id, v_row.calib_seed,
                      v_row.work_start, v_row.work_end,
                      v_row.group_work_start, v_row.group_work_end,
                      v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
                      v_row.registered_at;
END;
$$;


-- ─── 2) Cleanup: NULL group_work_* para slices fuera de rango ────────────
-- El catálogo de grupos tiene ~3800 entradas únicas. Cualquier slice que
-- empiece > 3800 ya está fuera. Usamos 4000 como threshold conservador.
UPDATE partitions
   SET group_work_start = NULL,
       group_work_end   = NULL
 WHERE group_work_start > 4000
    OR group_work_end   > 4000;


-- ─── 3) Permisos ─────────────────────────────────────────────────────────
GRANT EXECUTE ON FUNCTION assign_partition(text, int, int, int, int, int) TO anon;


-- ─── 4) Verificación ─────────────────────────────────────────────────────
-- SELECT device_id, group_work_start, group_work_end,
--        (group_work_end - group_work_start) AS sz
--   FROM partitions
--   ORDER BY registered_at;
