-- ════════════════════════════════════════════════════════════════════════════
-- Migration 03: usuarios nuevos clasifican rp ∈ [0, 50] (mix v1 + v2)
-- ════════════════════════════════════════════════════════════════════════════
-- Aplica esto en Supabase SQL Editor. Idempotente.
--
-- Diseño:
--   • assign_partition: prioriza zona v1 [calib_size, 31976) si hay espacio.
--     Así nuevos usuarios entran como CASO A en claim_v2_slice (mix v1+v2).
--   • claim_v2_slice CASE B (usuario sin v1 utilizable, ej. v1 ya lleno al
--     registrar): cuando antes asignaba 3000 puros en v2, ahora asigna mix
--     v1=p_target_total/2 + v2=p_target_total/2 si hay espacio en v1; si no,
--     fallback al comportamiento previo (todo en v2).
--   • UPDATE one-shot para EZ0TD: reemplaza su slice puro en v2 por
--     1500 v1 + 1500 v2. Sus clasificaciones previas (~119 en v2) se
--     conservan en `clasificaciones` pero salen del catálogo del usuario;
--     el efecto neto es un "fresh start" con cobertura de todo el rango rp.
-- ════════════════════════════════════════════════════════════════════════════

-- ─── 1) assign_partition: priorizar zona v1 ──────────────────────────────
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
  v_max_grp_end   int;
  v_work_start    int;
  v_work_end      int;
BEGIN
  -- Idempotencia
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

  -- Tope v1: máx work_end entre slices que viven enteros en zona v1
  SELECT COALESCE(MAX(p.work_end), p_calib_size)
    INTO v_max_v1_end
    FROM partitions p
   WHERE p.work_end <= v_v1_max_idx;
  IF v_max_v1_end < p_calib_size THEN
    v_max_v1_end := p_calib_size;
  END IF;

  -- Tope v2: máx de work_end_v2 (sanos) o de work_end de slices ubicados en v2 (rotos)
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

  SELECT COALESCE(MAX(p.group_work_end), p_group_calib_size)
    INTO v_max_grp_end
    FROM partitions p;

  -- ¿Cabe el bloque completo en v1?
  IF v_max_v1_end + p_block_size <= v_v1_max_idx THEN
    v_work_start := v_max_v1_end;
    v_work_end   := v_max_v1_end + p_block_size;
  ELSE
    -- v1 lleno → asignar en v2 (claim_v2_slice CASE B se ocupará después)
    v_work_start := v_max_v2_end;
    v_work_end   := v_max_v2_end + p_block_size;
  END IF;

  INSERT INTO partitions (
    device_id, calib_seed,
    work_start, work_end,
    group_work_start, group_work_end,
    calib_v, work_start_v2, work_end_v2
  ) VALUES (
    p_device_id, p_calib_seed,
    v_work_start, v_work_end,
    v_max_grp_end, v_max_grp_end + p_group_block_size,
    1, NULL, NULL
  ) RETURNING * INTO v_row;

  RETURN QUERY SELECT v_row.device_id, v_row.calib_seed,
                      v_row.work_start, v_row.work_end,
                      v_row.group_work_start, v_row.group_work_end,
                      v_row.calib_v, v_row.work_start_v2, v_row.work_end_v2,
                      v_row.registered_at;
END;
$$;


-- ─── 2) claim_v2_slice: CASE B asigna mix v1+v2 cuando hay espacio v1 ────
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
  v_unclass     int;
  v_v2_size     int;
  v_v2_start    int;
  v_v2_end      int;
  v_zone_top    int;
  v_v1_top      int;
  v_half        int;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('claim_v2_slice'));

  SELECT * INTO v_row FROM partitions WHERE partitions.device_id = p_device_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'claim_v2_slice: device % no tiene partition', p_device_id;
  END IF;

  -- Idempotencia
  IF v_row.calib_v = 2 THEN
    RETURN QUERY SELECT v_row.device_id, v_row.calib_v,
                        v_row.work_start, v_row.work_end,
                        v_row.work_start_v2, v_row.work_end_v2;
    RETURN;
  END IF;

  v_v1_len := v_row.work_end - v_row.work_start;
  v_v1_in_range := (v_row.work_end <= p_v1_max_idx) AND (v_v1_len > 0);

  -- Tope ocupado en zona v2
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
    -- ─── CASO A: usuario sano, extender v1 con v2 hasta sumar p_target_total
    v_unclass  := GREATEST(0, v_v1_len - GREATEST(0, p_n_classif_v1));
    v_v2_size  := GREATEST(0, p_target_total - v_unclass);
    v_v2_start := v_zone_top;
    v_v2_end   := LEAST(v_zone_top + v_v2_size, p_v2_max_idx);

    UPDATE partitions
       SET calib_v       = 2,
           work_start_v2 = v_v2_start,
           work_end_v2   = v_v2_end
     WHERE partitions.device_id = p_device_id
     RETURNING * INTO v_row;
  ELSE
    -- ─── CASO B: usuario sin v1 utilizable ────────────────────────────────
    -- Asignar slice mixto v1 (primera mitad) + v2 (segunda mitad) si hay
    -- espacio en zona v1. De lo contrario, fallback a 100% v2 (legacy).
    v_half := p_target_total / 2;

    SELECT COALESCE(MAX(p.work_end), 0)
      INTO v_v1_top
      FROM partitions p
     WHERE p.work_end <= p_v1_max_idx;
    -- piso = calib_size implícito: el primer slice arranca en >= 120,
    -- pero como nadie ocupa [0, 120), v_v1_top podría ser 0 si nadie tiene v1.
    IF v_v1_top < 120 THEN
      v_v1_top := 120;
    END IF;

    IF v_v1_top + v_half <= p_v1_max_idx THEN
      -- Mix: v1 nuevo + v2 nuevo
      UPDATE partitions
         SET calib_v       = 2,
             work_start    = v_v1_top,
             work_end      = v_v1_top + v_half,
             work_start_v2 = v_zone_top,
             work_end_v2   = LEAST(v_zone_top + (p_target_total - v_half), p_v2_max_idx)
       WHERE partitions.device_id = p_device_id
       RETURNING * INTO v_row;
    ELSE
      -- v1 sin espacio → fallback histórico: todo en v2
      UPDATE partitions
         SET calib_v       = 2,
             work_start    = v_zone_top,
             work_end      = LEAST(v_zone_top + p_target_total, p_v2_max_idx),
             work_start_v2 = NULL,
             work_end_v2   = NULL
       WHERE partitions.device_id = p_device_id
       RETURNING * INTO v_row;
    END IF;
  END IF;

  RETURN QUERY SELECT v_row.device_id, v_row.calib_v,
                      v_row.work_start, v_row.work_end,
                      v_row.work_start_v2, v_row.work_end_v2;
END;
$$;


-- ─── 3) UPDATE one-shot: migrar EZ0TD a slice mixto ──────────────────────
-- (sus clasificaciones previas se conservan en `clasificaciones`; solo
-- cambia el catálogo visible. El user pasa de "3000 puros v2" a "1500 v1 + 1500 v2".)
DO $$
DECLARE
  v_v1_max_idx CONSTANT int := 31976;
  v_v1_top    int;
  v_v2_top    int;
  v_half      CONSTANT int := 1500;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM partitions WHERE device_id = 'EZ0TD') THEN
    RAISE NOTICE 'EZ0TD no existe en partitions; saltando migración one-shot';
    RETURN;
  END IF;

  -- Tope v1 disponible (excluyendo EZ0TD para no contar su slice viejo)
  SELECT COALESCE(MAX(work_end), 120)
    INTO v_v1_top
    FROM partitions
   WHERE work_end <= v_v1_max_idx
     AND device_id <> 'EZ0TD';
  IF v_v1_top < 120 THEN v_v1_top := 120; END IF;

  -- Tope v2 disponible (igual exclusión)
  SELECT GREATEST(
    COALESCE((SELECT MAX(work_end_v2) FROM partitions WHERE work_end_v2 IS NOT NULL AND device_id <> 'EZ0TD'), v_v1_max_idx),
    COALESCE((SELECT MAX(work_end)    FROM partitions WHERE work_start >= v_v1_max_idx AND device_id <> 'EZ0TD'), v_v1_max_idx),
    v_v1_max_idx
  ) INTO v_v2_top;

  UPDATE partitions
     SET calib_v       = 2,
         work_start    = v_v1_top,
         work_end      = v_v1_top + v_half,
         work_start_v2 = v_v2_top,
         work_end_v2   = v_v2_top + v_half
   WHERE device_id = 'EZ0TD';

  RAISE NOTICE 'EZ0TD migrado: work=[%, %) v2=[%, %)',
                v_v1_top, v_v1_top + v_half, v_v2_top, v_v2_top + v_half;
END $$;


-- ─── 4) Permisos ─────────────────────────────────────────────────────────
GRANT EXECUTE ON FUNCTION assign_partition(text, int, int, int, int, int) TO anon;
GRANT EXECUTE ON FUNCTION claim_v2_slice(text, int, int, int, int)        TO anon;


-- ─── 5) Verificación rápida ──────────────────────────────────────────────
-- SELECT device_id, calib_v, work_start, work_end, work_start_v2, work_end_v2,
--        group_work_start, group_work_end
--   FROM partitions
--   ORDER BY registered_at;
