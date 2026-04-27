-- ════════════════════════════════════════════════════════════════════════════
-- Migration 06: forzar usuarios activos a 1000 pares mixtos 500/500
-- ════════════════════════════════════════════════════════════════════════════
-- Corrige el rebalance anterior para que los usuarios activos no queden con
-- 1000 pares de un solo rango. Cada activo queda con:
--   • 500 pares primarios rp<20 kpc
--   • 500 pares explícitos rp>=20 kpc en work_start_v2/work_end_v2
--   • 100 grupos
--
-- Si el usuario ya estaba en v1, conserva sus primeros 500 pares históricos
-- y recibe un slice v2 nuevo. Si ya estaba en v2, conserva sus primeros 500
-- pares históricos como slice v2 y recibe un slice v1 nuevo.
-- ════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  v_v1_max_idx CONSTANT int := 31976;
  v_v2_max_idx CONSTANT int := 79156;
  v_calib_size CONSTANT int := 120;
  v_q          CONSTANT int := 500;
  r            record;
  v_start      int;
  v_end        int;

  -- Busca el primer hueco libre en [p_start, p_stop), ignorando temporalmente
  -- el dispositivo que se está actualizando.
  -- p_zone = 'v1' revisa work_start/work_end.
  -- p_zone = 'v2' revisa work_start_v2/work_end_v2 y bloques primarios v2.
  -- Implementado inline con bucles para no dejar funciones auxiliares.
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS _active_rebalance_snapshot (
    device_id text primary key,
    ord       int not null,
    old_start int not null,
    old_end   int not null
  ) ON COMMIT DROP;

  TRUNCATE _active_rebalance_snapshot;

  INSERT INTO _active_rebalance_snapshot(device_id, ord, old_start, old_end)
  SELECT p.device_id, a.ord, p.work_start, p.work_end
    FROM partitions p
    JOIN (
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
    ) AS a(device_id, ord) ON a.device_id = p.device_id;

  -- 1) Usuarios cuyo bloque histórico estaba en v2: conservarlo como slice v2
  -- y limpiar el primario para que pueda recibir v1.
  FOR r IN
    SELECT * FROM _active_rebalance_snapshot
     WHERE old_start >= v_v1_max_idx
     ORDER BY ord
  LOOP
    UPDATE partitions
       SET work_start_v2 = r.old_start,
           work_end_v2   = LEAST(r.old_start + v_q, r.old_end),
           work_start    = v_calib_size,
           work_end      = v_calib_size
     WHERE device_id = r.device_id;
  END LOOP;

  -- 2) Usuarios cuyo bloque histórico estaba en v1: conservar primeros 500 v1
  -- y limpiar v2 para asignarlo sin solapamiento.
  FOR r IN
    SELECT * FROM _active_rebalance_snapshot
     WHERE old_start < v_v1_max_idx
     ORDER BY ord
  LOOP
    UPDATE partitions
       SET work_start    = r.old_start,
           work_end      = LEAST(r.old_start + v_q, r.old_end),
           work_start_v2 = NULL,
           work_end_v2   = NULL
     WHERE device_id = r.device_id;
  END LOOP;

  -- 3) Asignar huecos v1 a quienes venían de v2.
  FOR r IN
    SELECT * FROM _active_rebalance_snapshot
     WHERE old_start >= v_v1_max_idx
     ORDER BY ord
  LOOP
    v_start := v_calib_size;
    LOOP
      v_end := v_start + v_q;
      IF v_end > v_v1_max_idx THEN
        RAISE EXCEPTION 'No quedan huecos v1 para %', r.device_id;
      END IF;

      EXIT WHEN NOT EXISTS (
        SELECT 1
          FROM partitions p
         WHERE p.device_id <> r.device_id
           AND p.work_start < v_v1_max_idx
           AND int4range(v_start, v_end, '[)')
               && int4range(p.work_start, LEAST(p.work_end, v_v1_max_idx), '[)')
      );

      v_start := v_start + v_q;
    END LOOP;

    UPDATE partitions
       SET work_start = v_start,
           work_end   = v_end
     WHERE device_id = r.device_id;
  END LOOP;

  -- 4) Asignar huecos v2 a quienes venían de v1.
  FOR r IN
    SELECT * FROM _active_rebalance_snapshot
     WHERE old_start < v_v1_max_idx
     ORDER BY ord
  LOOP
    v_start := v_v1_max_idx;
    LOOP
      v_end := v_start + v_q;
      IF v_end > v_v2_max_idx THEN
        RAISE EXCEPTION 'No quedan huecos v2 para %', r.device_id;
      END IF;

      EXIT WHEN NOT EXISTS (
        SELECT 1
          FROM partitions p
         WHERE p.device_id <> r.device_id
           AND (
             (
               p.work_start_v2 IS NOT NULL
               AND p.work_end_v2 IS NOT NULL
               AND int4range(v_start, v_end, '[)')
                   && int4range(p.work_start_v2, p.work_end_v2, '[)')
             )
             OR (
               p.work_start >= v_v1_max_idx
               AND int4range(v_start, v_end, '[)')
                   && int4range(p.work_start, p.work_end, '[)')
             )
           )
      );

      v_start := v_start + v_q;
    END LOOP;

    UPDATE partitions
       SET work_start_v2 = v_start,
           work_end_v2   = v_end
     WHERE device_id = r.device_id;
  END LOOP;

  -- 5) Mantener columnas informativas si existen.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_v1'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_v1 = work_end - work_start';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_v2'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_v2 = work_end_v2 - work_start_v2';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'partitions' AND column_name = 'n_groups'
  ) THEN
    EXECUTE 'UPDATE partitions SET n_groups = group_work_end - group_work_start';
  END IF;
END;
$$;

-- Verificación:
-- SELECT device_id,
--        work_start, work_end, work_start_v2, work_end_v2,
--        greatest(least(work_end, 31976) - least(work_start, 31976), 0) as primary_v1,
--        case
--          when work_end <= 31976 then 0
--          when work_start >= 31976 then work_end - work_start
--          else work_end - 31976
--        end as primary_v2,
--        coalesce(work_end_v2 - work_start_v2, 0) as explicit_v2,
--        group_work_start, group_work_end,
--        group_work_end - group_work_start as n_groups
--   FROM partitions
--   ORDER BY device_id;
