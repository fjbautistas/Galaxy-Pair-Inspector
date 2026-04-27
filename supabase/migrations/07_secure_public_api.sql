-- ════════════════════════════════════════════════════════════════════════════
-- Migration 07: API publica mas estrecha para la PWA
-- ════════════════════════════════════════════════════════════════════════════
-- La app publica ya no necesita permisos directos amplios sobre tablas.
-- En su lugar usa RPCs SECURITY DEFINER con validacion de payload.
--
-- Orden recomendado:
--   1. Publicar el HTML que usa estas RPCs.
--   2. Ejecutar esta migracion en Supabase.
-- ════════════════════════════════════════════════════════════════════════════

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


CREATE OR REPLACE FUNCTION get_device_classifications(p_device_id text)
RETURNS TABLE (
  id_par         int,
  classification text
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
  SELECT c.id_par, c.classification
    FROM clasificaciones c
   WHERE c.device_id = p_device_id
   ORDER BY c.id_par;
END;
$$;


CREATE OR REPLACE FUNCTION upsert_classification(
  p_device_id      text,
  p_id_par         int,
  p_classification text,
  p_exported_at    timestamptz DEFAULT now()
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT _is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;
  IF NOT _is_valid_classification(p_classification) THEN
    RAISE EXCEPTION 'Invalid classification';
  END IF;
  IF p_id_par IS NULL OR p_id_par < 1 OR p_id_par > 20000000 THEN
    RAISE EXCEPTION 'Invalid id_par';
  END IF;

  INSERT INTO clasificaciones(device_id, id_par, classification, exported_at)
  VALUES (p_device_id, p_id_par, p_classification, COALESCE(p_exported_at, now()))
  ON CONFLICT (device_id, id_par)
  DO UPDATE SET
    classification = EXCLUDED.classification,
    exported_at    = EXCLUDED.exported_at;
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
  IF jsonb_array_length(p_rows) > 2000 THEN
    RAISE EXCEPTION 'Too many rows';
  END IF;

  CREATE TEMP TABLE _rows_to_upsert (
    device_id      text,
    id_par         int,
    classification text,
    exported_at    timestamptz
  ) ON COMMIT DROP;

  INSERT INTO _rows_to_upsert(device_id, id_par, classification, exported_at)
  SELECT device_id, id_par, classification, exported_at
    FROM jsonb_to_recordset(p_rows)
         AS x(device_id text, id_par int, classification text, exported_at timestamptz);

  IF EXISTS (
    SELECT 1 FROM _rows_to_upsert
     WHERE NOT _is_valid_device_id(device_id)
        OR NOT _is_valid_classification(classification)
        OR id_par IS NULL OR id_par < 1 OR id_par > 20000000
  ) THEN
    RAISE EXCEPTION 'Invalid row in p_rows';
  END IF;

  INSERT INTO clasificaciones(device_id, id_par, classification, exported_at)
  SELECT device_id, id_par, classification, COALESCE(exported_at, now())
    FROM _rows_to_upsert
  ON CONFLICT (device_id, id_par)
  DO UPDATE SET
    classification = EXCLUDED.classification,
    exported_at    = EXCLUDED.exported_at;

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;


CREATE OR REPLACE FUNCTION delete_classification(
  p_device_id text,
  p_id_par    int
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT _is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;
  IF p_id_par IS NULL OR p_id_par < 1 OR p_id_par > 20000000 THEN
    RAISE EXCEPTION 'Invalid id_par';
  END IF;

  DELETE FROM clasificaciones
   WHERE device_id = p_device_id
     AND id_par = p_id_par;
END;
$$;


-- Permisos RPC para anon.
GRANT EXECUTE ON FUNCTION get_device_classifications(text) TO anon;
GRANT EXECUTE ON FUNCTION upsert_classification(text, int, text, timestamptz) TO anon;
GRANT EXECUTE ON FUNCTION upsert_classifications(jsonb) TO anon;
GRANT EXECUTE ON FUNCTION delete_classification(text, int) TO anon;


-- Retirar permisos directos amplios sobre tablas publicas.
REVOKE SELECT, INSERT, UPDATE, DELETE ON clasificaciones FROM anon;
REVOKE USAGE, SELECT ON SEQUENCE clasificaciones_id_seq FROM anon;
REVOKE SELECT, INSERT, UPDATE, DELETE ON partitions FROM anon;

-- Eliminar politicas permisivas antiguas. Las RPC SECURITY DEFINER siguen
-- pudiendo operar; assign_partition_mixed/claim_v2_slice ya tienen GRANT EXECUTE.
DROP POLICY IF EXISTS public_write ON clasificaciones;
DROP POLICY IF EXISTS public_read ON partitions;
DROP POLICY IF EXISTS auto_register ON partitions;


-- Verificacion rapida:
-- SELECT has_table_privilege('anon', 'clasificaciones', 'select') AS anon_can_select_cl;
-- SELECT has_function_privilege('anon', 'get_device_classifications(text)', 'execute') AS anon_can_rpc_read;
