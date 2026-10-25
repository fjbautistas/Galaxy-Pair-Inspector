-- ════════════════════════════════════════════════════════════════════════════
-- Migración: seguridad + campaña 'sochias' (24-oct-2026).
--
-- 1. Congela las campañas del paper: en clasificaciones y partitions solo se
--    escribe en versiones abiertas ('sochias'). v5_3 y v6 quedan de solo lectura,
--    también para la service_role (para reabrir: DISABLE TRIGGER, ver al final).
-- 2. Token por dispositivo: register_device(alias) entrega un token una sola vez;
--    todas las RPC de la app piden (p_device_id, p_token). En la base solo se
--    guarda el sha256 del token.
-- 3. anon/authenticated sin acceso directo a tablas; solo EXECUTE en las 5 RPC
--    nuevas. Las funciones viejas se conservan, pero anon ya no puede llamarlas.
-- 4. Límites: 60 votos nuevos/min por alias, 2000 filas por alias y versión,
--    500 alias nuevos cada 10 min en total; partición con tamaños fijos.
--
-- Correr UNA vez en el SQL Editor, después de exportar las tablas. No modifica
-- ni borra filas existentes. La app 'sochias' (GalPairs.html nuevo) necesita
-- esta migración; la app v6 vieja deja de sincronizar al aplicarla.
-- ════════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 0. Versiones abiertas a escritura ────────────────────────────────────────
CREATE OR REPLACE FUNCTION public._is_open_version(p_catalog_version text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT p_catalog_version IN ('sochias');
$$;

CREATE OR REPLACE FUNCTION public._block_frozen_versions()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') AND NOT public._is_open_version(OLD.catalog_version) THEN
    RAISE EXCEPTION 'catalog_version % is frozen', OLD.catalog_version;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NOT public._is_open_version(NEW.catalog_version) THEN
    RAISE EXCEPTION 'catalog_version % is frozen', NEW.catalog_version;
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS clasificaciones_freeze ON public.clasificaciones;
CREATE TRIGGER clasificaciones_freeze
  BEFORE INSERT OR UPDATE OR DELETE ON public.clasificaciones
  FOR EACH ROW EXECUTE FUNCTION public._block_frozen_versions();

DROP TRIGGER IF EXISTS partitions_freeze ON public.partitions;
CREATE TRIGGER partitions_freeze
  BEFORE INSERT OR UPDATE OR DELETE ON public.partitions
  FOR EACH ROW EXECUTE FUNCTION public._block_frozen_versions();

-- TRUNCATE no dispara triggers de fila: se bloquea aparte.
CREATE OR REPLACE FUNCTION public._block_truncate()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'TRUNCATE is disabled on %', TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS clasificaciones_no_truncate ON public.clasificaciones;
CREATE TRIGGER clasificaciones_no_truncate
  BEFORE TRUNCATE ON public.clasificaciones
  FOR EACH STATEMENT EXECUTE FUNCTION public._block_truncate();

DROP TRIGGER IF EXISTS partitions_no_truncate ON public.partitions;
CREATE TRIGGER partitions_no_truncate
  BEFORE TRUNCATE ON public.partitions
  FOR EACH STATEMENT EXECUTE FUNCTION public._block_truncate();

-- ── 1. Tokens por dispositivo ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.device_tokens (
  device_id  text PRIMARY KEY,
  token_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS device_tokens_created_idx
  ON public.device_tokens(created_at);

CREATE INDEX IF NOT EXISTS clasificaciones_device_created_idx
  ON public.clasificaciones(device_id, created_at);

CREATE OR REPLACE FUNCTION public._token_hash(p_token text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT encode(sha256(convert_to(p_token, 'UTF8')), 'hex');
$$;

CREATE OR REPLACE FUNCTION public._check_token(p_device_id text, p_token text)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF p_token IS NULL OR p_token !~ '^[0-9a-f]{32}$'
     OR NOT EXISTS (
       SELECT 1 FROM public.device_tokens
        WHERE device_id = p_device_id
          AND token_hash = public._token_hash(p_token)
     ) THEN
    RAISE EXCEPTION 'invalid_token';
  END IF;
END;
$$;

-- Alias nuevo → token (solo se devuelve aquí, una vez). Alias ya tomado → error
-- 'alias_taken'. Los alias de v5_3/v6 no tienen token: el primero que los pida
-- los obtiene, pero sus votos viejos siguen congelados e ilegibles para anon.
CREATE OR REPLACE FUNCTION public.register_device(p_device_id text)
RETURNS text
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_token text;
BEGIN
  IF NOT public._is_valid_device_id(p_device_id) THEN
    RAISE EXCEPTION 'Invalid device_id';
  END IF;
  IF (SELECT count(*) FROM public.device_tokens
       WHERE created_at > now() - interval '10 minutes') >= 500 THEN
    RAISE EXCEPTION 'rate_limited';
  END IF;

  v_token := replace(gen_random_uuid()::text, '-', '');
  INSERT INTO public.device_tokens(device_id, token_hash)
  VALUES (p_device_id, public._token_hash(v_token))
  ON CONFLICT (device_id) DO NOTHING;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'alias_taken';
  END IF;
  RETURN v_token;
END;
$$;

-- ── 2. RPC con token (sobrecargas de los nombres que ya usa la app) ──────────
CREATE OR REPLACE FUNCTION public.get_device_classifications(
  p_device_id       text,
  p_token           text,
  p_catalog_version text
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
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  PERFORM public._check_token(p_device_id, p_token);
  IF NOT public._is_open_version(p_catalog_version) THEN
    RAISE EXCEPTION 'catalog_version % is frozen', p_catalog_version;
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

CREATE OR REPLACE FUNCTION public._check_vote_limits(p_device_id text, p_catalog_version text, p_new_rows int)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  -- Ritmo: solo para el voto individual; el "Sync to cloud" manual reenvía en
  -- lote y queda acotado por el tope de 2000 filas.
  IF p_new_rows <= 1 AND (SELECT count(*) FROM public.clasificaciones
       WHERE device_id = p_device_id
         AND created_at > now() - interval '1 minute') >= 60 THEN
    RAISE EXCEPTION 'rate_limited';
  END IF;
  IF (SELECT count(*) FROM public.clasificaciones
       WHERE device_id = p_device_id
         AND catalog_version = p_catalog_version) + p_new_rows > 2000 THEN
    RAISE EXCEPTION 'too_many_rows';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.upsert_classification(
  p_device_id        text,
  p_token            text,
  p_item_type        text,
  p_item_uid         text,
  p_classification   text,
  p_catalog_version  text,
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
  PERFORM public._check_token(p_device_id, p_token);
  IF NOT public._is_open_version(p_catalog_version) THEN
    RAISE EXCEPTION 'catalog_version % is frozen', p_catalog_version;
  END IF;
  PERFORM public._check_vote_limits(p_device_id, p_catalog_version, 1);

  -- Validaciones y escritura: la función original (sin token).
  PERFORM public.upsert_classification(
    p_device_id, p_item_type, p_item_uid, p_classification,
    p_pair_uid, p_stable_system_id, p_id_par_v5,
    p_catalog_version, p_exported_at, left(COALESCE(p_source, 'app'), 32)
  );
END;
$$;

CREATE OR REPLACE FUNCTION public.upsert_classifications(
  p_device_id text,
  p_token     text,
  p_rows      jsonb
) RETURNS int
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  PERFORM public._check_token(p_device_id, p_token);
  IF jsonb_typeof(p_rows) <> 'array' THEN
    RAISE EXCEPTION 'p_rows must be a JSON array';
  END IF;
  IF jsonb_array_length(p_rows) > 2000 THEN
    RAISE EXCEPTION 'Too many rows';
  END IF;
  IF EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_rows) r
     WHERE r->>'device_id' IS DISTINCT FROM p_device_id
        OR NOT public._is_open_version(r->>'catalog_version')
  ) THEN
    RAISE EXCEPTION 'rows must belong to this device and an open catalog_version';
  END IF;
  PERFORM public._check_vote_limits(
    p_device_id, p_rows->0->>'catalog_version',
    (SELECT count(*)::int FROM jsonb_array_elements(p_rows) r
      WHERE NOT EXISTS (
        SELECT 1 FROM public.clasificaciones c
         WHERE c.device_id = p_device_id
           AND c.item_type = r->>'item_type'
           AND c.item_uid  = r->>'item_uid'
           AND c.catalog_version = r->>'catalog_version')));

  RETURN public.upsert_classifications(p_rows);
END;
$$;

CREATE OR REPLACE FUNCTION public.delete_classification(
  p_device_id       text,
  p_token           text,
  p_item_type       text,
  p_item_uid        text,
  p_catalog_version text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  PERFORM public._check_token(p_device_id, p_token);
  IF NOT public._is_open_version(p_catalog_version) THEN
    RAISE EXCEPTION 'catalog_version % is frozen', p_catalog_version;
  END IF;
  PERFORM public.delete_classification(p_device_id, p_item_type, p_item_uid, p_catalog_version);
END;
$$;

-- Tamaños fijos en el servidor (los de GalPairs v6/sochias): el cliente ya no
-- puede pedir bloques enormes. El lock evita bloques solapados si muchos
-- usuarios entran a la vez.
CREATE OR REPLACE FUNCTION public.assign_partition_mixed(
  p_device_id       text,
  p_token           text,
  p_calib_seed      int,
  p_catalog_version text
) RETURNS public.partitions
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  PERFORM public._check_token(p_device_id, p_token);
  IF NOT public._is_open_version(p_catalog_version) THEN
    RAISE EXCEPTION 'catalog_version % is frozen', p_catalog_version;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtext('assign_partition:' || p_catalog_version));

  RETURN public.assign_partition_mixed(
    p_device_id, p_calib_seed,
    950,    -- p_block_size
    550,    -- p_calib_size
    190,    -- p_group_block_size
    80,     -- p_group_calib_size
    10992,  -- p_v1_max_idx (n_pairs_v1)
    18526,  -- p_v2_max_idx (total_pairs)
    p_catalog_version
  );
END;
$$;

-- ── 3. Permisos: nada directo para anon/authenticated ────────────────────────
DROP POLICY IF EXISTS classifications_public_read ON public.clasificaciones;
DROP POLICY IF EXISTS partitions_public_read ON public.partitions;

ALTER TABLE public.clasificaciones ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.partitions      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.device_tokens   ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON ALL TABLES    IN SCHEMA public FROM anon, authenticated;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon, authenticated;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, anon, authenticated;

-- Objetos futuros creados por postgres nacen cerrados.
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES    FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON SEQUENCES FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated;

GRANT USAGE ON SCHEMA public TO anon;
GRANT EXECUTE ON FUNCTION public.register_device(text) TO anon;
GRANT EXECUTE ON FUNCTION public.get_device_classifications(text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION public.upsert_classification(
  text, text, text, text, text, text, text, text, integer, timestamptz, text) TO anon;
GRANT EXECUTE ON FUNCTION public.upsert_classifications(text, text, jsonb) TO anon;
GRANT EXECUTE ON FUNCTION public.delete_classification(text, text, text, text, text) TO anon;
GRANT EXECUTE ON FUNCTION public.assign_partition_mixed(text, text, int, text) TO anon;

NOTIFY pgrst, 'reload schema';

COMMIT;

-- ── Verificación (correr aparte) ─────────────────────────────────────────────
-- Debe listar solo las 6 funciones de arriba:
-- SELECT p.proname, pg_get_function_identity_arguments(p.oid)
--   FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--  WHERE n.nspname = 'public' AND has_function_privilege('anon', p.oid, 'EXECUTE');
-- Debe dar 0 filas:
-- SELECT table_name, privilege_type FROM information_schema.role_table_grants
--  WHERE table_schema = 'public' AND grantee IN ('anon', 'authenticated');
-- Conteos sin cambios respecto al respaldo:
-- SELECT catalog_version, count(*) FROM public.clasificaciones GROUP BY 1;
--
-- ── Reabrir temporalmente (solo si hace falta corregir v5_3/v6) ──────────────
-- ALTER TABLE public.clasificaciones DISABLE TRIGGER clasificaciones_freeze;
--   ... corrección ...
-- ALTER TABLE public.clasificaciones ENABLE TRIGGER clasificaciones_freeze;
