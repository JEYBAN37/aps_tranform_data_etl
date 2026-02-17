def query_familia(database):
    return f"""SELECT 
        s.base_anterior AS db,
        f.id AS familia_id,
        f.sociambiental_id,
        f.apellidos,
        s.apellidosfamilia,
        f.numeropersonas,
        s.fecha,
        s.longitud,
        s.latitud,
        s.hacinamiento,
        s.aguaservicio,
        s.diposicionexcretas,
        s.basura,
        s.vivienda,
        s.direccion,
        o.familiograma,
        f.calculoapgar,
        f.apgarFuncionalidad,
        f.zaritFuncionalidad,
        f.calculozarit,
        f.lgtbi,
        f.poblacionvulnerable,
        f.cursovidafamilia,
        f.poblacionetnica,
        f.resguardo,
        o.ecomapa,
        o.resultadoecomapa,
        o.dirfamiliograma,
        o.plancuidado,
        o.dirplancuidado,
        o.date,
        s.id AS sociambiental_existente,
        u.microterritorio,
        u.cod_microterritorio,
        u.comuna,
        u.territorio,
        u.zona,
        r.nombres AS responsable_nombre,
        r.numero AS responsable_numero,
        r.ebs AS responsable_ebs,
        o.resultadofamiliograma,
        re.nombres AS responsable_plancuidado,
        re.numero AS resposable_plancuidado_id,
        -- 👉 Total de personas por familia (SUMA DE CURSOS DE VIDA)
        COALESCE(j.total_juventud, 0)
        + COALESCE(i.total_infantil, 0)
        + COALESCE(p.total_primera_infancia, 0)
        + COALESCE(a.total_adolescentes, 0)
        AS total_personas_cursos_vida,

        CASE
            WHEN f.sociambiental_id IS NULL THEN 'SIN_SOCIOAMBIENTAL_ID'
            WHEN s.id IS NULL THEN 'SOCIOAMBIENTAL_INVALIDO'
            ELSE 'SOCIOAMBIENTAL_OK'
        END AS estado,
        f.numerodocumento AS representante_doc_id,
        f.rol,
        f.celular,
        s.estrato,
        s.numerohabitantes,
        f.tipofamilia,
        f.cursovidafamilia,
        s.riesgoexterno,
        s.vacunamascotas,
        s.vector,
        s.riesgo,
        f.antecedenteenfermedad,
        f.riesgopsicosocial,
        f.estilodevidapredominante,
        f.cepilladodientes,
        f.higiene

    FROM {database}.familias f

    LEFT JOIN {database}.sociambientals s 
           ON f.sociambiental_id = s.id

    LEFT JOIN {database}.ubicaciones u 
           ON s.ubicacion_id = u.id

    LEFT JOIN {database}.responsables r 
           ON s.responsable_id = r.id

    LEFT JOIN {database}.observacions o 
           ON o.familia_id = f.id

    LEFT JOIN {database}.responsables re
    	   ON o.responsable_id = re.id

    -- 👉 Subconsulta: Juventud adultos por familia
    LEFT JOIN (
        SELECT familia_id, COUNT(*) AS total_juventud
        FROM {database}.juventudadultos
        GROUP BY familia_id
    ) j ON j.familia_id = f.id

    -- 👉 Subconsulta: Infantiles por familia
    LEFT JOIN (
        SELECT familia_id, COUNT(*) AS total_infantil
        FROM {database}.infantils
        GROUP BY familia_id
    ) i ON i.familia_id = f.id

    -- 👉 Subconsulta: Primera infancia por familia
    LEFT JOIN (
        SELECT familia_id, COUNT(*) AS total_primera_infancia
        FROM {database}.primerainfancias
        GROUP BY familia_id
    ) p ON p.familia_id = f.id

    -- 👉 Subconsulta: Adolescencias por familia
    LEFT JOIN (
        SELECT familia_id, COUNT(*) AS total_adolescentes
        FROM {database}.adolescencias
        GROUP BY familia_id
    ) a ON a.familia_id = f.id
    """