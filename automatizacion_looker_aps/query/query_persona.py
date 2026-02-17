def query_persona(database):
    return f"""SELECT 
                '{database}' AS db,
                t.* ,
                s.id as sociambiental_id,
                s.fecha,
                s.barriovereda,
                s.direccion,
                f.celular,
                s.vivienda,
                s.apellidosfamilia,
                u.microterritorio,
                u.cod_microterritorio,
                u.comuna,
                u.territorio,
                u.zona,
                r.nombres,
                r.numero,
                r.ebs,
                CASE 
                    WHEN t.familia_id IS NULL THEN 'SIN_FAMILIA_ID'
                    WHEN f.id IS NULL THEN 'ID_FAMILIA_INVALIDO'
                    WHEN COUNT(*) OVER (PARTITION BY t.doc_id) = 1 THEN 'UNICO_Y_VALIDO'
                    ELSE 'DUPLICADO'
                END AS estado
            FROM (
    SELECT 
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        valoracionmedica AS valoracion,
        saludoral AS higiene_oral,
        aseguradora,
        regimen,
        metodosanticonceptivos,
        infeccionestransmisionsexual,
        controlprenatal AS controlP,
        consumospa,
        tomacitologia,
        mamografia,
        discapacidad,
        fechanac,
        sexo,
        'NO APLICA' AS desnutricion,
        iniciovidasexual,
        riesgoembarazo,
        canalizacionuno,
        sopechamaltrato,
        'NO APLICA' AS desarrolloinfantil,
        'Adulto' AS cursodevida,
        c.nombre,
        estadocanalizacion
    FROM {database}.juventudadultos
    LEFT JOIN {database}.canalizaciones c ON c.id = juventudadultos.canalizacion_id

    UNION ALL

    SELECT 
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        'NO APLICA' AS gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        crecimientoydesarrollo AS valoracion,
        higieneoral AS higiene_oral,
        aseguradora,
        regimen,
        'NO APLICA' AS metodosanticonceptivos,
        'NO APLICA' AS infeccionestransmisionsexual,
        'NO APLICA' AS controlP,
        'NO APLICA' AS consumospa,
        'NO APLICA' AS tomacitologia,
        'NO APLICA' AS mamografia,
        discapacidad,
        fechanac,
        sexo,
        desnutricion,
        'NO APLICA' AS iniciovidasexual,
        'NO APLICA' AS riesgoembarazo,
        canalizacionuno,
        'NO APLICA' AS sopechamaltrato,
        desarrolloinfantil,
        'Infante' AS cursodevida,
         c.nombre,
         estadocanalizacion
    FROM {database}.infantils
    LEFT JOIN {database}.canalizaciones c ON c.id = infantils.canalizacion_id

    UNION ALL

    SELECT 
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        'NO APLICA' AS gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        crecimientoydesarrollo AS valoracion,
        higieneoral AS higiene_oral,
        aseguradora,
        regimen,
        'NO APLICA' AS metodosanticonceptivos,
        'NO APLICA' AS infeccionestransmisionsexual,
        'NO APLICA' AS controlP,
        'NO APLICA' AS consumospa,
        'NO APLICA' AS tomacitologia,
        'NO APLICA' AS mamografia,
        discapacidad,
        fechanac,
        sexo,
        desnutricion,
        'NO APLICA' AS iniciovidasexual,
        'NO APLICA' AS riesgoembarazo,
        canalizacionuno,
        'NO APILCA' AS sopechamaltrato,
        desarrolloinfantil,
        'PrimeraInfancia' AS cursodevida,
        c.nombre,
        estadocanalizacion
    FROM {database}.primerainfancias
    LEFT JOIN {database}.canalizaciones c ON c.id = primerainfancias.canalizacion_id

    UNION ALL

    SELECT 
        numerodoc AS doc_id,
        familia_id,
        tipodocumento,
        primerapellido,
        segundoapellido,
        primernombre,
        segundonombre,
        gestacion,
        condicioncronica,
        esquemavacunacion,
        desparasitacion,
        valoracionmedica AS valoracion,
        saludoral AS higiene_oral,
        aseguradora,
        regimen,
        metodosanticonceptivos,
        infeccionestransmisionsexual,
        controlprenatal AS controlP,
        consumospa,
        'NO APLICA' AS tomacitologia,
        'NO APLICA' AS mamografia,
        discapacidad,
        fechanac,
        sexo,
        'NO APLICA' AS desnutricion,
        iniciovidasexual,
        riesgoembarazo,
        canalizacionuno,
        sopechamaltrato,
        'NO APLICA' AS desarrolloinfantil,
        'Adolescencia' AS cursodevida,
        c.nombre,
        estadocanalizacion
    FROM {database}.adolescencias
    LEFT JOIN {database}.canalizaciones c ON c.id = adolescencias.canalizacion_id
) t
            LEFT JOIN {database}.familias f ON f.id = t.familia_id
            LEFT JOIN {database}.sociambientals s ON f.sociambiental_id = s.id 
            LEFT JOIN {database}.ubicaciones u ON s.ubicacion_id = u.id
            LEFT JOIN {database}.responsables r ON s.responsable_id  = r.id	
            """