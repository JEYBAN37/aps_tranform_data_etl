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
        j.id,
        j.numerodoc AS doc_id,
        j.familia_id,
        j.tipodocumento,
        j.primerapellido,
        j.segundoapellido,
        j.primernombre,
        j.segundonombre,
        j.gestacion,
        j.condicioncronica,
        j.esquemavacunacion,
        j.desparasitacion,
        j.valoracionmedica AS valoracion,
        j.saludoral AS higiene_oral,
        j.aseguradora,
        j.regimen,
        j.metodosanticonceptivos,
        j.infeccionestransmisionsexual,
        j.controlprenatal AS controlP,
        j.consumospa,
        j.tomacitologia,
        j.mamografia,
        j.discapacidad,
        j.fechanac,
        j.sexo,
        j.iniciovidasexual,
        j.riesgoembarazo,
        j.canalizacionuno,
        j.sopechamaltrato,
        j.desnutricion,
        j.grupopoblacional AS cursodevida,
        c.nombre,
        j.estadocanalizacion
    FROM {database}.juventudadultos j
    LEFT JOIN {database}.canalizaciones c ON c.id = j.canalizacion_id
) t
            LEFT JOIN {database}.familias f ON f.id = t.familia_id
            LEFT JOIN {database}.sociambientals s ON f.sociambiental_id = s.id 
            LEFT JOIN {database}.ubicaciones u ON s.ubicacion_id = u.id
            LEFT JOIN {database}.responsables r ON s.responsable_id  = r.id	
            """