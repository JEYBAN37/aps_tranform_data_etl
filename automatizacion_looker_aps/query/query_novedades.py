def query_novedades(database):
    return f"""SELECT 
                '{database}' AS db,
                v.id AS visita_negada_id,
                u.microterritorio,
                u.cod_microterritorio,
                u.comuna,
                u.territorio,
                r.nombres AS responsable_nombre,
                r.numero AS responsable_numero,
                r.ebs AS responsable_ebs,
                v.longitud,
                v.latitud,
                v.estadocasa,
                v.fecha ,
                v.observacion
            FROM {database}.visitasnegadas v 

            LEFT JOIN agsolutic_aps2024.ubicaciones u 
                   ON v.ubicacion_id = u.id

            LEFT JOIN {database}.responsables r 
                   ON v.responsable_id = r.id
            """