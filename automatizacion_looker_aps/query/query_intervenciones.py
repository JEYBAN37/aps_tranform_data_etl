def query_intervenciones(database):
    return f"""SELECT 
        i.*,
        u.nombres AS responsable_nombre,
        u.numero AS responsable_numero,
        u.profesion AS responsable_profesion
    FROM {database}.interveciones i
    LEFT JOIN {database}.responsables u ON i.responsable_id = u.id
    """