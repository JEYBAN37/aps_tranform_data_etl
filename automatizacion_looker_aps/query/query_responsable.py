def query_responsables(database):
    return f"""
    SELECT 
       r.id,
       r.nombres,
       r.numero,
       r.contrato
    FROM {database}.responsables r
    """