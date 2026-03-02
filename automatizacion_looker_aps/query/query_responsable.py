def query_responsables(database):
    return f"""
    SELECT 
       r.id,
       r.nombres
    FROM {database}.responsables r
    """