# delete
import jaydebeapi
import pandas as pd

from credenciales import MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD, DRIVER_PATH, DRIVER_MYSQL, MYSQL_APS
from mysql_conector import ejecutar_consulta_mysql


def main():
    # crea la conexion a la base de datos replica
    conexion_replica = jaydebeapi.connect(
        DRIVER_MYSQL,
        MYSQL_APS,
            [MYSQL_REPLICA_USER, MYSQL_REPLICA_PASSWORD],
        DRIVER_PATH
        )

    connection_mysql_replica = conexion_replica.cursor()

    QUERY_FILTRO_fAMILIAS = """
        SELECT 
        j.discapacidad ,
        j.condicioncronica ,
        j.condicioncronica1 ,
        j.esquemavacunacion ,
        j.valoracionmedica ,
        j.tomacitologia,
        j.saludoral ,
        j.gestacion ,
        j.controlprenatal ,
        j.canalizacionuno ,
        j.canalizaciondos,
        j.canalizaciontres,
        j.canalizacion_id ,
        j.educacion ,
        j.niveleducativo,
        j.estadocanalizacion ,
        j.observacioncanalizacion ,
        j.fechaRegistro ,
        j.registroCanalizacion ,
        j.remisionEspecifica ,
        j.id,
        j.familia_id ,
        j.tipodocumento ,
        j.numerodoc ,
        j.primerapellido ,
        j.segundoapellido ,
        j.primernombre ,
        j.segundonombre ,
        j.fechanac ,
        j.edad ,
        j.cursovida ,
        j.sexo ,
        j.genero ,
        j.aseguradora ,
        j.regimen ,
        j.estadoafiliacion ,
        j.telefono ,
        j.email ,
        s.fecha ,
        j.observacioncanalizacion ,
        s.direccion ,
        s.apellidosfamilia , 
        u.microterritorio ,
        u.cod_microterritorio ,
        u.comuna ,
        u.territorio ,
        u.zona,
        f.celular ,
        f.correo,
        c.id ,
        c.tipo ,
        c.nombre ,
        c.sede,
        c.telefonoInstitucional ,
        c.enlaceuno ,
        c.cargouno ,
        c.telefono1 ,
        c.correo1 
        FROM agsolutic_aps2024.juventudadultos j
        LEFT JOIN agsolutic_aps2024.familias f 
        ON  j.familia_id = f.id 
        LEFT JOIN agsolutic_aps2024.sociambientals s 
               ON s.id = f.sociambiental_id
        LEFT JOIN agsolutic_aps2024.ubicaciones u 
        ON s.ubicacion_id = u.id
        LEFT JOIN agsolutic_aps2024.canalizaciones c 
        ON c.id = j.canalizacion_id 
    """
    # traer la informacion requerida

    gestantes_query = ejecutar_consulta_mysql(QUERY_FILTRO_fAMILIAS,connection_mysql_replica,)


    df_id_Familias_con_juevetud_adultos = pd.DataFrame(gestantes_query).drop_duplicates()
    df_familias_id = pd.DataFrame(id_familias)

    df_familias_sin_juvetud = df_familias_id[~df_familias_id.isin(df_id_Familias_con_juevetud_adultos)]

    print(len(df_familias_sin_juvetud))
    print(df_familias_sin_juvetud)

    connection_mysql_replica.close()
    conexion_replica.close()

if __name__ == "__main__":
    main()
