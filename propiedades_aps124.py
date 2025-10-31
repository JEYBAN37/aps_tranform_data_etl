MICROTERRITORIO = '001'
TERRITORIO = 'T01'

PROPIEDADES_TIPO_1 = [
    # 1
    'NI',
    # 2
    '900091143',
    # 5,
    'EMPRESA SOCIAL DE ESTADO PASTO SALUD ESE',
    # 6
    '900091143',
    # 7
    'CC',
    # 8
    '79453251',
    # 9
    'DIEGO',
    # 10
    'MORALES',
    # 11
    'CC',
    # 12
    '93407260',
    # 13
    'DIEGO',
    # 14
    'MORALES',
    # 15
    'GERENCIA@PASTOSALUDESE.GOV.CO'
    ]


    # 0
TIPO_REGISTROS = ['1','2', '3']
    # 3
FECHA_INICIAL = '2025-10-27'

    # 4
FECHA_FINAL = '2025-10-27'

PROPIEDADES_TIPO_2 = [
        # 2
        '1',
        # 3
        '52',
        # 4
        'UZPE011',
        # 5
        '52001',
        # 20
        '900091143',
        #58
        '1'
    ]

PROPIEDADES_TIPO_3 = [
        # 2
        '1',
    ]

ENFERMEDADES_CRONICAS = [
    "Cardiovascular",
    "Diabetes",
    "Dislipidemia",
    "Endocrinológica",
    "Enfermedades reumáticas",
    "Epilepsia",
    "Gastrointestinal",
    "Hipertensión",
    "Hipertiroidismo",
    "Hipotiroidismo",
    "Metabólica",
    "Neurológica",
    "Renal, otras enfermedades crónicas",
    "Respiratoria",
]

NIVEL_ESTUDIO = [ '1' , '2' , '3' , '4' , '5' , '6' , '7' , '8' , '9' , '10' , '11' , '12' , '13']

AFILIACION = {
            'CONTRIBUTIVO': '2',
            'PARTICULAR': '2',
            'REGIMEN ESPECIAL': '3',
            'REGIMEN EXCEPCION': '4',
            'SUBSIDIADO': '1',
            'SD': '5'
        }

ETNIA = {
            '1':'01',
            '2':'02',
            '5':'05',
            '6':'06',
            '7':'07'# Indígena
        }

DISCAPACIDAD = {
                                                         'FISICA': '1',
                                                         'AUDITIVA': '2',
                                                            'VISUAL': '3',
                                                         'SORDOCEGUERA': '4',
                                                         'INTELECTUAL': '5',
                                                         'PSICOSOCIAL': '6',
                                                         'MULTIPLE': '7',
                                                         'SIN DISCAPACIDAD': '8',
                                                     }

ANIMALES_PERMITIDO = {
        "no aplica":13,
        "no":13,
        "perros": 1,
        "gato": 2,
        "porcinos": 3,
        "bóvidos": 4,
        "equidos": 5,
        "ovinos": 6,
        "cuyes": 9,
        "aves de producción": 7,
        "aves ornamentales": 8,
        "peces ornamentales": 9,
        "hamster": 9,
        "cobayos": 10,
        "conejos": 10,
        "animales silvestres": 11,
        "otro": 12,
        "ninguno": 13
    }

TIPOS_VIVIENDA = {
    1: "Casa",
    2: "Casa Indígena",
    3: "Carpa",
    4: "Apartamento",
    5: "Pieza/ Cuarto en Inquilinato",
    6: "Contenedor",
    7: "Embarcación",
    8: "Vagón",
    9: "Refugio Natural",
    10: "Cueva",
    11: "Puente",
    12: "Otro"
}

PROPIEDADES_TIPO_1 = [
    # 1
    'NI',
    # 2
    '900091143',
    # 5,
    'EMPRESA SOCIAL DE ESTADO PASTO SALUD ESE',
    # 6
    '900091143',
    # 7
    'CC',
    # 8
    '79453251',
    # 9
    'DIEGO',
    # 10
    'MORALES',
    # 11
    'CC',
    # 12
    '93407260',
    # 13
    'DIEGO',
    # 14
    'MORALES',
    # 15
    'GERENCIA@PASTOSALUDESE.GOV.CO'
]
COLUMNAS_PERSONAS_JOVENADULTO = [
            'primer_nombre',
            'segundo_nombre',
            'primer_apellido',
            'segundo_apellido',
            'tipodoc',
            'numerodoc',
            'fechanac',
            'sexo',
            'gestacion',
            'rol',
            'educacion',
            'estudio',
            'regimen',
            'cursovida',
            'saludalternativa',
            'discapacidad',
            'peso',
            'talla',
            'indicemasacorporal',
            'condicioncronica',
            'canalizacionuno',
            'canalizaciondos',
            'canalizaciontres',
            'estadocanalizacion',
            'condicioncronica',
            'ocupacion',
            'etnia',
            'edad',
            'riesgopsicosocial',
            'sopechamaltrato',
            'familia_id'
        ]
def query_familias (territorio,microterritorio):
    return  F"""
        SELECT DISTINCT 
         f.id as id_familia_db,
         s.id as id_sociambiental_db,
         s.latitud,
         s.longitud,
         s.direccion,
         s.hacinamiento, 
         u.territorio, 
         u.cod_microterritorio,
         u.microterritorio,
         s.estrato,
         s.numerohogares,
         s.numerohabitantes,
         f.hogar,
         r.tipodoc,
         r.numero,
         r.profesion,
         s.fecha,
         s.vivienda,
         s.pared,
         s.piso,
         s.techo,
         s.dormitorios,
         s.riesgo,
         s.acceso,
         f.combustible,
         s.vector,
         s.riesgoexterno,
         s.actividad,
         s.mascotas,
         s.numeroPerros,
         s.numeroGatos,
         s.aguaservicio,
         s.diposicionexcretas,
         s.aguaresiduales,
         s.basura,
         f.tipofamilia,
         f.numeropersonas,
         o.resultadofamiliograma,
         f.calculoapgar,
         f.calculozarit,
         f.zaritfuncionalidad,
         o.resultadoecomapa,
         f.poblacionvulnerable,
         f.riesgopsicosocial,
         f.estilodevidapredominante,
         f.antecedenteenfermedad,
         f.antecedenteenfermedad1,
         f.antecedenteenfermedad2,
         f.saludalternativa,
         f.alimentos,
         f.programasocial,
         f.higiene
        FROM agsolutic_aps2024.familias f
        LEFT JOIN agsolutic_aps2024.sociambientals s
        ON f.sociambiental_id = s.id
        LEFT JOIN agsolutic_aps2024.ubicaciones u 
        ON s.ubicacion_id = u.id 
        LEFT JOIN agsolutic_aps2024.responsables r
        ON s.responsable_id = r.id
        LEFT JOIN agsolutic_aps2024.observacions o
        ON f.id = o.familia_id
        WHERE u.territorio = '{territorio}' AND u.cod_microterritorio = '{microterritorio}'
        ORDER BY s.id
        """

def traer_joven_adultos(id_list_sql):
    return f""" 
        SELECT 
         j.primernombre,
         j.segundonombre,
         j.primerapellido,
         j.segundoapellido,
         j.tipodocumento,
         j.numerodoc,
         j.fechanac,
         j.sexo,
         j.gestacion,
         j.rol,
         j.educacion,
         j.niveleducativo,
         j.regimen,
         j.cursovida,
         j.saludalternativa,
         j.discapacidad,
         j.peso,
         j.talla,
         j.indicemasacorporal,
         j.condicioncronica,
         j.canalizacionuno,
         j.canalizaciondos,
         j.canalizaciontres,
         j.estadocanalizacion,
         j.condicioncronica,
         j.ocupacion,
         j.etnia,
         j.edad,
         j.riesgopsicosocial,
         j.sopechamaltrato,
         j.familia_id
        FROM agsolutic_aps2024.familias f
        LEFT JOIN agsolutic_aps2024.juventudadultos j
        ON f.id = j.familia_id
        WHERE f.id IN ({id_list_sql})
        """