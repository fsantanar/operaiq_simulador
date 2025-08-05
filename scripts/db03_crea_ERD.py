# Generador de ERD visual a partir de estructura de tablas y relaciones
from graphviz import Digraph
from pathlib import Path

# Inicializa el grafo
dot = Digraph(comment='ERD - ERP SAD Inversiones')
dot.attr(rankdir='LR', fontsize='10')

# Diccionario de tablas con columnas (nombre: [(columna, tipo, opcional)])
tablas = {
    "roles": [
    ("id", "INTEGER", "PK", ""),
    ("nombre", "TEXT", "UNIQUE", "NOT NULL"),
    ("descripcion", "TEXT", "", "NOT NULL"),
    ("hh_en_uf_fijo","FLOAT","","NOT NULL"),
    ("hh_en_uf_honorario","FLOAT","","NOT NULL")
]   ,
    "trabajadores": [
        ("id", "INTEGER", "PK", ""),
        ("rut", "TEXT", "", "NOT NULL"),
        ("nombre", "VARCHAR(100)", "", "NOT NULL"),
        ("id_rol", "INTEGER", "FK -> roles.id", "NOT NULL"),
        ("iniciacion", "DATE", "", "NOT NULL"),
        ("termino", "DATE", "", ""),
        ("modalidad_contrato","TEXT","","NOT NULL")
    ],
    "disponibilidades_trabajadores": [
        ("id","INTEGER","PK",""),
        ("id_trabajador","INTEGER","FK -> trabajadores.id","NOT NULL"),
        ("dia_semana","INTEGER","","NOT NULL"),
        ("feriado","BOOLEAN","","NOT NULL"),
        ("hora_inicio","FLOAT","","NOT NULL"),
        ("hora_fin","FLOAT","","NOT NULL"),
        ("horas_dia","FLOAT","","NOT NULL")
    ],
    "clientes": [
        ("id", "INTEGER", "PK", ""),
        ("nombre", "VARCHAR(100)", "", "NOT NULL"),
        ("rut", "VARCHAR(20)", "", "NOT NULL"),
        ("correo", "TEXT", "", ""),
        ("celular", "TEXT", "", ""),
        ("es_empresa", "BOOLEAN", "", "NOT NULL"),
        ("expectativa_pago", "FLOAT", "", ""),
        ("expectativa_tiempo", "FLOAT", "", ""),
    ],
    "contactos": [
        ("id", "INTEGER", "PK", ""),
        ("id_cliente", "INTEGER", "FK -> clientes.id", "NOT NULL"),
        ("nombre", "TEXT", "", "NOT NULL"),
        ("correo", "TEXT", "", ""),
        ("celular", "TEXT", "", ""),
        ("notas", "TEXT", "", "")
    ],
    "tipos_insumo": [
        ("id", "INTEGER", "PK", ""),
        ("nombre", "VARCHAR(100)", "UNIQUE", "NOT NULL"),
        ("descripcion", "TEXT", "", ""),
        ("unidad", "VARCHAR(20)", "", ""),
        ("categoria", "VARCHAR(50)", "", ""),
        ("reutilizable", "BOOLEAN", "", "NOT NULL"),
        ("retorno_en_n_trabajos","FLOAT","","NOT NULL"),
        ("seguimiento_automatizado", "BOOLEAN", "", "DEFAULT TRUE NOT NULL"),
        ("nivel_critico", "INTEGER", "", ""),
        ("dias_entrega_referencia" , "FLOAT", "", "NOT NULL"),
        ("entrega_dias_inhabiles", "BOOLEAN", "", "NOT NULL"),
        ("cobrable","BOOLEAN","","NOT NULL")

    ],
    "tipos_trabajo": [
        ("id", "INTEGER", "PK", ""),
        ("nombre", "VARCHAR(100)", "UNIQUE", "NOT NULL"),
        ("descripcion", "TEXT", "", ""),
        ("horas_trabajo_estimados", "FLOAT", "", "NOT NULL"),
        ("dias_totales_entrega_insumos", "FLOAT", "", ""),
        ("dias_habiles_entrega_insumos", "FLOAT", "", ""),
        ("precio_uf_estimado", "FLOAT", "", "NOT NULL"),
    ],
    "precios_insumos": [
        ("id", "INTEGER", "PK", ""),
        ("id_tipo_insumo", "INTEGER", "FK -> tipos_insumo.id", "NOT NULL"),
        ("precio_por_paquete", "INTEGER", "", "NOT NULL"),
        ("unidades_por_paquete", "INTEGER", "", "NOT NULL"),
        ("minimo_paquetes_por_compra", "INTEGER", "", ""),
        ("fecha_precio", "DATE", "", "NOT NULL"),
        ("fecha_vigencia", "DATE", "", ""),
        ("observaciones", "TEXT", "", ""),
        ("proveedor", "TEXT", "", ""),
        ("dias_entrega", "FLOAT", "", ""),
        ("entrega_dias_inhabiles", "BOOLEAN", "", "")
    ],
    "precios_trabajos": [
        ("id", "INTEGER", "PK", ""),
        ("id_tipo_trabajo", "INTEGER", "FK -> tipos_trabajo.id", "NOT NULL"),
        ("precio_por_trabajo", "INTEGER", "", "NOT NULL"),
        ("minimo_trabajos", "INTEGER", "", "NOT NULL"),
        ("maximo_trabajos", "INTEGER", "", ""),
        ("fecha_precio", "DATE", "", "NOT NULL"),
        ("fecha_vigencia", "DATE", "", ""),
        ("observaciones", "TEXT", "", "")
    ],
    "tipos_servicio": [
        ("id", "INTEGER", "PK", ""),
        ("nombre", "VARCHAR(100)", "UNIQUE", "NOT NULL"),
        ("descripcion", "TEXT", "", ""),
        ("horas_trabajo_estimados", "FLOAT", "", "NOT NULL"),
        ("dias_totales_entrega_insumos", "FLOAT", "", ""),
        ("dias_habiles_entrega_insumos", "FLOAT", "", ""),
        ("precio_uf_estimado","FLOAT","",""),
        ("lugar_atencion","TEXT","","NOT NULL"),
        ("tipo_maquinaria","TEXT","","NOT NULL"),
        ("tipo_cliente","TEXT","","NOT NULL"),
        ("periodicidad_tipica_meses","FLOAT","",""),
    ],

    "proyectos": [
        ("id", "INTEGER", "PK", ""),
        ("id_cliente", "INTEGER", "FK -> clientes.id", "NOT NULL"),
        ("nombre", "TEXT", "", "NOT NULL"),
        ("descripcion", "TEXT", "",""),
        ("duracion_meses_estimada", "FLOAT", "", ""),
        ("fecha_inicio", "DATE", "", ""),
        ("fecha_fin", "DATE", "", ""),
    ],
    "movimientos_financieros": [
        ("id", "INTEGER", "PK", ""),
        ("fechahora_movimiento", "DATETIME", "", "NOT NULL"),
        ("numero_mes_balance","INTEGER","",""),
        ("numero_año_balance","INTEGER","",""),
        ("categoria", "ENUM( \"ingreso\", \"egreso\") ", "", "NOT NULL"),
        ("tipo", "VARCHAR(20)", "", "NOT NULL"),
        ("monto", "INTEGER", "", "NOT NULL"),
        ("divisa", "TEXT", "", "NOT NULL"),
        ("descripcion", "TEXT", "", ""),
        ("incluye_iva", "BOOLEAN", "", ""),
        ("deducible", "BOOLEAN", "", ""),
        ("nombre_y_carpeta_archivo_boleta", "TEXT", "", "NOT NULL"),
        ("lugar_fisico_boleta", "TEXT", "", "NOT NULL"),
        ("id_movimiento_recurrente_si_aplica","INTEGER","","INDEX"),
    ],
    "movimientos_recurrentes": [
        ("id", "INTEGER", "PK", ""),
        ("nombre", "VARCHAR(100)", "", "NOT NULL"),
        ("descripcion", "TEXT", "", ""),
        ("categoria", "ENUM(\"ingreso\", \"egreso\")", "", "NOT NULL"),
        ("tipo", "VARCHAR(50)", "", "NOT NULL"),
        ("divisa", "VARCHAR(10)", "", "DEFAULT 'CLP'"),
        ("valor_periodo", "INTEGER", "", "NOT NULL"),
        ("unidad_periodo", "ENUM(\"dias\", \"semanas\", \"meses\", \"años\")","","NOT NULL"),
        ("info_extra_recurrencia", "TEXT", "", ""),
        ("modo_calculo_monto", "TEXT", "", "NOT NULL"),
        ("valor_fijo", "FLOAT", "", ""),
        ("valor_por_servicio", "FLOAT", "", ""),
        ("fecha_inicio", "DATE", "", "NOT NULL"),
        ("fecha_fin", "DATE", "", ""),
        ("ultima_actualizacion", "DATETIME", "", ""),      
    ],

    "insumos": [
        ("id", "INTEGER", "PK", ""),
        ("id_tipo_insumo", "INTEGER", "FK -> tipos_insumo.id", "NOT NULL"),
        ("cantidad", "INTEGER", "", "NOT NULL"),
        ("descripcion", "TEXT", "", ""),
        ("fechahora_adquisicion_actualizacion", "DATETIME", "", "NOT NULL"),
        ("fecha_caducidad", "DATE", "", ""),
        ("id_movimiento_financiero_si_aplica", "INTEGER", "", "INDEX")
    ],
    "consumos": [
        ("id", "INTEGER", "PK", ""),
        ("id_tipo_insumo", "INTEGER", "FK -> tipos_insumo.id", "NOT NULL"),
        ("item_especifico","TEXT","",""),
        ("cantidad", "FLOAT", "", "NOT NULL"),
        ("porcentaje_de_uso", "FLOAT", "", "DEFAULT 100.0"),
        ("uso_ponderado", "FLOAT", "", ""),
        ("fechahora_inicio_uso", "DATETIME", "", ""),
        ("fechahora_fin_uso", "DATETIME", "", ""),
        ("validado","BOOLEAN","","DEFAULT FALSE NOT NULL"),
        ("id_trabajo_si_aplica", "INTEGER", "", "INDEX"),
        ("descontado_en_insumos","BOOLEAN","","NOT NULL"),
        ("id_insumo_si_aplica", "INTEGER", "", "INDEX"),
    ],
    "servicios": [
        ("id", "INTEGER", "PK", ""),
        ("id_proyecto", "INTEGER", "FK -> proyectos.id", "NOT NULL"),
        ("ids_tipo_servicio", "TEXT", "", "INDEX NOT NULL"),
        ("unidad_tipo_servicio", "INTEGER", "", ""),
        ("estado", "ENUM(\"planificado\", \"confirmado\", \"en curso\", \"rechazado\", \"inviable\", \"finalizado\", \"cliente perdido\")", "", "NOT NULL"),
        ("fecha_actualizacion_estado", "DATE", "", ""),
        ("fecha_solicitud", "DATETIME", "", ""),
        ("fecha_esperada", "DATETIME", "", ""),
        ("fecha_propuesta", "DATETIME", "", ""),
        ("fecha_limite_planificacion", "DATE", "", "NOT NULL"),
        ("nombre_orden_trabajo", "TEXT", "", ""),
        ("fecha_inicio_trabajos", "DATE", "", ""),
        ("fecha_fin_trabajos", "DATE", "", ""),
        ("total_precio_ot","INTEGER","",""),
        ("id_movimiento_financiero_si_aplica","INTEGER","","INDEX"),
        ("demora_pago_dias","INTEGER","","NOT NULL")
    ],
    "cotizaciones": [
        ("id", "INTEGER", "PK", ""),
        ("id_servicio", "INTEGER", "FK -> servicios.id", "NOT NULL"),
        ("fecha_cotizacion", "DATE", "", "NOT NULL"),
        ("fecha_entrega", "DATE", "", ""),
        ("descripcion", "TEXT", "", ""),
        ("total_estimado", "INTEGER", "", "NOT NULL"),
        ("nombre_archivo", "TEXT", "", "NOT NULL"),
        ("estado", "TEXT", "", "NOT NULL"),
    ],
    "tipos_servicio_a_tipos_trabajo": [
        ("id", "INTEGER", "PK", ""),
        ("id_tipo_servicio", "INTEGER", "FK -> tipos_servicio.id", "NOT NULL"),
        ("id_tipo_trabajo", "INTEGER", "FK -> tipos_trabajo.id", "NOT NULL"),
    ],
    "trabajos": [
        ("id", "INTEGER", "PK", ""),
        ("nombre", "TEXT", "", "NOT NULL"),
        ("id_tipo_trabajo", "INTEGER", "FK -> tipos_trabajo.id", "NOT NULL"),
        ("id_servicio", "INTEGER", "FK -> servicios.id", "NOT NULL"),
        ("n_maquina", "INTEGER", "", "NOT NULL"),
        ("estacionamiento", "TEXT","", ""),
        ("orden_en_ot", "INTEGER", "", ""),
        ("descripcion", "TEXT", "", ""),
        ("horas_hombre_asignadas", "FLOAT", "", ""),
        ("fechahora_inicio", "DATETIME", "", ""),
        ("fechahora_fin", "DATETIME", "", ""),
    ],


    "asignaciones": [
        ("id", "INTEGER", "PK", ""),
        ("id_trabajo", "INTEGER", "FK -> trabajos.id", "NOT NULL"),
        ("id_trabajador", "INTEGER", "FK -> trabajadores.id", "NOT NULL"),
        ("fechahora_inicio_ventana", "DATETIME", "", ""),
        ("fechahora_fin_ventana", "DATETIME", "", ""),
        ("horas_hombre_asignadas", "FLOAT", "", "NOT NULL"),
        ("horas_trabajadas_total", "FLOAT", "", ""),
        ("horas_trabajadas_extra", "FLOAT", "", ""),
        ("porcentaje_de_trabajo", "FLOAT", "", ""),
        ("porcentaje_de_avance", "FLOAT", "", ""),
        ("observaciones", "TEXT", "", ""),
        ("anuladas", "BOOLEAN", "", "DEFAULT False NOT NULL"),
    ],

    "requerimientos_trabajadores": [
        ("id", "INTEGER", "PK", ""),
        ("id_trabajo_si_aplica", "INTEGER", "", "INDEX"),
        ("id_tipo_trabajo", "INTEGER", "FK -> tipos_trabajo.id", "NOT NULL"),
        ("id_rol", "INTEGER", "FK -> roles.id", "NOT NULL"),
        ("horas_hombre_requeridas", "FLOAT", "", "NOT NULL"),
    ],

    "requerimientos_materiales": [
        ("id", "INTEGER", "PK", ""),
        ("id_trabajo_si_aplica", "INTEGER", "", "INDEX"),
        ("id_tipo_trabajo", "INTEGER", "FK -> tipos_trabajo.id", "NOT NULL"),
        ("id_tipo_insumo", "INTEGER", "FK -> tipos_insumo.id", "NOT NULL"),
        ("cantidad_requerida", "FLOAT", "", "NOT NULL"),
        ("porcentaje_de_uso", "FLOAT", "", "DEFAULT 100.0 NOT NULL"),
        ("cantidad_ponderada", "FLOAT", "", "NOT NULL"),
        ("observaciones", "TEXT", "", "")
    ]

}

# Agrega nodos
for tabla, columnas in tablas.items():
    label = f"""<
    <TABLE BORDER="1" CELLBORDER="1" CELLSPACING="0">
    <TR>
        <TD BGCOLOR="lightblue" COLSPAN="2">
        <FONT POINT-SIZE="24"><B>{tabla}</B></FONT>
        </TD>
    </TR>
    {"".join(
        f"<TR><TD>{col[0]}</TD><TD>{col[1]}" +
        (" <B>[PK]</B>" if "PK" in col[2] else "") +
        (f", {col[3]}" if len(col) > 3 and col[3] else "") +
        "</TD></TR>" for col in columnas)}
    </TABLE>
    >"""
    dot.node(tabla, label=label, shape='plaintext')

# Agrega relaciones explícitas con flechas
relaciones = [
    ("trabajadores", "roles", "solid"),
    ("disponibilidades_trabajadores", "trabajadores", "solid"),
    ("contactos", "clientes", "solid"),
    ("precios_insumos", "tipos_insumo", "solid"),
    ("precios_trabajos", "tipos_trabajo", "solid"),
    ("proyectos","clientes","solid"),
    ("movimientos_financieros","movimientos_recurrentes","dashed"),
    ("insumos", "tipos_insumo", "solid"),
    ("insumos","movimientos_financieros","dashed"),
    ("consumos", "tipos_insumo", "solid"),
    ("consumos","trabajos","dashed"),
    ("consumos","insumos","dashed"),
    ("cotizaciones", "servicios", "solid"),
    ("servicios", "proyectos", "solid"),
    ("servicios", "tipos_servicio", "dashed"),
    ("servicios", "movimientos_financieros", "dashed"),
    ("tipos_servicio_a_tipos_trabajo", "tipos_trabajo", "solid"),
    ("tipos_servicio_a_tipos_trabajo", "tipos_servicio", "solid"),
    ("trabajos", "servicios", "solid"),
    ("trabajos", "tipos_trabajo", "solid"),
    ("asignaciones", "trabajos", "solid"),
    ("asignaciones", "trabajadores", "solid"),
    ("requerimientos_trabajadores", "trabajos", "dashed"),
    ("requerimientos_trabajadores", "tipos_trabajo", "solid"),
    ("requerimientos_trabajadores", "roles", "solid"),
    ("requerimientos_materiales", "trabajos", "dashed"),
    ("requerimientos_materiales", "tipos_trabajo", "solid"),
    ("requerimientos_materiales", "tipos_insumo", "solid"),
]


for origen, destino, estilo in relaciones:
    dot.edge(origen, destino,style=estilo)

base_dir = Path(__file__).resolve().parent.parent
output_path = base_dir / 'erd' / 'diagrama_entidad_relacion'
dot.render(output_path, format='png', cleanup=True)
