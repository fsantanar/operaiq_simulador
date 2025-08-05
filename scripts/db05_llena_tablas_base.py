import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.conexion import db


# Ruta donde están los CSV
CARPETA_CSV = '../contenido_tablas/'

# Lista de archivos csv y sus tablas destino
ARCHIVOS_TABLAS = [
    ('roles.csv', 'roles'),
    ('trabajadores.csv', 'trabajadores'),
    ('disponibilidades_trabajadores.csv','disponibilidades_trabajadores'),
    ('clientes.csv', 'clientes'),
    ('contactos.csv', 'contactos'),
    ('tipos_insumo.csv', 'tipos_insumo'),
    ('tipos_trabajo.csv', 'tipos_trabajo'),
    ('tipos_servicio.csv', 'tipos_servicio'),
    ('precios_insumos.csv', 'precios_insumos'),
    ('precios_trabajos.csv', 'precios_trabajos'),
    ('tipos_servicio_a_tipos_trabajo.csv', 'tipos_servicio_a_tipos_trabajo'),
    ('requerimientos_trabajadores.csv', 'requerimientos_trabajadores'),
    ('requerimientos_materiales.csv', 'requerimientos_materiales'),
    ('movimientos_recurrentes.csv','movimientos_recurrentes'),
]


for csv_filename, table in ARCHIVOS_TABLAS:
    csv_path = CARPETA_CSV+csv_filename
    with db.atomic():
        with db.connection().cursor() as cursor:
            with open(csv_path, 'r') as f:
                print(f'Llenando Tabla: {table}')
                # Skip header
                next(f)
                cursor.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, DELIMITER ';')", f)
    # <-- Cuando termina el `with db.atomic()`, el cambio queda automáticamente confirmado
