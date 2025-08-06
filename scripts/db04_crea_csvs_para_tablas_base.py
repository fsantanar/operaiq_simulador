import pandas as pd
import random
import numpy as np
from datetime import datetime
import yaml
import time
import os
import unidecode
import json
import datetime
import math
import sys
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils import lee_json, calcular_dv, decide_compra

ti = time.time()

# Ruta base del proyecto (dos niveles arriba del script)
base_dir = Path(__file__).resolve().parent.parent

# Rutas absolutas construidas desde la base
config_path = base_dir / 'config.yml'
carpeta_inputs = str(base_dir / 'inputs') + '/'
carpeta_contenido_tablas = str(base_dir / 'contenido_tablas') + '/'

# Cargar la configuración desde el archivo YAML
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)


guardar_no_cifrado = config['csvs']['guardar_copia_no_cifrada']
semilla = config['instancias']['semilla']
fecha_inicio_empresa = config['csvs']['fecha_inicio_empresa']
año_inicio_empresa,mes_inicio_empresa,dia_inicio_empresa=fecha_inicio_empresa.split('/')
año_inicio_empresa = int(año_inicio_empresa)
total_clientes = config['csvs']['clientes']['total_clientes']
fraccion_clientes_empresas = config['csvs']['clientes']['fraccion_clientes_empresas']
n_clientes_empresas = int(total_clientes * fraccion_clientes_empresas)
n_clientes_personas = total_clientes - n_clientes_empresas
primer_rut_cliente = config['csvs']['clientes']['primer_rut_cliente']
ultimo_rut_cliente = config['csvs']['clientes']['ultimo_rut_cliente']
expectativa_pago_general = config['csvs']['clientes']['expectativa_pago_general']
expectativa_tiempo_general = config['csvs']['clientes']['expectativa_tiempo_general']
expectativa_pago_media, expectativa_pago_std, expectativa_pago_limite = expectativa_pago_general
expectativa_tiempo_media, expectativa_tiempo_std, expectativa_tiempo_limite = expectativa_tiempo_general
fraccion_cotiz_recotiz_ot = config['csvs']['proyectos']['fraccion_cotiz_recotiz_ot']
fraccion_proyectos_cotizados = fraccion_cotiz_recotiz_ot[0]
fraccion_proyectos_recotizados = fraccion_cotiz_recotiz_ot[1]
fraccion_proyectos_con_ot = fraccion_cotiz_recotiz_ot[2]
uf_por_año = config['csvs']['precios']['uf_por_año']
tasa_overhead = config['csvs']['precios']['tasa_overhead']
margen_ganancia = config['csvs']['precios']['margen_ganancia']
precio_uf_dia_estacionamiento_liviana = config['csvs']['precios']['precio_uf_dia_estacionamiento_liviana']
precio_uf_dia_estacionamiento_pesada = config['csvs']['precios']['precio_uf_dia_estacionamiento_pesada']
fraccion_de_hh_en_sueldo = config['csvs']['trabajadores']['fraccion_de_hh_en_sueldo']

random.seed(semilla)


def gauss_con_limite_inferior(mu, sigma, n_elementos, limite_inferior):
    res = []
    for n in range(n_elementos):
        while True:
            x = random.gauss(mu, sigma)
            if x >= limite_inferior:
                res.append(x)
                break
    return res

def generar_correo_personal(nombre, apellido1, apellido2,dominio_forzado=None):
    # Limpiar tildes y espacios, pasar todo a minúscula
    nombre = unidecode.unidecode(nombre.strip().lower())
    apellido1 = unidecode.unidecode(apellido1.strip().lower())
    apellido2 = unidecode.unidecode(apellido2.strip().lower())

    # Posibles formatos de nombre
    formatos = [
        f"{nombre}.{apellido1}",
        f"{nombre}{apellido1}",
        f"{apellido1}.{nombre}",
        f"{nombre[0]}{apellido1}",
        f"{nombre}{apellido1[0]}",
        f"{nombre}.{apellido1}{apellido2[0]}",
        f"{nombre}_{apellido1}",
        f"{apellido1}{nombre[0]}",
        f"{nombre}{random.randint(10,99)}",
        f"{nombre}.{apellido1}{random.randint(1,9)}"
    ]

    if dominio_forzado is None:
        # Dominios comunes
        dominios = [
            "gmail.com", "hotmail.com", "outlook.com",
            "yahoo.com", "live.cl", "gmail.com", "gmail.com"
        ]
        dominio = random.choice(dominios)
    else:
        dominio = dominio_forzado

    correo = random.choice(formatos) + "@" + dominio
    return correo



info_nombres_clientes = lee_json(carpeta_inputs+'input01_nombres_clientes.json')
nombres_chile = info_nombres_clientes['nombres_chile']
apellidos_chile = info_nombres_clientes['apellidos_chile']
info_empresas_chile = info_nombres_clientes['nombres_empresas']


ids_clientes = list(range(1, total_clientes + 1))
random.shuffle(ids_clientes)
nombres_clientes_personas = random.choices(nombres_chile, k=n_clientes_personas)
primeros_apellidos_clientes_personas = random.choices(apellidos_chile, k=n_clientes_personas)
segundos_apellidos_clientes_personas = random.choices(apellidos_chile, k=n_clientes_personas)
ruts_personas = random.sample(range(primer_rut_cliente, ultimo_rut_cliente + 1), n_clientes_personas)
ruts_empresas = random.sample(range(70000000, 80000000 + 1), n_clientes_personas)
lista_empresas_chile = list(info_empresas_chile.keys())

expectativas_pago_clientes = gauss_con_limite_inferior(expectativa_pago_media, expectativa_pago_std,
                                                       total_clientes, expectativa_pago_limite)
expectativas_tiempo_clientes = gauss_con_limite_inferior(expectativa_tiempo_media, expectativa_tiempo_std,
                                                       total_clientes, expectativa_tiempo_limite)


################################
###                          ###
###  GENERAR LISTA CLIENTES  ###
###                          ###
################################
clientes = []

for n_cliente in range(1,total_clientes+1):

    celular_cliente = '+569'+str(random.randint(10000000,99999999))
    id_cliente = ids_clientes[n_cliente-1]
    expectativas_pago_cliente = expectativas_pago_clientes[n_cliente-1]
    expectativas_tiempo_cliente = expectativas_tiempo_clientes[n_cliente-1]

    # Aquí evaluamos si estamos recorriendo las personas o las empresas
    if n_cliente <= n_clientes_personas: # Si es una persona
        ind_cliente = n_cliente-1
        
        nombre_cliente = nombres_clientes_personas[ind_cliente]
        primer_apellido_cliente = primeros_apellidos_clientes_personas[ind_cliente]
        segundo_apellido_cliente = segundos_apellidos_clientes_personas[ind_cliente]
        nombre_completo_cliente = nombre_cliente + " " + primer_apellido_cliente + " " + segundo_apellido_cliente
        numero_rut_cliente = ruts_personas[ind_cliente]
        dv = calcular_dv(numero_rut_cliente)
        rut_cliente = f"{numero_rut_cliente}-{dv}"
        correo_cliente = generar_correo_personal(nombre_cliente, primer_apellido_cliente, segundo_apellido_cliente)
        es_empresa=False

    if n_cliente > n_clientes_personas: # Si es una empresa
        ind_cliente = n_cliente-n_clientes_personas-1
        nombre_completo_cliente = lista_empresas_chile[ind_cliente]
        numero_rut_cliente = ruts_empresas[ind_cliente]
        dv = calcular_dv(numero_rut_cliente)
        rut_cliente = f"{numero_rut_cliente}-{dv}"
        correo_cliente = info_empresas_chile[nombre_completo_cliente]
        es_empresa=True


    clientes.append({"id": id_cliente, "nombre": nombre_completo_cliente, "rut": rut_cliente,
                     "correo": correo_cliente, "celular": celular_cliente, "es_empresa":es_empresa,
                     "expectativas_pago": round(expectativas_pago_cliente,2),
                     "expectativas_tiempo": round(expectativas_tiempo_cliente,2)})

df_clientes = pd.DataFrame(clientes)
df_clientes.sort_values(by='id',inplace=True)
df_clientes.reset_index(drop=True,inplace=True)

df_clientes.to_csv(carpeta_contenido_tablas+'clientes.csv',index=False,sep=';')

    

#################################
###                           ###
###  GENERAR LISTA CONTACTOS  ###
###                           ###
#################################
df_empresas = df_clientes[df_clientes['es_empresa'] == True].copy()


contactos = []
nombres_usados = set()

for i, row in df_empresas.iterrows():
    intentos = 0
    while True:
        intentos += 1
        if intentos > 1000:
            raise Exception("Demasiados intentos para generar un nombre único.")

        nombre = random.choice(nombres_chile)
        apellido1 = random.choice(apellidos_chile)
        apellido2 = random.choice(apellidos_chile)
        nombre_completo = f"{nombre} {apellido1} {apellido2}"

        if nombre_completo not in nombres_usados:
            nombres_usados.add(nombre_completo)
            break

    dominio = row['correo'].split("@")[-1]
    correo_contacto = generar_correo_personal(nombre, apellido1, apellido2, dominio_forzado=dominio)
    celular = '+569' + str(random.randint(10000000, 99999999))

    contactos.append({
        "id": len(contactos) + 1,
        "id_cliente": row['id'],
        "nombre": nombre_completo,
        "correo": correo_contacto,
        "celular": celular,
        "notas": ""
    })

# Guardar como CSV
df_contactos = pd.DataFrame(contactos)
df_contactos.to_csv(carpeta_contenido_tablas+"contactos.csv", sep=";", index=False)

#######################################
###                                 ###
###        GENERA LISTAS            ###
###     ROLES, TRABAJADORES  Y      ###
###  DISPONIBILIDADES_TRABAJADORES  ###
#######################################



# Cargar el archivo JSON
data_roles = lee_json(carpeta_inputs+"input02_roles_y_trabajadores.json")


# Crear DataFrame de trabajadores y roles
roles = []
trabajadores = []
disponibilidades = []
n_trabajador = 0
n_disponibilidad = 0
for ind_rol,rol in enumerate(data_roles):
    roles.append({
        "id": ind_rol+1,
        "nombre": rol["nombre"],
        "descripcion": rol["descripcion"],
        "hh_en_uf_fijo": rol["hh_en_uf_fijo"],
        "hh_en_uf_honorario": rol["hh_en_uf_honorario"]
    })
    for trabajador in rol["trabajadores"]:
        n_trabajador += 1
        trabajadores.append({
            "id": n_trabajador,
            "rut": trabajador["rut"],
            "nombre": trabajador["nombre"],
            "id_rol": ind_rol+1,
            "iniciacion": trabajador["iniciacion"],
            "termino": trabajador["termino"],
            "modalidad_contrato": trabajador["modalidad_contrato"]
        })
        for disponibilidad in trabajador['disponibilidades']:
            n_disponibilidad += 1
            disponibilidades.append({
                "id": n_disponibilidad,
                "id_trabajador": n_trabajador,
                "dia_semana": disponibilidad['dia_semana'],
                "feriado": disponibilidad['feriado'],
                "hora_inicio": disponibilidad['hora_inicio'],
                "hora_fin": disponibilidad['hora_fin'],
                "horas_dia": disponibilidad['hora_fin']-disponibilidad['hora_inicio']
            })

df_roles = pd.DataFrame(roles)
df_trabajadores = pd.DataFrame(trabajadores)
df_disponibilidades = pd.DataFrame(disponibilidades)

df_roles.to_csv(carpeta_contenido_tablas+"roles.csv", sep=";", index=False)
df_trabajadores.to_csv(carpeta_contenido_tablas+"trabajadores.csv", sep=";", index=False)
df_disponibilidades.to_csv(carpeta_contenido_tablas+"disponibilidades_trabajadores.csv", sep=";", index=False)




######################################
###                                ###
###     GENERAR LISTAS TIPOS       ###
###   INSUMO Y PRECIOS INSUMO      ###
######################################


data_insumos = lee_json(carpeta_inputs+'input03_tipos_insumos.json')
# Listas para poblar los DataFrames
tipos_insumo = []
precios_insumos = []
año_actual = datetime.datetime.now().year

for idx, insumo in enumerate(data_insumos, start=1):
    # Agrega entrada a tipos_insumo
    nprecios = len(insumo.get("precios")) # Si no hay precios indica que no es cobrable
    tipos_insumo.append({
        "id": idx,
        "nombre": insumo["nombre"],
        "descripcion": insumo.get("descripcion"),
        "unidad": insumo.get("unidad"),
        "categoria": insumo.get("categoria"),
        "reutilizable": insumo.get("reutilizable"),
        "retorno_en_n_trabajos": insumo.get("retorno_en_n_trabajos"),
        "seguimiento_automatizado": insumo.get("seguimiento_automatizado"),
        "nivel_critico": insumo.get("nivel_critico"),
        "dias_entrega_referencia": insumo.get("dias_entrega_referencia"),
        "entrega_dias_inhabiles": insumo.get("entrega_dias_inhabiles"),
        "cobrable": nprecios>0
    })

    # Agrega entradas a precios_insumos
    for valoracion in insumo["precios"]:
        if nprecios==0: # Es decir si no es cobrable
            continue # nos saltamos este insumo
        # Agregamos una entrada por año tomando el valor actual y corrigiendo por UF
        for año in range(año_inicio_empresa,año_actual+1):
            precio_año = int(valoracion["precio_por_paquete"]*uf_por_año[str(año)]/uf_por_año[str(año_actual)])
            precios_insumos.append({
                "id": len(precios_insumos) + 1,
                "id_tipo_insumo": idx,
                "precio_por_paquete": precio_año,
                "unidades_por_paquete": valoracion["unidades_por_paquete"],
                "minimo_paquetes_por_compra": valoracion["minimo_paquetes_compra"],
                "fecha_precio": datetime.date(año, 1, 1),
                "fecha_vigencia": datetime.date(año, 12, 31),
                "observaciones": None,
                "proveedor": None,
                "dias_entrega": None,
                "entrega_dias_inhabiles": None,
            })

# Crea los DataFrames
df_tipos_insumo = pd.DataFrame(tipos_insumo)
df_precios_insumos = pd.DataFrame(precios_insumos)

df_tipos_insumo.to_csv(carpeta_contenido_tablas+"tipos_insumo.csv", sep=";", index=False)
df_precios_insumos.to_csv(carpeta_contenido_tablas+"precios_insumos.csv", sep=";", index=False)


##########################################
###                                    ###
###       GENERAR LISTA TIPOS DE       ###
###    TRABAJO, PRECIOS DE TRABAJOS    ###
###   REQUERIMIENTOS DE TRABAJADORES   ###
###   Y REQUERIMIENTOS MATERIALES      ###
##########################################

input_tipos_trabajo = lee_json(carpeta_inputs+'input04_tipos_trabajo.json')


# Inicializar listas para dataframes
tipos_trabajo_df = []
precios_trabajos_df = []
requerimientos_trabajadores_df = []
requerimientos_materiales_df = []

id_tt = 1
id_precio = 1
id_rq_trab = 1
id_rq_mat = 1

for trabajo in input_tipos_trabajo:
    hhs_roles = [] # para calcular la duracion estimada como la hh mas alta dividido por 8
    precio_UF_mano_de_obra = 0
    costo_insumos_por_año =  {año:0 for año in range(año_inicio_empresa,año_actual+1)}
    dias_habiles_entregas= []
    dias_totales_entregas= [] 

    for req in trabajo["requerimientos_trabajadores"]:
        rol = req["rol"]
        id_rol = df_roles[df_roles['nombre']==rol]['id'].values[0]
        horas_rol = req["horas_hombre"]
        hhs_roles.append(horas_rol)
        precio_HH_UF_rol = df_roles[df_roles['nombre']==rol]['hh_en_uf_fijo'].values[0]
        precio_UF_mano_de_obra += horas_rol * precio_HH_UF_rol
        requerimientos_trabajadores_df.append({
            "id": id_rq_trab,
            "id_trabajo_si_aplica": None,
            "id_tipo_trabajo": id_tt,
            "id_rol": id_rol,
            "horas_hombre_requeridas": horas_rol,
            "asignar_feriados": False
        })
        id_rq_trab += 1


    for req in trabajo["requerimientos_materiales"]:
        row_tipo_insumo = df_tipos_insumo[df_tipos_insumo['nombre']==req["nombre_insumo"]]
        id_tipo_insumo = row_tipo_insumo['id'].values[0]
        n_retorno = row_tipo_insumo['retorno_en_n_trabajos'].values[0]
        cobrable = row_tipo_insumo['cobrable'].values[0]
        dias_entrega_referencia = row_tipo_insumo['dias_entrega_referencia'].values[0]
        entrega_dias_inhabiles = row_tipo_insumo['entrega_dias_inhabiles'].values[0]
        if entrega_dias_inhabiles==True:
            dias_totales_entregas.append(dias_entrega_referencia)
        elif entrega_dias_inhabiles==False:
            dias_habiles_entregas.append(dias_entrega_referencia)


        cantidad = req["cantidad"]
        porcentaje = req.get("porcentaje_de_uso", 100)
        requerimientos_materiales_df.append({
            "id": id_rq_mat,
            "id_trabajo_si_aplica": None,
            "id_tipo_trabajo": id_tt,
            "id_tipo_insumo":id_tipo_insumo,
            "cantidad_requerida": cantidad,
            "porcentaje_de_uso": porcentaje,
            "cantidad_ponderada": cantidad*porcentaje/100,
            "observaciones": None
        })
        id_rq_mat += 1
        for año in range(año_inicio_empresa,año_actual+1):
            if cobrable==False: # Si no es cobrable no lo sumamos al precio de los insumos
                continue
            # para cada año calcular el precio de esa cantidad de insumos de ese tipo
            # esto se calcula eligiendo la opcion de compra que nos permite conseguir
            # al menos la cantidad necesaria con el menor monto posible
            # (que no es necesariamente la compra mas conveniente)
            #
            #primero tomamos los precios que corresponden al insumo y año correcto
            df_precios_insumos_año = df_precios_insumos[(pd.to_datetime(df_precios_insumos['fecha_precio']).dt.year==año)
                                                        & (df_precios_insumos['id_tipo_insumo']==id_tipo_insumo)]
            monto_gastado_en_cantidad = decide_compra(df_precios_insumos_año,cantidad)[2]
            # una vez que tenemos el monto gastado en la cantidad a usar calculamos el monto a cobrar
            # por esa compra que para insumos reutilizables es una fraccion del monto gastado
            monto_a_cobrar_por_insumo = monto_gastado_en_cantidad / n_retorno
            # finalmente añadimos el monto al costo de insumos de ese año para ese trabajo
            costo_insumos_por_año[año] += monto_a_cobrar_por_insumo

    # Ahora que ya calculamos el precio en UF del total de la mano de obra 
    # y el costo de los insumos por año podemos valorizar el trabajo para cada año
    # y llenar la tabla precios_trabajos
    for año in range(año_inicio_empresa,año_actual+1):
        costo_mano_de_obra = precio_UF_mano_de_obra * uf_por_año[str(año)]
        costo_insumos = costo_insumos_por_año[año]
        costo_total = costo_mano_de_obra + costo_insumos * (1 + tasa_overhead)
        precio_final = int(costo_total * (1 + margen_ganancia))
        # el precio del año actual lo guardo para indicar el precio estimado en UF del tipo de trabajo
        if año == año_actual:
            precio_actual_pesos = precio_final

        precios_trabajos_df.append({
        "id": id_precio,
        "id_tipo_trabajo": id_tt,
        "precio_por_trabajo": precio_final,
        "minimo_trabajos": 1,
        "maximo_trabajos": None,
        "fecha_precio": datetime.date(año, 1, 1),
        "fecha_vigencia": datetime.date(año, 12, 31),
        "observaciones": None,
        })
        id_precio += 1


    
    # Finalmente ahora que ya tengo la duración en dias estimada (del trabajo y de los insumos)
    # y el precio actual puedo llenar la tabla tipos trabajo
    horas_trabajo_estimados = max(hhs_roles)
    precio_uf_estimado = precio_actual_pesos / uf_por_año[str(año_actual)]
    if len(dias_totales_entregas)==0:
        dias_totales_entrega = None
    else:
        dias_totales_entrega = round(max(dias_totales_entregas),2)
    if len(dias_habiles_entregas)==0:
        dias_habiles_entrega = None
    else:
        dias_habiles_entrega = round(max(dias_habiles_entregas),2)

    tipos_trabajo_df.append({
        "id": id_tt,
        "nombre": trabajo["nombre"],
        "descripcion": trabajo["descripcion"],
        "horas_trabajo_estimados": round(horas_trabajo_estimados,2),
        "dias_totales_entrega_insumos": dias_totales_entrega,
        "dias_habiles_entrega_insumos": dias_habiles_entrega,
        "precio_uf_estimado": round(precio_uf_estimado,2),
    })
    id_tt += 1

# Convertir a DataFrames
df_tipos_trabajo = pd.DataFrame(tipos_trabajo_df)
df_precios_trabajos = pd.DataFrame(precios_trabajos_df)
df_requerimientos_trabajadores = pd.DataFrame(requerimientos_trabajadores_df)
df_requerimientos_materiales = pd.DataFrame(requerimientos_materiales_df)

df_tipos_trabajo.to_csv(carpeta_contenido_tablas+"tipos_trabajo.csv", sep=";", index=False)
df_precios_trabajos.to_csv(carpeta_contenido_tablas+"precios_trabajos.csv", sep=";", index=False)
df_requerimientos_trabajadores.to_csv(carpeta_contenido_tablas+"requerimientos_trabajadores.csv", sep=";", index=False)
df_requerimientos_materiales.to_csv(carpeta_contenido_tablas+"requerimientos_materiales.csv", sep=";", index=False)



##########################################
###                                    ###
###       GENERAR LISTA TIPOS DE       ###
###    SERVICIO Y TIPOS DE SERVICIO    ###
###      A TIPOS DE TRABAJO            ###
##########################################

input_tipos_servicio = lee_json(carpeta_inputs+'input05_tipos_servicio.json')


# Inicializar listas para dataframes
datos_tipos_servicio = []
datos_tsatt = [] # tipos servicio a tipos trabajo

id_tipo_servicio = 1
id_tsatt = 1

for tipo_servicio in input_tipos_servicio:
    horas_trabajo_estimados = 0
    dias_totales_entregas_insumos = []
    dias_habiles_entregas_insumos = []
    precio_uf_estimado = 0
    tipos_trabajo = tipo_servicio['tipos_trabajo']
    lugar_atencion = tipo_servicio['lugar_atencion']
    tipo_maquinaria = tipo_servicio['tipo_maquinaria']
    tipo_cliente = tipo_servicio['tipo_cliente']
    periodicidad_tipica_meses = tipo_servicio['periodicidad_tipica_meses']
    for nombre_trabajo in tipos_trabajo:
        row_trabajo = df_tipos_trabajo[df_tipos_trabajo['nombre'] == nombre_trabajo]
        duracion_trabajo = row_trabajo['horas_trabajo_estimados'].values[0]
        precio = row_trabajo['precio_uf_estimado'].values[0]
        id_tipo_trabajo = row_trabajo['id'].values[0]
        dias_totales_insumos = row_trabajo['dias_totales_entrega_insumos'].values[0]
        dias_habiles_insumos = row_trabajo['dias_habiles_entrega_insumos'].values[0]
        # Filtramos nan's y None's para que queden solo números validos
        if dias_totales_insumos is not None and not np.isnan(dias_totales_insumos):
            dias_totales_entregas_insumos.append(dias_totales_insumos)
        if dias_habiles_insumos is not None and not np.isnan(dias_habiles_insumos):
            dias_habiles_entregas_insumos.append(dias_habiles_insumos)

        horas_trabajo_estimados += duracion_trabajo
        precio_uf_estimado += precio
        datos_tsatt.append({
            'id': id_tsatt,
            'id_tipo_servicio': id_tipo_servicio,
            'id_tipo_trabajo': id_tipo_trabajo,
            })
        id_tsatt += 1
    if len(dias_totales_entregas_insumos)==0:
        dias_totales_entrega = None
    else:
        dias_totales_entrega = round(max(dias_totales_entregas_insumos),2)
    if len(dias_habiles_entregas_insumos)==0:
        dias_habiles_entrega = None
    else:
        dias_habiles_entrega = round(max(dias_habiles_entregas_insumos),2)

    # Acá agregamos el precio del estacionamiento de la maquinaria liviana o pesada
    # que se trabajó en el taller o de forma remota
    if lugar_atencion=='taller':
        if tipo_maquinaria=='liviana':
            precio_uf_estimado += horas_trabajo_estimados/8 * precio_uf_dia_estacionamiento_liviana
        elif tipo_maquinaria=='liviana':
            precio_uf_estimado += horas_trabajo_estimados/8 * precio_uf_dia_estacionamiento_pesada

    datos_tipos_servicio.append({
        'id': id_tipo_servicio,
        'nombre': tipo_servicio['nombre'],
        'descripcion': tipo_servicio['descripcion'],
        'horas_trabajo_estimados': round(horas_trabajo_estimados,2),
        'dias_totales_entrega_insumos': dias_totales_entrega,
        'dias_habiles_entrega_insumos': dias_habiles_entrega,
        'precio_uf_estimado': round(precio_uf_estimado,2),
        'lugar_atencion': lugar_atencion,
        'tipo_maquinaria': tipo_maquinaria,
        'tipo_cliente': tipo_cliente,
        'periodicidad_tipica_meses': periodicidad_tipica_meses,
        })
    id_tipo_servicio += 1

df_tipos_servicio = pd.DataFrame(datos_tipos_servicio)
df_tipos_servicio.to_csv(carpeta_contenido_tablas+"tipos_servicio.csv", sep=";", index=False)

df_tsatt = pd.DataFrame(datos_tsatt)
df_tsatt.to_csv(carpeta_contenido_tablas+"tipos_servicio_a_tipos_trabajo.csv", sep=";", index=False)

##########################################
###                                    ###
###       GENERAR LISTA MOVIMIENTOS    ###
###             RECURRENTES            ###
##########################################

input_movimientos_recurrentes = lee_json(carpeta_inputs+'input06_movimientos_recurrentes.json')

trabajadores_fijos = df_trabajadores[df_trabajadores['modalidad_contrato']=='fijo']
for _,row_trabajador in trabajadores_fijos.iterrows():
     nombre, id_rol, id_trabajador = row_trabajador['nombre'], row_trabajador['id_rol'], row_trabajador['id']
     iniciacion, termino = row_trabajador['iniciacion'], row_trabajador['termino']
     horas_trabajo_dias_habiles = sum(df_disponibilidades[(df_disponibilidades['id_trabajador']==id_trabajador) & 
                                                          (~df_disponibilidades['feriado'])]['horas_dia'])
     hh_en_uf = df_roles[df_roles['id']==id_rol]['hh_en_uf_fijo'].values[0]
     # El salario lo calculo segun el valor de la hh la fracion de pago y las horas trabajadas semanales
     salario_en_uf = hh_en_uf*fraccion_de_hh_en_sueldo*horas_trabajo_dias_habiles*52/12
     input_movimientos_recurrentes.append({
        'nombre': f'Salario de {nombre}',
        'descripcion': 'Calculado como una fracción de su valor de HH al mes en UF',
        'categoria': 'egreso',
        'tipo': 'salario',
        'divisa': 'UF',
        'valor_periodo': 1,
        'unidad_periodo': 'meses',
        'info_extra_recurrencia': None,
        'modo_calculo_monto': 'fijo_en_uf',
        'valor_fijo': salario_en_uf,
        'valor_por_servicio': None,
        'fecha_inicio': iniciacion,
        'fecha_fin': termino
    })

df_movimientos_recurrentes = pd.DataFrame(input_movimientos_recurrentes)
df_movimientos_recurrentes.insert(0, 'id', np.arange(1,len(df_movimientos_recurrentes)+1))
df_movimientos_recurrentes['ultima_actualizacion']=df_movimientos_recurrentes['fecha_inicio']
df_movimientos_recurrentes.to_csv(carpeta_contenido_tablas+"movimientos_recurrentes.csv", sep=";", index=False)