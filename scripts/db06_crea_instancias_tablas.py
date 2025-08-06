print('Comenzando db06', flush=True)
import time
import numpy as np
import yaml
import random as rn
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from peewee import chunked, fn
import os
import sys
from pathlib import Path
print('Importó primeros modulos', flush=True)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.conexion import db

from src.modelos import (Trabajadores, DisponibilidadesTrabajadores, Clientes, TiposInsumo,
                         TiposTrabajo, PreciosInsumos, TiposServicio, Proyectos,
                         MovimientosFinancieros, MovimientosRecurrentes, Insumos, Consumos, Servicios,
                         Cotizaciones, Trabajos, Asignaciones,
                         RequerimientosTrabajadores, RequerimientosMateriales)
from src.modelos import TiposServicioATiposTrabajo as TSATT
from src.utils import (decide_compra, hash_dataframe, define_delta_tiempo, desplazar_dias_habiles,
                       es_feriado, fechahora_a_float_hora, id_actual_modelo, obtener_intervalos_dia,
                       restar_rangos, combina_dia_y_float_hora_en_dt, integral_trapezoide)
import os
import logging
print('Importo segundos modulos', flush=True)

# Ruta base del proyecto
base_dir = Path(__file__).resolve().parent.parent

# Crear carpeta de logs si no existe
log_dir = base_dir / 'logs'
log_dir.mkdir(parents=True, exist_ok=True)
runtime_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = log_dir / f'log_{runtime_timestamp}.log'


logging.basicConfig(
    filename=log_filename,
    filemode='w',  # 'w' para sobrescribir, 'a' para agregar
    format='%(levelname)s - %(message)s',
    level=logging.INFO
)

logging.info("Inicio del programa")
logging.info("")
logging.info("Configuración Usada:")

# Cargar la configuración desde el archivo YAML
config_path = base_dir / 'config.yml'
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)


yaml_string = yaml.dump(config, default_flow_style=False, sort_keys=False)
logging.info('\n'+yaml_string+'\n')



seed = config['instancias']['semilla']
rng_random = rn.Random(seed)
rng_numpy = np.random.default_rng(seed)



str_fecha_inicio_empresa = config['csvs']['fecha_inicio_empresa']
fechahora_inicio_empresa =  datetime.datetime.strptime(str_fecha_inicio_empresa, '%Y/%m/%d')
str_fecha_cierre = config['csvs']['fecha_cierre']
fechahora_cierre =  datetime.datetime.strptime(str_fecha_cierre, '%Y/%m/%d')
max_intentos_compra = config['instancias']['servicios']['max_intentos_compra']
maximo_factor_demora_cr_a_ideal = config['instancias']['servicios']['maximo_factor_demora_cr_a_ideal']
uf_por_año = config['csvs']['precios']['uf_por_año']
probabilidad_perdida_por_rechazo = config['instancias']['clientes']['probabilidad_perdida_por_rechazo']
solo_trabajadores_fijos = config['instancias']['solo_trabajadores_fijos']


print('Paso 3', flush=True)



def filtra_dfs_insumo_consumo(ids_tipos_insumo, df_insumos,df_consumos,inicio_trabajos,fin_trabajos,tipo_filtro):
    """ filtra los dataframes de insumo y consumo para una atención o servicio para que solo tengan
    las ids de tipos de insumo y los periodos relevantes de insumo y consumo.
    """
    assert tipo_filtro in ('atencion', 'servicio'), f"tipo debe ser 'atencion' o 'servicio', no '{tipo_filtro}'"
    
    # Si no hay insumos tampoco hay consumos
    if len(df_insumos)==0:
        return pd.DataFrame([]),pd.DataFrame([])
    # Si es para una atencion en particular solo me sirven los insumos que ya han llegado al momento del inicio de los trabajos
    if tipo_filtro == 'atencion':
        df_insumos_filtrado_original = df_insumos[(df_insumos['fechahora_adquisicion_actualizacion']<=inicio_trabajos) &
                                                  (df_insumos['id_tipo_insumo'].isin(ids_tipos_insumo))]
    # Si es para todas las atenciones del servicio filtro los insumos que llegan en algún momento del intervalo total
    else: # o sea tipo_filtro == 'servicio'
        df_insumos_filtrado_original = df_insumos[(df_insumos['fechahora_adquisicion_actualizacion']<=fin_trabajos) &
                                                  (df_insumos['id_tipo_insumo'].isin(ids_tipos_insumo))]

    ids_insumos_filtrados = sorted(list(set(df_insumos_filtrado_original['id'])))
    df_insumos_filtrado = df_insumos_filtrado_original.copy()
    # Si no hay consumos devolvemos solo el de insumos
    if len(df_consumos)==0:
        return df_insumos_filtrado,pd.DataFrame([])
    cond_id_insumo = df_consumos['id_insumo_si_aplica'].isin(ids_insumos_filtrados)
    cond_desechable = ~df_consumos['insumo_reutilizable']
    cond_reutilizable = ((df_consumos['insumo_reutilizable']) & ((df_consumos['fechahora_inicio_uso']<fin_trabajos) &
                                                            (df_consumos['fechahora_fin_uso']>inicio_trabajos)))
    df_consumos_filtrado_original = df_consumos[cond_id_insumo & (cond_desechable | cond_reutilizable)]
    
    df_consumos_filtrado = df_consumos_filtrado_original.copy()
    return df_insumos_filtrado, df_consumos_filtrado


def calcula_ventanas_estacionamientos(Asignaciones, Trabajos, lugar_atencion,df_estacionamientos_totales,
                                      tipo_maquinaria, fechahora_solicitud,dt_fecha_maxima):
    # Calcula un dataframe con las ventanas disponibles para estacionamientos que ocurren 
    # entre la fecha de solicitud y la fecha maxima
    #
    # WARNING Por ahora solo tengo definidos servicios a realizar en el taller
    if lugar_atencion!='taller':
        return []
    
    row_estacionamiento = df_estacionamientos_totales[df_estacionamientos_totales['tipo_estacionamiento']==tipo_maquinaria]
    total_estacionamientos = row_estacionamiento['cantidad'].values[0]

    query_asignaciones_estacionamientos = (Asignaciones
                                           .select(Asignaciones.fechahora_inicio_ventana, Asignaciones.fechahora_fin_ventana,
                                                   Asignaciones.horas_hombre_asignadas, Trabajos.estacionamiento)
                                           .join(Trabajos, on=(Asignaciones.id_trabajo==Trabajos.id))
                                           .where((Trabajos.estacionamiento.ilike(f"{tipo_maquinaria}%"))
                                                  & (Asignaciones.fechahora_fin_ventana>fechahora_solicitud)
                                                  & (Asignaciones.fechahora_inicio_ventana<dt_fecha_maxima))
                                           .order_by(Asignaciones.fechahora_inicio_ventana))
    df_asignaciones_estacionamientos = pd.DataFrame(list(query_asignaciones_estacionamientos.dicts()))



    datos_ventanas_estacionamientos = []
    for numero_estacionamiento in range(1,total_estacionamientos+1):
        nombre_estacionamiento = tipo_maquinaria + str(numero_estacionamiento)
        if len(df_asignaciones_estacionamientos)>0:
            asignaciones_estacionamiento = df_asignaciones_estacionamientos[df_asignaciones_estacionamientos['estacionamiento']== nombre_estacionamiento]
            asignaciones_est_ordenado = asignaciones_estacionamiento.sort_values('fechahora_inicio_ventana',kind="mergesort", ascending=True)
        else:
            asignaciones_est_ordenado = pd.DataFrame(columns=['vacio'])
        
        fecha_inicio_ventana = fechahora_solicitud
        for _, row in asignaciones_est_ordenado.iterrows():
            datos_ventanas_estacionamientos.append({
                'nombre_estacionamiento': nombre_estacionamiento,
                'inicio_ventana': fecha_inicio_ventana,
                'fin_ventana': row['fechahora_inicio_ventana'],
                'horas_ventana': (row['fechahora_inicio_ventana'] - fecha_inicio_ventana).total_seconds() / 3600            
            })
            fecha_inicio_ventana = row['fechahora_fin_ventana']
        if dt_fecha_maxima>fecha_inicio_ventana:
            datos_ventanas_estacionamientos.append({
                'nombre_estacionamiento': nombre_estacionamiento,
                'inicio_ventana': fecha_inicio_ventana,
                'fin_ventana': dt_fecha_maxima,
                'horas_ventana': (dt_fecha_maxima - fecha_inicio_ventana).total_seconds() / 3600
            })

    df_ventanas_estacionamientos = pd.DataFrame(datos_ventanas_estacionamientos)
    
    return df_ventanas_estacionamientos



def restringir_rangos(df_rangos_a_restringir, lista_ordenada_restricciones):
    """ Restringe un dataframe de rangos de datetimes a que intersecten todos
    con una lista de restricciones
    """
    # Si no hay restriciones solo devuelvo el dataframe original
    if len(lista_ordenada_restricciones)==0:
        return (len(df_rangos_a_restringir)==0), df_rangos_a_restringir
    
    col_inicio, col_fin = ['dt_inicio','dt_fin']
    restricciones_min = lista_ordenada_restricciones[0][0]
    restricciones_max = lista_ordenada_restricciones[-1][1]
    # Lo primero que hago es restringir todo el dataframe al rango global de restricciones
    # para excluir filas que definitivamente no intersectan ningún rango de restricción
    df_rangos = df_rangos_a_restringir[(df_rangos_a_restringir[col_fin]>restricciones_min) &
                                       (df_rangos_a_restringir[col_inicio]<restricciones_max)]
    # Defino el dataframe a recorrer que parte siendo igual a df_rangos pero va cambiando para
    # cada restriccion
    df_revisar = df_rangos.copy()
    # Y tambien defino el dataframe para los resultados usando el mismo formato que el datafrmae
    # original pero sin datos
    df_resultado = df_rangos_a_restringir.iloc[0:0].copy()
    ind_restriccion = 0
    # Mantengo la iteracion mientras queden restricciones que revisar y dataframe que revisar
    while ( (ind_restriccion < len(lista_ordenada_restricciones)) and (not df_revisar.empty)):
        restriccion = lista_ordenada_restricciones[ind_restriccion]
        tir, tfr = restriccion

        # Condición 1: agregar al resultado (modificado), no seguir revisando
        df_fin_intermedio = df_revisar[(df_revisar[col_fin]>tir) & (df_revisar[col_fin]<=tfr)]
        df_resultado1 = df_fin_intermedio.copy()
        df_resultado1[col_inicio] = df_resultado1[col_inicio].clip(lower=tir)
        df_resultado1["hora_inicio"] = fechahora_a_float_hora(df_resultado1[col_inicio])
        df_resultado1["horas_ventana"] = df_resultado1["hora_fin"] - df_resultado1["hora_inicio"]
        

        # Condición 2: agregar al resultado (modificado) y también para seguir revisando (modificado diferente)
        df_fin_pasado_traslapante = df_revisar[(df_revisar[col_fin]>tfr) & (df_revisar[col_inicio]<tfr)]
        df_resultado2 = df_fin_pasado_traslapante.copy()
        df_revisar2 = df_fin_pasado_traslapante.copy()
        df_resultado2[col_inicio] = df_resultado2[col_inicio].clip(lower=tir)
        df_resultado2['hora_inicio'] = fechahora_a_float_hora(df_resultado2[col_inicio])
        df_resultado2[col_fin] = tfr # al asignar un escalar python degrada a us entonces hay que volver a ns
        df_resultado2[col_fin] = df_resultado2[col_fin].astype("datetime64[ns]")
        df_resultado2['hora_fin'] = fechahora_a_float_hora(df_resultado2[col_fin])
        df_resultado2['horas_ventana'] = df_resultado2['hora_fin'] - df_resultado2['hora_inicio']
        df_revisar2[col_inicio] = df_revisar2[col_inicio].astype("datetime64[ns]")
        df_revisar2['hora_inicio'] = fechahora_a_float_hora(df_revisar2[col_inicio])
        df_revisar2['horas_ventana'] = df_revisar2['hora_fin'] - df_revisar2['hora_inicio']

        # Condición 3: no agregar al resultado, pero sí seguir revisando (sin modificar)
        df_fin_pasado_fuera = df_revisar[(df_revisar[col_fin]>tfr) & (df_revisar[col_inicio]>=tfr)]
        df_revisar3 = df_fin_pasado_fuera.copy()

        # Unir nuevos resultados y nueva tanda a revisar
        df_resultado = pd.concat([df_resultado, df_resultado1, df_resultado2], ignore_index=True)
        df_revisar = pd.concat([df_revisar2, df_revisar3], ignore_index=True)

        ind_restriccion += 1
    exito_restriccion = (len(df_resultado)!=0)
    return exito_restriccion, df_resultado



def excluye_asignaciones_de_ventanas(df_ventanas, df_asignaciones, tipo):
    """
    Excluye de las ventanas de disponibilidad las ventanas cubiertas por las asignaciones.

    Parámetros:
        df_ventanas: DataFrame con ventanas de disponibilidad (de trabajadores o estacionamientos).
        df_asignaciones: DataFrame con asignaciones que deben ser descontadas.
        tipo: 'trabajadores' o 'estacionamientos'
    """
    assert tipo in ('trabajadores', 'estacionamientos'), f"tipo debe ser 'trabajadores' o 'estacionamientos', no '{tipo}'"
    
    if tipo == 'trabajadores':
        nombre_columna = 'id_trabajador'
        dt_inicio_col = 'dt_inicio'
        dt_fin_col = 'dt_fin'
    else:  # tipo == 'estacionamientos'
        nombre_columna = 'nombre_estacionamiento'
        dt_inicio_col = 'inicio_ventana'
        dt_fin_col = 'fin_ventana'


    for _, row in df_asignaciones.iterrows():
        indices_a_borrar = []
        filas_a_agregar = []
        if tipo == 'trabajadores':
            id_objeto = row['id_trabajador']
        else:  # tipo == 'estacionamientos'
            id_objeto = row['estacionamiento']

        inicio_asignacion = row['fechahora_inicio_ventana'].to_pydatetime()
        fin_asignacion = row['fechahora_fin_ventana'].to_pydatetime()

        condicion = ((df_ventanas[nombre_columna] == id_objeto) &
                     ~(df_ventanas[dt_fin_col] <= inicio_asignacion) &
                     ~(df_ventanas[dt_inicio_col] >= fin_asignacion))

        df_afectadas = df_ventanas[condicion]
        df_no_afectadas = df_ventanas[~condicion]

        for ind_fila, fila in df_afectadas.iterrows():
            inicio_ventana = fila[dt_inicio_col].to_pydatetime()
            fin_ventana = fila[dt_fin_col].to_pydatetime()

            nuevos_rangos = restar_rangos([inicio_ventana, fin_ventana],
                                           [inicio_asignacion, fin_asignacion])

            if len(nuevos_rangos) == 0:
                indices_a_borrar.append(ind_fila)

            if len(nuevos_rangos) >= 1:
                df_afectadas.loc[ind_fila, dt_inicio_col] = nuevos_rangos[0][0]
                df_afectadas.loc[ind_fila, dt_fin_col] = nuevos_rangos[0][1]
                if tipo == 'trabajadores':
                    hi = fechahora_a_float_hora(nuevos_rangos[0][0])
                    hf = fechahora_a_float_hora(nuevos_rangos[0][1])
                    df_afectadas.loc[ind_fila, 'hora_inicio'] = hi
                    df_afectadas.loc[ind_fila, 'hora_fin'] = hf
                    df_afectadas.loc[ind_fila, 'horas_ventana'] = hf - hi
                else:
                    duracion = (nuevos_rangos[0][1] - nuevos_rangos[0][0]).total_seconds() / 3600
                    df_afectadas.loc[ind_fila, 'horas_ventana'] = duracion

            if len(nuevos_rangos) == 2:
                if tipo == 'trabajadores':
                    hi2 = fechahora_a_float_hora(nuevos_rangos[1][0])
                    hf2 = fechahora_a_float_hora(nuevos_rangos[1][1])
                    horas_ventana = hf2 - hi2
                    nueva_fila = [
                        fila['id_trabajador'], fila['id_rol'], fila['dia'], fila['dia_semana'],
                        hi2, hf2, horas_ventana, nuevos_rangos[1][0], nuevos_rangos[1][1]
                    ]
                    filas_a_agregar.append(nueva_fila)
                else:
                    duracion = (nuevos_rangos[1][1] - nuevos_rangos[1][0]).total_seconds() / 3600
                    filas_a_agregar.append({
                        'nombre_estacionamiento': id_objeto,
                        'inicio_ventana': nuevos_rangos[1][0],
                        'fin_ventana': nuevos_rangos[1][1],
                        'horas_ventana': duracion
                    })

        df_afectadas = df_afectadas.drop(index=indices_a_borrar)

        if len(filas_a_agregar) > 0:
            if tipo == 'trabajadores':
                columnas = df_afectadas.columns
                df_agregar = pd.DataFrame(filas_a_agregar, columns=columnas)
            else:
                df_agregar = pd.DataFrame(filas_a_agregar)
            df_afectadas = pd.concat([df_afectadas, df_agregar], ignore_index=True)

        df_ventanas = pd.concat([df_no_afectadas, df_afectadas], ignore_index=True)

    df_ventanas = df_ventanas.sort_values(by=dt_inicio_col,kind="mergesort", ascending=True).reset_index(drop=True)
    return df_ventanas



def calcula_disponibilidad_insumo_y_por_id_detallado(df_insumos_filtrado, df_consumos_filtrado,id_tipo_insumo,
                                                     es_reutilizable,seguimiento_automatizado):
    """Calcula disponibilidad global y por cada id_insumo en el instante de máximo consumo"""
    disponibilidad_por_id = {}

    # Lo primero es que si no hay insumos todo lo disponible es 0
    if len(df_insumos_filtrado)==0 or id_tipo_insumo not in set(df_insumos_filtrado['id_tipo_insumo']):
        return 0, 0.0, disponibilidad_por_id
    
    # Si hay insumos entonces calculamos la disponibilidad
    else:
        df_insumos_tipo = df_insumos_filtrado[df_insumos_filtrado['id_tipo_insumo']==id_tipo_insumo].sort_values('id',kind="mergesort")
        n_insumos = sum(df_insumos_tipo['cantidad'])

        # Si no hay consumos o no hay seguimiento automatizado asumimos que todos los insumos estan disponibles
        if not seguimiento_automatizado or len(df_consumos_filtrado)==0 or id_tipo_insumo not in set(df_consumos_filtrado['id_tipo_insumo']):
            for _, row in df_insumos_tipo.iterrows():
                disponibilidad_por_id[row['id']] = (row['cantidad'], 100.0)
            return n_insumos, 100.0, disponibilidad_por_id


        # Si hay consumos y seguimiento entonces calculamos el consumo para determinar cuanto está disponible
        else:
            rows_consumo = df_consumos_filtrado[df_consumos_filtrado['id_tipo_insumo']==id_tipo_insumo]
            
            # Si es reutilizable calculo lo disponible (global y por id_insumo) como lo libre en el momento
            # de mayor consumo
            if es_reutilizable:
                # Gardamos cada inicio o fin de un consumo como un evento aumento o disminución del uso ponderado respectivamente
                eventos = []
                for _, row in rows_consumo.iterrows():
                    eventos.append((row['fechahora_inicio_uso'], +row['uso_ponderado']))
                    eventos.append((row['fechahora_fin_uso'], -row['uso_ponderado']))
                eventos.sort()
                # Luego recorro los eventos y sus deltas de uso para calcular el momento de mayor consumo y el consumo asociado
                uso_actual = 0
                uso_max = 0
                tiempo_max = None
                for tiempo, delta in eventos:
                    uso_actual += delta
                    if uso_actual > uso_max:
                        uso_max = uso_actual
                        tiempo_max = tiempo

                # Filtrar consumos activos en el momento de uso maximo
                activos = rows_consumo[
                    (rows_consumo['fechahora_inicio_uso'] < tiempo_max) &
                    (rows_consumo['fechahora_fin_uso'] > tiempo_max)]

                # Finalmente calculamos la disponibilidad por id y la global
                for _, row in df_insumos_tipo.iterrows():
                    id_insumo = row['id']
                    cantidad_total = row['cantidad']
                    cantidad_en_uso = activos[activos['id_insumo_si_aplica'] == id_insumo]['uso_ponderado'].sum()
                    porcentaje_disponible = round(100 * (cantidad_total - cantidad_en_uso) / cantidad_total, 9)
                    disponibilidad_por_id[id_insumo] = (cantidad_total, porcentaje_disponible)

                porcentaje_global = round(100 * (n_insumos - uso_max) / n_insumos, 9)
                return n_insumos, porcentaje_global, disponibilidad_por_id

            # Si es desechable simplemente resto los consumos que son por definicion al 100%
            else:
                usados_por_id = rows_consumo.groupby('id_insumo_si_aplica')['cantidad'].sum().to_dict()
                for _, row in df_insumos_tipo.iterrows():
                    usados = usados_por_id.get(row['id'], 0)
                    disponibles = row['cantidad'] - usados
                    n_insumos -= usados
                    disponibilidad_por_id[row['id']] = (disponibles, 100)
                return n_insumos, 100, disponibilidad_por_id




def simular_negociacion(precio_cobrado, precio_esperado, 
                        fechahora_solicitud, fechahora_esperada, fin_trabajos):
    """
    Calcula si una negociación es aceptada y retorna (resultado: bool, probabilidad: float).
    La aceptación depende del precio y la demora comparados con los valores esperados.
    """
    cuociente_precio = precio_cobrado / precio_esperado
    demora_real = (fin_trabajos - fechahora_solicitud).total_seconds()
    demora_esperada = (fechahora_esperada - fechahora_solicitud).total_seconds()
    cuociente_demora = demora_real / demora_esperada if demora_esperada > 0 else float('inf')

    prob_precio = min(1, 10 ** (1 - cuociente_precio))
    prob_demora = min(1, 10 ** (1 - cuociente_demora))

        
    probabilidad_final = prob_precio * prob_demora
    aceptado = rng_random.random() < probabilidad_final

    if aceptado is True:
        razon = 'ninguna'
    else:
        if prob_precio < 1 and prob_demora == 1:
            razon = 'precio'
        elif prob_precio == 1 and prob_demora < 1:
            razon = 'demora'
        elif prob_precio < 1 and prob_demora < 1:
            razon = 'precio_y_demora'
        else:
            razon = 'caso_limite'


    return aceptado, probabilidad_final, razon


def crea_dict_para_movimiento_financiero(id_mov, fechahora_movimiento,
                                         categoria,tipo,monto,descripcion,incluye_iva,deducible,id_gasto_recurrente=None):
    mes_movimiento, año_movimiento = fechahora_movimiento.month, fechahora_movimiento.year
    dic = {
        'id': id_mov,
        'fechahora_movimiento': fechahora_movimiento,
        'numero_mes_balance': mes_movimiento,
        'numero_año_balance': año_movimiento,
        'categoria': categoria,
        'tipo': tipo,
        'monto': monto,
        'divisa': 'CLP',
        'descripcion': descripcion,
        'incluye_iva': incluye_iva,
        'deducible': deducible,
        'nombre_y_carpeta_archivo_boleta': None,
        'lugar_fisico_boleta': None,
        'id_gasto_recurrente_si_aplica': id_gasto_recurrente
    }
    return dic


def crea_dataframes_fijos(Servicios, Cotizaciones, Proyectos, Clientes, TiposServicio,
                          DisponibilidadesTrabajadores, Trabajadores, config, trabajadores_fijos=True):
    """Crea todos los dataframes utilizados por todos los servicios"""
    info_estacionamientos = config['instancias']['estacionamientos']
    datos_estacionamientos_totales = []
    id_est = 0
    for tipo_estacionamiento, cantidad in info_estacionamientos.items():
        id_est += 1
        datos_estacionamientos_totales.append([id_est, tipo_estacionamiento, cantidad])
    df_estacionamientos_totales = pd.DataFrame(datos_estacionamientos_totales,columns=['id','tipo_estacionamiento','cantidad'])

    query_servicios = (Servicios
                    .select(Servicios.id,Servicios.id_proyecto,Servicios.ids_tipo_servicio,
                            Servicios.unidad_tipo_servicio,Servicios.fecha_solicitud,
                            Servicios.fecha_esperada, Servicios.fecha_limite_planificacion, Servicios.demora_pago_dias,
                            Servicios.fecha_inicio_trabajos, Servicios.fecha_fin_trabajos,
                            fn.substring(Cotizaciones.descripcion, 'precio_esperado=([0-9]+)').cast('int').alias('precio_esperado'),
                            Cotizaciones.total_estimado, Clientes.id.alias('id_cliente'), Clientes.es_empresa,
                            TiposServicio.horas_trabajo_estimados, TiposServicio.dias_habiles_entrega_insumos,
                            TiposServicio.dias_totales_entrega_insumos, TiposServicio.lugar_atencion,
                            TiposServicio.tipo_maquinaria )
                    .join(Cotizaciones, on=(Cotizaciones.id_servicio==Servicios.id))
                    .join(Proyectos, on=(Servicios.id_proyecto==Proyectos.id))
                    .join(Clientes, on=(Proyectos.id_cliente==Clientes.id))
                    .join(TiposServicio, on=(Servicios.ids_tipo_servicio.cast('int')==TiposServicio.id))
                    .order_by(Servicios.fecha_solicitud, Servicios.id)
    )

    df_servicios = pd.DataFrame(list(query_servicios.dicts()))
    df_servicios['precio_esperado'] = pd.to_numeric(df_servicios['precio_esperado'])

    # Luego definimos las disponibilidades de los trabajadores consultando las tablas
    query_disponibilidad_trabajadores = (DisponibilidadesTrabajadores
                                            .select(DisponibilidadesTrabajadores,Trabajadores.id_rol,
                                                    Trabajadores.modalidad_contrato)
                                            .join(Trabajadores, on=(DisponibilidadesTrabajadores.id_trabajador
                                                                    ==Trabajadores.id))
                                            .order_by(DisponibilidadesTrabajadores.id))
    df_disponibilidad_trabajadores = pd.DataFrame(list(query_disponibilidad_trabajadores.dicts()))

    # Y los parametros de los trabajadores para limitar las disponibilidades a los periodos en que estuvieron contratados
    df_trabajadores = pd.DataFrame(list(Trabajadores.select().order_by(Trabajadores.id).dicts()))

    # Si trabajamos solo con trabajadores fijos filtramos las disponibilidades de trabajadores y los trabajadores
    if trabajadores_fijos is True:
        df_disponibilidad_trabajadores = (df_disponibilidad_trabajadores
                                          [df_disponibilidad_trabajadores['modalidad_contrato']=='fijo']
                                          .sort_values('id').reset_index(drop=True))
        df_trabajadores = (df_trabajadores[df_trabajadores['modalidad_contrato']=='fijo']
                           .sort_values('id').reset_index(drop=True))


    ids_roles_totales = sorted(list(set(df_trabajadores['id_rol'])))
    df_ventanas_trabajadores_totales = calcula_ventanas_trabajadores(Asignaciones, Trabajadores,
                                                                     df_disponibilidad_trabajadores,
                                                                     ids_roles_totales,
                                                                     fechahora_inicio_empresa,
                                                                     fechahora_cierre, df_trabajadores,
                                                                     solo_fijos = trabajadores_fijos)

    return (df_estacionamientos_totales, df_servicios, df_disponibilidad_trabajadores,
            df_trabajadores, df_ventanas_trabajadores_totales)



def crea_dataframes_variables(Insumos, Consumos):
    """Crea los dataframes que cambian de servicio a servicio"""
    #Aqui leo la informacion de los estacionamientos una vez que ya hay disponibilidad registrada en insumos

    query_consumos = (Consumos
                      .select(Consumos.id_tipo_insumo,
                              Consumos.cantidad,
                              Consumos.porcentaje_de_uso.alias('porcentaje_uso'),
                              Consumos.uso_ponderado,
                              Consumos.fechahora_inicio_uso,
                              Consumos.fechahora_fin_uso,
                              Consumos.id_insumo_si_aplica,
                              TiposInsumo.reutilizable.alias('insumo_reutilizable'))
                      .join(TiposInsumo, on=(Consumos.id_tipo_insumo == TiposInsumo.id))
                      .order_by(Consumos.id))
    df_consumos = pd.DataFrame(list(query_consumos.dicts()))

    if df_consumos.empty:
    # Si el dataframe está vacío entonces le damos el mismo formato que los de la tabla
        df_consumos = pd.DataFrame({
            'id_tipo_insumo': pd.Series(dtype='int'),
            'cantidad': pd.Series(dtype='int'),
            'porcentaje_uso': pd.Series(dtype='float'),
            'uso_ponderado': pd.Series(dtype='float'),
            'fechahora_inicio_uso': pd.Series(dtype='datetime64[ns]'),
            'fechahora_fin_uso': pd.Series(dtype='datetime64[ns]'),
            'id_insumo_si_aplica': pd.Series(dtype='int'),
            'insumo_reutilizable': pd.Series(dtype='boolean')
        })


    query_insumos = (Insumos.select(Insumos.id, Insumos.id_tipo_insumo, Insumos.cantidad,
                                   Insumos.fechahora_adquisicion_actualizacion)
                     .order_by(Insumos.id))
    # Insumos lo podemos definir según la tabla porque ya tiene las entradas de los estacionamientos
    df_insumos = pd.DataFrame(list(query_insumos.dicts()))

    if df_insumos.empty:
    # Si el dataframe está vacío entonces le damos el mismo formato que los de la tabla
        df_insumos = pd.DataFrame({
            'id': pd.Series(dtype='int'),
            'id_tipo_insumo': pd.Series(dtype='int'),
            'cantidad': pd.Series(dtype='int'),
            'fechahora_adquisicion_actualizacion': pd.Series(dtype='datetime64[ns]')
        })

    return df_insumos, df_consumos


def determina_parametros_servicio(row_servicio, TSATT, RequerimientosTrabajadores, Asignaciones,
                                  Trabajadores, RequerimientosMateriales, TiposInsumo, Trabajos,
                                  TiposServicio, df_insumos, df_consumos, trabajadores_fijos=True):
    id_proyecto = row_servicio['id_proyecto']
    id_servicio = row_servicio['id']
    fechahora_solicitud = row_servicio['fecha_solicitud'].to_pydatetime()
    fechahora_esperada = row_servicio['fecha_esperada'].to_pydatetime()
    fecha_maxima = row_servicio['fecha_limite_planificacion']
    precio_esperado = row_servicio['precio_esperado']
    precio_total = row_servicio['total_estimado']
    fechahora_maxima = datetime.datetime.combine(fecha_maxima+datetime.timedelta(days=1),
                                            datetime.datetime.min.time())
    ids_tipo_servicio = row_servicio['ids_tipo_servicio']
    tipo_maquinaria = row_servicio['tipo_maquinaria']
    lugar_atencion = row_servicio['lugar_atencion']
    numero_maquinas = row_servicio['unidad_tipo_servicio']
    horas_trabajo_por_maquina = row_servicio['horas_trabajo_estimados']
    demora_pago_dias = row_servicio['demora_pago_dias']
    # Primero obtengo el nombre del servicio
    query_tipos_servicio = (TiposServicio
                            .select(TiposServicio.nombre)
                            .where(TiposServicio.id==ids_tipo_servicio)
                            .order_by(TiposServicio.id))
    nombre_servicio = list(query_tipos_servicio.dicts())[0]['nombre']

    # Definimos la id actual mas alta de los insumos al momento antes de que empiece el servicio
    # para ir asignando desde acá las ids de los nuevos insumos
    # para cada intento reseteamos la id para que parta desde acá
    max_id_insumos_antes_servicio = max(df_insumos['id']) if len(df_insumos) > 0 else 0


    info_ids_tipo_trabajo = list(TSATT.select(TSATT.id_tipo_trabajo)
                                .where(TSATT.id_tipo_servicio==ids_tipo_servicio).order_by(TSATT.id).dicts())
    ids_tipo_trabajo = [el['id_tipo_trabajo'] for el in info_ids_tipo_trabajo]

    # WARNING: EN LA SIMULACION NO TIENE MUCHO SENTIDO TENER INSUMOS SIN SEGUIMIENTO AUTOMATIZADO
    # PORQUE EL SEGUIMIENTO ES COMPLETO, TIENE MAS SENTIDO PARA LA EJECUCIÓN REAL DONDE
    # SE NECESITA HACER UN SISTEMA ESPECIAL DE SEGUIMIENTO MANUAL PARA ESTOS INSUMOS
    query_req_materiales = (RequerimientosMateriales.select(RequerimientosMateriales.id_tipo_trabajo,
                                                            TiposInsumo.id.alias('id_tipo_insumo'),
                                                            TiposInsumo.nombre,
                                                            RequerimientosMateriales.cantidad_requerida,
                                                            RequerimientosMateriales.porcentaje_de_uso,
                                                            RequerimientosMateriales.cantidad_ponderada,
                                                            TiposInsumo.reutilizable,
                                                            TiposInsumo.seguimiento_automatizado)
                            .where(RequerimientosMateriales.id_tipo_trabajo.in_(ids_tipo_trabajo))
                            .join(TiposInsumo,on=(RequerimientosMateriales.id_tipo_insumo==TiposInsumo.id))
                            .order_by(RequerimientosMateriales.id))

    req_materiales = pd.DataFrame(list(query_req_materiales.dicts()))
    # Calculo el momento en el que pueden empezar los trabajos como el momento en que llegan los insumos en caso
    # que haya 0 consumo de ellos, es decir lo más rápido que pueden llegar
    fechahora_minima = calcula_minimo_tiempo_entrega(req_materiales,numero_maquinas, df_insumos,
                                                     TiposInsumo, fechahora_solicitud)
    tipos_insumo = sorted(list(set(req_materiales['id_tipo_insumo'])))

    (df_insumos_servicio, df_consumos_servicio
    ) = filtra_dfs_insumo_consumo(tipos_insumo, df_insumos,df_consumos,fechahora_minima,fechahora_maxima,'servicio')

    df_ventanas_estacionamientos = calcula_ventanas_estacionamientos(Asignaciones, Trabajos, lugar_atencion,
                                                                     df_estacionamientos_totales, tipo_maquinaria,
                                                                     fechahora_minima,fechahora_maxima)


    query_req_trabajadores = (RequerimientosTrabajadores
                            .select(RequerimientosTrabajadores.id_tipo_trabajo,
                                    RequerimientosTrabajadores.id_rol,
                                    RequerimientosTrabajadores.horas_hombre_requeridas)
                            .where(RequerimientosTrabajadores.id_tipo_trabajo.in_(ids_tipo_trabajo))
                            .order_by(RequerimientosTrabajadores.id))


    req_trabajadores = pd.DataFrame(list(query_req_trabajadores.dicts()))
    ids_roles_involucrados = sorted(list(set(req_trabajadores['id_rol'].values)))
    df_ventanas_trabajadores = calcula_ventanas_trabajadores(Asignaciones, Trabajadores, df_disponibilidad_trabajadores,
                                                            ids_roles_involucrados, fechahora_minima, fechahora_maxima,
                                                            df_trabajadores, solo_fijos=trabajadores_fijos)
    return (df_ventanas_trabajadores,df_ventanas_estacionamientos, req_trabajadores,
            fechahora_minima, fechahora_maxima, ids_tipo_servicio, ids_tipo_trabajo, req_materiales,
            numero_maquinas,max_id_insumos_antes_servicio, fechahora_solicitud,fechahora_esperada,
            df_insumos_servicio, df_consumos_servicio, horas_trabajo_por_maquina,precio_esperado,
            precio_total, id_proyecto, id_servicio, nombre_servicio, demora_pago_dias, tipo_maquinaria)




def determina_asignaciones_servicio(df_ventanas_trabajadores, df_ventanas_estacionamientos,
                                    req_trabajadores_servicio,numero_maquinas):
    """
    Intenta asignar trabajadores y estacionamientos para todas las máquinas de un servicio.
    Devuelve éxito y el DataFrame con todas las asignaciones si fue posible completar el servicio.
    """
    # Genero un dataframe donde voy a guardar todas las asignaciones de trabajadores para despues
    # si las asignaciones son exitosas actualizar la tabla Asignaciones y DisponibilidadesTrabajadores
    # para el proximo servicio
    df_asignaciones_servicio = pd.DataFrame([])
    # Por cada atención (máquina)...
    for n_maquina in range(1,numero_maquinas+1):
        # Determino de ser posible todas las asignaciones de trabajadores necesarias para la atención
        # minimizando el tiempo total y respetando que las lagunas sin trabajo no sean mayores que el maximo
        hash_trabajadores_atencion = hash_dataframe(df_ventanas_trabajadores)
        hash_estacionamientos_atencion = hash_dataframe(df_ventanas_estacionamientos)
        exito_asignacion, df_asignaciones_atencion = determina_asignaciones_atencion(df_ventanas_trabajadores,
                                                                                     df_ventanas_estacionamientos,
                                                                                     req_trabajadores_servicio)
        # Si falla una atención falla todo el servicio lo que queda guardado en exito_asignacion
        # lo que termina todos los intentos para el servicio
        if exito_asignacion == False:
            return False, df_asignaciones_servicio


        # Si la asignacion es exitosa primero actualizo los dataframes para ajustar las ventanas
        # de disponibilidad
        hash_asignaciones = hash_dataframe(df_asignaciones_atencion)
        logging.info(f'Con determina_asignaciones_atencion para la maquina {n_maquina} usamos de entrada el df_ventanas_trabajadores de hash {hash_trabajadores_atencion} y el df_ventanas_estacionamientos de hash {hash_estacionamientos_atencion}')
        logging.info(f'Con lo que obtuvmos las asignaciones de hash {hash_asignaciones}')
        
        df_ventanas_trabajadores = excluye_asignaciones_de_ventanas(df_ventanas_trabajadores,
                                                                    df_asignaciones_atencion,
                                                                    'trabajadores')
        df_ventanas_estacionamientos = excluye_asignaciones_de_ventanas(df_ventanas_estacionamientos,
                                                                        df_asignaciones_atencion,
                                                                        'estacionamientos')

        hash_trabajadores_despues = hash_dataframe(df_ventanas_trabajadores)
        hash_estacionamientos_despues = hash_dataframe(df_ventanas_estacionamientos)

        logging.info(f'Luego al excluir las asignaciones de las ventanas de los trabajadores obtenemos el dataframe  de hash {hash_trabajadores_despues}')
        logging.info(f'Luego al excluir las asignaciones de las ventanas de los estacionamientos obtenemos el dataframe  de hash {hash_estacionamientos_despues}')
        # Y antes de ir a la atención de la próxima máquina actualizo el dataframe de las asignaciones del servicio
        df_asignaciones_atencion['n_maquina'] = n_maquina
        df_asignaciones_servicio = pd.concat([df_asignaciones_servicio,df_asignaciones_atencion], ignore_index=True)
    # Si llega hasta acá sin haber retornado es porque todas las asignaciones fueron exitosas
    df_asignaciones_servicio = df_asignaciones_servicio.reset_index(drop=True)
    return True, df_asignaciones_servicio


def determina_asignaciones_atencion(df_ventanas_trabajadores, df_ventanas_estacionamientos, req_trabajadores_servicio,
                                    maximo_factor_demora = maximo_factor_demora_cr_a_ideal):
    """
    Función para determinar las asignaciones de trabajadores correspondientes a una atóncion (maquina)
    iterando sobre todos los estacionamientos donde esta se puede atenter.
    """
        
    #Iteramos por estacionamiento para intentar asignar la atencion a cada uno de ellos y elegir el mejor
    #o uno razonablemente bueno que nos evite la iteracion sobre el resto
    #Partimos por simular una asignacion con un estacionamiento "ideal" que se desocupa tan rapido
    #como el primero disponible y esta disponible en todo el rango hasta la ultima disponibilidad
    #asi nos sirve para evaluar que tan buenas son las asignaciones de cada 
    #estacionamiento reales y detener la iteración sobre estacionamientos si la asignacion es suficientemente
    #rápida.
    #
    #Si no hay ventanas disponibles intersectantes entre las de estacionamientos y trabajadores se corta
    #la función y se devuelve no exito, lo mismo si la asignacion ideal falla.
    #Al iterar entre los estacionamientos se va determinando el que termina maás rápido, y si ninguno es exitoso
    #se determina no exito
    
    estacionamiento_elegido = None
    asignaciones_atencion_final = None
    exito_asignacion_total = False

    primera_disponibilidad = min(df_ventanas_estacionamientos['inicio_ventana'])
    ultima_disponibilidad  = max(df_ventanas_estacionamientos['fin_ventana'])
    disponibilidad_total_horas = (ultima_disponibilidad - primera_disponibilidad).total_seconds()/3600
    lista_ordenada_restricciones = [[primera_disponibilidad, ultima_disponibilidad]]
    exito_restriccion, df_ventanas_ideal = restringir_rangos(df_ventanas_trabajadores, lista_ordenada_restricciones)
    # Si la restriccion ideal de ventanas no es exitosa entonces toda la asignacion es fallida
    if not exito_restriccion:
        return exito_asignacion_total, asignaciones_atencion_final
    # Ahora hacemos las asignaciones para la combinacion atención/estacionamiento
    exito_asignacion_atencion_ideal, asignaciones_atencion_ideal = determina_asignaciones_atencion_est(req_trabajadores_servicio,
                                                                                                       df_ventanas_ideal)
    # Si la asignacion ideal fue exitosa
    if exito_asignacion_atencion_ideal:
        # Lo primero que vemos es revisar si hay algún estacionamiento que sea equivalente al ideal
        df_ventanas_ideales = df_ventanas_estacionamientos[(df_ventanas_estacionamientos['inicio_ventana']==primera_disponibilidad) &
                                                           (df_ventanas_estacionamientos['fin_ventana']==ultima_disponibilidad) & 
                                                           (abs(df_ventanas_estacionamientos['horas_ventana']-disponibilidad_total_horas)<1)]
        
        # si existe entonces las asignaciones ideales las asignamos a la de cualquiera de los estacionamientos ideales
        estacionamientos_ideales = sorted(list(set(df_ventanas_ideales['nombre_estacionamiento'])))
        if len(estacionamientos_ideales)>0:
            estacionamiento_elegido = estacionamientos_ideales[0]
            asignaciones_atencion_final = asignaciones_atencion_ideal
            asignaciones_atencion_final['estacionamiento'] = estacionamiento_elegido
            return True, asignaciones_atencion_final
        # Si no hay un estacionamiento ideal entonces itero entre los estacionamientos para encontrar uno lo suficientemente bueno
        else:
            # Primero calculo los nombres de los estacionamientos ordenados por los que tienen mas horas primero
            horas_por_estacionamiento = df_ventanas_estacionamientos.groupby('nombre_estacionamiento')['horas_ventana'].sum()
            orden_nombres_estacionamientos = horas_por_estacionamiento.sort_values(ascending=False,kind="mergesort").index.to_list()
            # Luego definimos las variables para guardar las asignaciones
            mejor_estacionamiento, mejor_asignacion, finalizacion_mas_pronta = None, None, None
            
            inicio_atencion_ideal = min(asignaciones_atencion_ideal['fechahora_inicio_ventana'])
            fin_atencion_ideal = max(asignaciones_atencion_ideal['fechahora_fin_ventana'])
            ind_estacionamiento = 0
            # Mientras queden estacionamientos por revisar y no hayamos encontrado uno lo suficientemente bueno...
            while (not exito_asignacion_total) and (ind_estacionamiento < len(orden_nombres_estacionamientos)):
                # Primero combinamos las ventanas de disponibilidad
                # de los trabajadores y del estacionamiento en un solo grupo de ventanas globales
                nombre_estacionamiento = orden_nombres_estacionamientos[ind_estacionamiento]
                df_ventanas_estacionamiento_ord = df_ventanas_estacionamientos[df_ventanas_estacionamientos['nombre_estacionamiento']==
                                                                               nombre_estacionamiento]
                lista_ordenada_restricciones = [[row['inicio_ventana'],row['fin_ventana']]
                                                for _,row in df_ventanas_estacionamiento_ord.iterrows()]
                exito_restriccion_estacionamiento, df_ventanas = restringir_rangos(df_ventanas_trabajadores,
                                                                                   lista_ordenada_restricciones)
                # Si la restriccion de rangos no es exitosa entonces pasamos automaticamente al siguiente estacionamiento
                if not exito_restriccion_estacionamiento:
                    ind_estacionamiento += 1
                    continue
                # Ahora hacemos las asignaciones para la combinacion atóncion/estacionamiento
                exito_asignacion_estacionamiento, asignaciones_estacionamiento = determina_asignaciones_atencion_est(req_trabajadores_servicio,
                                                                                                                    df_ventanas)
                # Si es exitoso con ese estacionamiento primero evaluamos si
                # es suficientemente buena como para parar la iteracion
                if exito_asignacion_estacionamiento:
                    finalizacion = max(asignaciones_estacionamiento['fechahora_fin_ventana'])
                    # maximo_factor_demora_cr_a_ideal es el maximo valor que puede tener la demora en un estacionamiento
                    # con respecto a la demora ideal como para que el estacionamiento sea considerado como suficientemente
                    # bueno para elegirlo no seguir buscando en otros
                    if (finalizacion-inicio_atencion_ideal) <= maximo_factor_demora*(fin_atencion_ideal-inicio_atencion_ideal):
                        estacionamiento_elegido = nombre_estacionamiento
                        asignaciones_atencion_final = asignaciones_estacionamiento
                        asignaciones_atencion_final['estacionamiento'] = estacionamiento_elegido
                        return True, asignaciones_atencion_final
                    # Si no es lo suficientemente bueno evaluamos si es la mejor asignacion hasta ahora y seguimos iterando
                    else:
                        if (finalizacion_mas_pronta is None) or (finalizacion<finalizacion_mas_pronta):
                            mejor_estacionamiento = nombre_estacionamiento
                            mejor_asignacion = asignaciones_estacionamiento
                            finalizacion_mas_pronta = finalizacion
                # Seguimos buscando si llegamos a este punto
                ind_estacionamiento += 1
            
            # Si una vez terminado el recorrido por los estacionamientos hay al menos una asignacion exitosa
            # entonces la asignacion total es exitosa
            if mejor_estacionamiento is not None:
                exito_asignacion_total = True
                estacionamiento_elegido = mejor_estacionamiento
                asignaciones_atencion_final = mejor_asignacion
    # Si la asignacion ideal no es exitosa entonces toda la asignacion se considera fallida
    else:                      
        return False, pd.DataFrame([])
    # Si hay asignacion agrego el estacionamiento
    if asignaciones_atencion_final is not None:
        asignaciones_atencion_final['estacionamiento'] = estacionamiento_elegido
    return exito_asignacion_total, asignaciones_atencion_final


def determina_asignaciones_atencion_est(req_trabajadores_servicio, df_ventanas):
    ids_tipo_trabajo = sorted(list(set(req_trabajadores_servicio['id_tipo_trabajo'].values)))
    
    #Defino un boolean de exito por defecto True que cambia a false si falla cualquier trabajo
    # En cuyo caso termina toda la ejecución de la función
    exito_atencion = True
    # Aquí defino el dataframe donde va a ir el resultado de las asignaciones de la atención
    df_asignaciones_atencion = pd.DataFrame([])

    # Para cada trabajo
    for ind_tipo_trabajo in range(len(ids_tipo_trabajo)):
        id_tipo_trabajo = ids_tipo_trabajo[ind_tipo_trabajo]

        # si empieza un trabajo que no es el primero entonces el trabajo 
        # tiene que empezar despues del fin del ultimo trabajo
        if ind_tipo_trabajo>=1:
            limite_inicio_trabajo = fin_ultimo_trabajo.to_pydatetime()
            # con el valor de abajo simulamos como que no hay limite superior
            limite_fin_trabajo = datetime.datetime(2200,12,31,12,0)
            # Restringimos las ventanas para que no traslapen los tiempos de los distintos trabajos
            exito_restriccion, df_ventanas = restringir_rangos(df_ventanas, [[limite_inicio_trabajo,limite_fin_trabajo]])
            # Si el rango de ventanas disponible es vacio entonces cortamos toda
            # la ejecución
            if exito_restriccion == False:
                exito_trabajo = False
                exito_atencion = False
                df_asignaciones_atencion = []
                break
                                                         
        requisitos_trabajo = req_trabajadores_servicio[req_trabajadores_servicio['id_tipo_trabajo']==id_tipo_trabajo]
        # Si ya hay trabajadores asignados para trabajos previos los definimos como prioritarios
        # para reforzar que los mismos trabajadores hagan una sola atencion
        prioritarios = []
        if len(df_asignaciones_atencion)>0:
            prioritarios = sorted(list(set(df_asignaciones_atencion['id_trabajador'])))

        exito_trabajo, df_asignaciones_trabajo = determina_asignaciones_trabajo_est(requisitos_trabajo, df_ventanas,
                                                                                    trabajadores_prioritarios = prioritarios)
        
        if exito_trabajo == True:
            # Primero agrego la informacion de la id del tipo de trabajo
            df_asignaciones_trabajo['id_tipo_trabajo'] = id_tipo_trabajo
            df_asignaciones_atencion = pd.concat([df_asignaciones_atencion,df_asignaciones_trabajo], ignore_index=True)
            fin_ultimo_trabajo = max(df_asignaciones_trabajo['fechahora_fin_ventana'])
        elif exito_trabajo == False:
            exito_atencion = False
            break
        
    return exito_atencion, df_asignaciones_atencion


def determina_asignaciones_trabajo_est(df_requisitos_trabajo, df_ventanas,
                                       trabajadores_prioritarios=[],verbose=False):

    # Primero ordeno los requisitos por carga descendente para que cada trabajador trabaje dentro
    # de la ventana de trabajo del trabajador asignado al requisito anterior
    requisitos_trabajo_ord = df_requisitos_trabajo.sort_values(by=['horas_hombre_requeridas'],kind="mergesort",
                                                               ascending=False).reset_index(drop=True)

    ind_requisito = 0
    estado_asignaciones = [pd.DataFrame([])] * len(df_requisitos_trabajo)
    restricciones_por_requisito = [pd.DataFrame([])] * len(df_requisitos_trabajo)
    exito_funcion = True
    while 0 <= ind_requisito < len(df_requisitos_trabajo):
        restricciones_requisito = restricciones_por_requisito[ind_requisito]
        requisito_actual = requisitos_trabajo_ord.iloc[ind_requisito]
        id_rol = requisito_actual['id_rol']
        horas_requeridas = requisito_actual['horas_hombre_requeridas']
        # Primero defino las ventanas asociadas a este rol
        df_ventanas_rol = df_ventanas[df_ventanas['id_rol']==id_rol]
        df_ventanas_rol = df_ventanas_rol.reset_index(drop=True) # Con indices ordenados para no tener problemas
        # definimos por defecto como True para que quede así en caso que no haya que restringir
        exito_restriccion = True
        # Luego si el requisito no es el primero hay que restringir la ventana a las asignaciones de trabajo
        # del trabajador del requisito anterior
        if ind_requisito>=1:
            lista_asignaciones_previa = [[row['fechahora_inicio_ventana'],row['fechahora_fin_ventana']]
                                        for _,row in estado_asignaciones[ind_requisito-1].iterrows()]
            exito_restriccion, df_ventanas_rol = restringir_rangos(df_ventanas_rol,lista_asignaciones_previa)
            if not exito_restriccion:
                exito_asignacion = False

        # Si hay efectivamente aun un df_ventanas_rol no vacio lo filtramos
        if exito_restriccion:    
            # Ahora filtramos por los rangos restringidos

            df_ventanas_rol_filtrado = excluye_asignaciones_de_ventanas(df_ventanas_rol, restricciones_requisito, 'trabajadores')
        # Buscar asignación válida compatible con ventanas_permitidas        
        exito_asignacion, asignacion = determina_asignaciones_requisito(df_ventanas_rol_filtrado,
                                                                        horas_requeridas,
                                                                        trabajadores_prioritarios=trabajadores_prioritarios)
        
        if verbose==True:
            print()
            print()
            print(f'Buscando asignar {horas_requeridas} horas para el rol {int(id_rol)}')
            print('El siguiente es el dataframe de las ventanas del rol filtrado por asignaciones anteriores')
            print(df_ventanas_rol_filtrado.sort_values('dt_inicio',kind="mergesort")[:20])
            print()
            print('Y lo siguiente es la asignacion')
            print(asignacion)

        if exito_asignacion:
            # Primero agrego la id del rol al resultado de la asignacion
            asignacion['id_rol'] = id_rol
            # Aquí lo fuerzo a que sea entero
            asignacion['id_rol'] = asignacion['id_rol'].astype('int')
            # asignacion indica la id del trabajador del rol y los datatimes de la ventana
            estado_asignaciones[ind_requisito] = asignacion
            ind_requisito += 1  # avanzar

        # Si no fue exitosa la asignacion del requisito
        else:
            # Si es el primer requisito entonces falla toda la asignacion para este trabajo
            if ind_requisito == 0:
                logging.info("No hay solución posible")
                exito_funcion = False
                break

            # Si no es el primer requisito retrocedemos anulamos la asignacion para el requisito
            # anterior e imponemos la restriccion de esa asignacion para no volver a intentar ese camino fallido
            ind_requisito -= 1
            asignacion_fallida = estado_asignaciones[ind_requisito]
            restricciones_por_requisito[ind_requisito] = pd.concat([restricciones_por_requisito[ind_requisito],
                                                                                                asignacion_fallida])
            estado_asignaciones[ind_requisito] = pd.DataFrame([])

            # Resetear restricciones posteriores
            for j in range(ind_requisito + 1, len(df_requisitos_trabajo)):
                restricciones_por_requisito[j] = pd.DataFrame([])

    df_asignaciones = pd.concat(estado_asignaciones,ignore_index=True)
    return exito_funcion, df_asignaciones



def determina_asignaciones_requisito(df_ventanas, horas_requisito, trabajadores_prioritarios=[]):
    # Buscar el primer trabajador que pueda cumplir las horas asociadas al requisito laboral
    # usando el dataframe que lista todas las ventanas de disponibilidad de los trabajadores
    # de ese rol en el periodo en que se necesita el requisito del trabajo
    # Devuelve un boolean indicando exito o fracazo y las asignaciones del trabajador si se encontraron

    # Si no hay df_ventanas que revisar devolvemos inmediatamente fallo
    if len(df_ventanas)==0:
        return False, []
    # Lo primero que hacermos es ordenar las ventanas por prioridad considerando el momento
    # de inicio, luego el largo de las ventanas y despues la prioridad de los trabajadores
    df_ventanas['prioridad_trabajador'] = 1  # valor por defecto
    df_ventanas.loc[df_ventanas['id_trabajador'].isin(trabajadores_prioritarios), 'prioridad_trabajador'] = 0
    df_ventanas_ord = df_ventanas.sort_values(by=["dt_inicio","horas_ventana","prioridad_trabajador"],kind="mergesort",
                                              ascending=[True,False,True]).reset_index(drop=True)
    df_ventanas_ord = df_ventanas_ord.drop(columns=['prioridad_trabajador'])
    
    # Guardamos la información de la primera ventana porque si el candidato cumple el requisito
    # empezando al mismo tiempo que ella entonces la solución es perfecta y hay que dejar de buscar
    dia_inicial_global, hora_inicial_global = df_ventanas_ord.iloc[0][['dia','hora_inicio']]
    
    acumulado = {} # aquí van las horas acumuladas por trabajador
    candidato = None # para la id del trabajador candidato a cumplir el requisito
    ultimo_dia_candidato = None # Para guardar el dia en que el candidato cumple el requisito
    ti = None # Para guardar el tiempo en que inicia la ultima ventana necesaria por el candidato a elegido
    tr = None # Para guardar el tiempo en que el candidato completa el requisito
    elegido = None # Para guardar el trabajador confirmado que cumple el requisito

    # hago una copia del dataframe para ir recorriendolo sin problemas mientras el original lo voy
    # modificando eliminando las filas recorridas porque tengo que usar las filas restantes tambien
    df_ventanas_original = df_ventanas_ord.copy()
    for idx, row in df_ventanas_original.iterrows():
        tid = row["id_trabajador"]
        dia = row["dia"]
        hi = row["hora_inicio"]
        horas_ventana = row["horas_ventana"]
        df_ventanas_ord = df_ventanas_ord.drop(index=idx)

        if tid not in acumulado:
            acumulado[tid] = 0
        horas_faltantes = horas_requisito - acumulado[tid]
        acumulado[tid] += horas_ventana

        # Si el trabajador con esta ventana llega a cumplir el requisito
        if horas_ventana >= horas_faltantes:
            candidato = tid
            ultimo_dia_candidato, ti = dia, hi
            tr = ti + horas_faltantes
            break
    
    # Si no hay candidato la asignacion es fallida
    if candidato is None:
        return False, []

    # Si hay candidato lo primero que hacemos es verificar si la ventana con la que cumple el requisito
    # empieza en el minimo de tiempo de inicio de las ventanas en cuyo caso el candidato es confirmado
    if (ultimo_dia_candidato == dia_inicial_global) and (ti==hora_inicial_global):
        elegido = candidato
    
    # Si existe candidato (si no ya habriamos retornado) pero no está confirmado aún
    # buscamos otros trabajadores que puedan terminar antes que el candidato
    if elegido is None:
        # partimos viendo que trabajadores diferentes al candidato tienen ventanas no contabilizadas 
        # que puedan hacerlo terminar antes que el candidato. Para eso tiene que necesariamente empezar antes
        # de tr porque sino termina mas tarde que el candidato (las que empiezan antes de ti ya fueron contabilizadas).
        # No importa cuando termine la ventana porque eso no determina cuando termina el requisito
        traslape_ultima_ventana = df_ventanas_ord[(df_ventanas_ord['dia']==ultimo_dia_candidato)
                                              & (df_ventanas_ord['hora_inicio']<tr)
                                              & (df_ventanas_ord['id_trabajador']!=candidato)]
        trabajadores_traslapantes = sorted(list(set(traslape_ultima_ventana['id_trabajador'])))
        # ahora tengo que ver para ellos las horas disponibles antes del momento de
        # cumplimiento del requisito para el candidato, y contar si son suficientes para terminar antes que el
        # Y para ellos calcular cuando cumple los requisitos cada
        # uno para después determinar al elegido como al que lo cumple antes.
        datos_trabajadores_exitosos = []
        for id_trabajador in trabajadores_traslapantes:
            # Como voy a usar acumulado para este trabajador me aseguro que tenga un valor en el diccionario
            # lo cual no es necesariamente el caso porque pueden aun no haberse recorrido ventanas de él
            if id_trabajador not in acumulado:
                acumulado[id_trabajador]=0
            df_trabajador = (traslape_ultima_ventana[traslape_ultima_ventana["id_trabajador"] == id_trabajador]
                             .sort_values(by=["dt_inicio"],kind="mergesort"))
            # Si no hay las horas suficientes en toda las ventanas traslapantes para el trabajador
            # se descarta como exitoso y se pasa al siguiente
            if sum(df_trabajador['horas_ventana'])<(horas_requisito-acumulado[id_trabajador]):
                continue
            # Para trabajadores que si tienen las horas necesarias calculo cuando terminan el requisito
            # y guardo los que terminan antes que el candidato
            else:
                # Recorremos las ventanas hasta cumplir el requisito
                for _,fila in df_trabajador.iterrows():
                    # Horas faltantes antes de contar esta ventana
                    horas_faltantes = horas_requisito-acumulado[id_trabajador]
                    # Luego actualizamos acumulado para que cuente esta ventana
                    acumulado[id_trabajador] += fila['horas_ventana']
                    # Si con esta ventana se cumple el requisito antes que el candidato, anotamos la id del trabajador
                    # y el momento de cumplimiento del requisito y pasamos al siguiente trabajador
                    if fila['horas_ventana']>=horas_faltantes:
                        hora_cumplimiento = fila['hora_inicio'] + horas_faltantes
                        if hora_cumplimiento<tr:
                            datos_trabajadores_exitosos.append({
                                'id_trabajador': id_trabajador,
                                'dia_cumplimiento': fila['dia'],
                                'hora_cumplimiento': hora_cumplimiento
                            })
                        continue
        # Ahora que recorrimos todos los trabajadores con ventanas traslapantes con la ultima del candidato
        # vemos si existen quienes terminen antes que el candidato en cuyo caso elegimos al mas rapido
        if len(datos_trabajadores_exitosos)==0:
            elegido = candidato
        else:
            df_trabajadores_exitosos = pd.DataFrame(datos_trabajadores_exitosos)
            elegido = (df_trabajadores_exitosos.sort_values(by=['dia_cumplimiento','hora_cumplimiento'],kind="mergesort").iloc[0])['id_trabajador']
        
    
    # A esta altura ya tenemos el trabajador elegido entonces solo falta guardar las asignaciones
    df_final = (df_ventanas_original[df_ventanas_original["id_trabajador"] == elegido]
                .sort_values(by=["dia", "hora_inicio"],kind="mergesort"))
    horas_restantes = horas_requisito
    asignaciones = []
    ind_final = 0
    while (ind_final < len(df_final)) and (horas_restantes>0) :
        row = df_final.iloc[ind_final]
        dia = row["dia"]
        hi = row["hora_inicio"]
        duracion = min([row["horas_ventana"], horas_restantes])
        ti = row["dt_inicio"]
        tf = ti + datetime.timedelta(hours = duracion)
        horas_restantes -= duracion
        ind_final += 1
        asignaciones.append({
            'id_trabajador': elegido,
            'fechahora_inicio_ventana': ti,
            'fechahora_fin_ventana': tf,
            'horas_hombre_asignadas': duracion
        })
    # Finalmente devuelvo True indicando asignacion exitosa y las asignaciones
    df_asignaciones = pd.DataFrame(asignaciones)
    return True, df_asignaciones


debug_df_ventanas_total = None
debug_df_ventanas_con_periodo_contrato = None
debug_df_ventanas_filtrado = None

print('Paso 4', flush=True)

def calcula_ventanas_trabajadores(Asignaciones, Trabajadores, df_disponibilidad_trabajadores,
                                  ids_roles_involucrados, fechahora_minima, fechahora_maxima,
                                  df_trabajadores, solo_fijos=True):

    # Filtramos primero las disponibilidades semanales por las ids involucradas
    df_disponibilidad_roles = df_disponibilidad_trabajadores[df_disponibilidad_trabajadores['id_rol']
                                                            .isin(ids_roles_involucrados)]
    if solo_fijos==True:
        df_disponibilidad_roles = df_disponibilidad_roles[df_disponibilidad_roles['modalidad_contrato']=='fijo']


    intervalos_dia = obtener_intervalos_dia(fechahora_minima,fechahora_maxima)
    n_intervalos = len(intervalos_dia)

    # Si no hay intervalos devuelvo vacío
    if n_intervalos==0:
        df_ventanas_total = pd.DataFrame([])

    # Si hay un solo intervalo busco para el y termino
    elif n_intervalos==1:
        intervalo = intervalos_dia.iloc[0]
        dia = intervalo['dia']
        dias, dia_semana = [dia], intervalo['dia_semana']
        feriado = es_feriado(dia)
        hora_inicio, hora_fin = intervalo['hora_inicio'], intervalo['hora_fin']
        df_ventanas_tipo_dia = ventanas_trabajadores_por_tipo_dia(df_disponibilidad_roles, dias,
                                                                dia_semana,feriado, hora_inicio, hora_fin)
        df_ventanas_total = df_ventanas_tipo_dia
    # Si hay mas de uno...
    else:
        df_ventanas_total = pd.DataFrame([])
        primera_hora_inicio = intervalos_dia.iloc[0]['hora_inicio']
        ultima_hora_fin = intervalos_dia.iloc[-1]['hora_fin']

        # Si el primero tiene hora de inicio distinta de cero lo busco de forma independiente y lo remuevo
        if primera_hora_inicio != 0:
            intervalo = intervalos_dia.iloc[0]
            dia = intervalo['dia']
            feriado = es_feriado(dia)
            dias, dia_semana= [dia], intervalo['dia_semana']
            df_ventanas_tipo_dia = ventanas_trabajadores_por_tipo_dia(df_disponibilidad_roles, dias,
                                                                    dia_semana,feriado,
                                                                    primera_hora_inicio, 24)
            df_ventanas_total = pd.concat([df_ventanas_total, df_ventanas_tipo_dia])
            intervalos_dia = intervalos_dia.drop(index=0)

        # Si el ultimo tiene hora de termino distinta de 24 lo busco de forma independiente y lo remuevo
        if ultima_hora_fin != 24:
            intervalo = intervalos_dia.iloc[-1]
            dia = intervalo['dia']
            feriado = es_feriado(dia)
            dias, dia_semana= [dia], intervalo['dia_semana']
            df_ventanas_tipo_dia = ventanas_trabajadores_por_tipo_dia(df_disponibilidad_roles, dias,
                                                                    dia_semana,feriado,
                                                                    0, ultima_hora_fin)
            df_ventanas_total = pd.concat([df_ventanas_total, df_ventanas_tipo_dia])
            ultimo_indice = intervalos_dia.index[-1]
            intervalos_dia = intervalos_dia.drop(index=ultimo_indice)
        
        # Si aun quedan filas ahora las busco por grupo
        if len(intervalos_dia)>0:
            dias_semana = sorted(list(set(intervalos_dia['dia_semana'])))
            for dia_semana in dias_semana:
                dias = intervalos_dia[intervalos_dia['dia_semana']==dia_semana]['dia']
                lista_dias = sorted(dias.to_list())
                # Separo los dias entre habiles y feriados
                dias_feriados, indices_feriados = [], []
                dias_habiles, indices_habiles = [], []
                for ind_dia, dia in enumerate(lista_dias):
                    if es_feriado(dia):
                        dias_feriados.append(dia)
                        indices_feriados.append(ind_dia)
                    else:
                        dias_habiles.append(dia)
                        indices_habiles.append(ind_dia)

                indices_df_dias = list(dias.index)
                indices_df_feriados = list(np.array(indices_df_dias)[indices_feriados])
                indices_df_habiles = list(np.array(indices_df_dias)[indices_habiles])

                if len(dias_habiles)>0:
                    df_ventanas_tipo_dia = ventanas_trabajadores_por_tipo_dia(df_disponibilidad_roles, dias_habiles,
                                                                            dia_semana,False, 0, 24)
                    df_ventanas_total = pd.concat([df_ventanas_total, df_ventanas_tipo_dia])
                    intervalos_dia = intervalos_dia.drop(index=indices_df_habiles)
        
                if len(dias_feriados)>0:
                    df_ventanas_tipo_dia = ventanas_trabajadores_por_tipo_dia(df_disponibilidad_roles, dias_feriados,
                                                                            dia_semana,True, 0, 24)
                    df_ventanas_total = pd.concat([df_ventanas_total, df_ventanas_tipo_dia])
                    intervalos_dia = intervalos_dia.drop(index=indices_df_feriados)

        # Ahora ordenamos los indices para entregar un resultado con indices correlativos y unicos
        df_ventanas_total.reset_index(inplace=True,drop=True)

    # Ahora restringimos las ventanas de los trabajadores para solo considerar aquellos dias que cada
    # trabajador efectivamente fue parte de la empresa
    df_ventanas_con_periodo_contrato = df_ventanas_total.merge(df_trabajadores[['id', 'iniciacion', 'termino']],
                                                               left_on='id_trabajador',right_on='id',how='left')

    # Filtro directo sin conversión
    df_ventanas_filtrado = df_ventanas_con_periodo_contrato[
        (df_ventanas_con_periodo_contrato['dia'] >= df_ventanas_con_periodo_contrato['iniciacion']) &
        (
            df_ventanas_con_periodo_contrato['termino'].isna() |
            (df_ventanas_con_periodo_contrato['dia'] <= df_ventanas_con_periodo_contrato['termino'])
        )
    ].drop(columns=['id','iniciacion','termino'])

    # Ahora que tenemos el total de ventanas agregamos las columnas datetime dt_inicio y dt_fin
    # para hacer la comparacion más fácil con asignaciones y df_ventanas_estacionamientos
    # definidas con datetime como la de asignaciones
    global debug_df_ventanas_total
    global debug_df_ventanas_con_periodo_contrato
    global debug_df_ventanas_filtrado
    debug_df_ventanas_total = df_ventanas_total.copy()  # Copiamos por si se modifica internamente
    debug_df_ventanas_con_periodo_contrato = df_ventanas_con_periodo_contrato.copy()
    debug_df_ventanas_filtrado = df_ventanas_filtrado.copy()

    # Si ventanas_filtrado queda vacio entonces simplemente lo formateo y devuelvo ese dataframe
    if len(df_ventanas_filtrado)==0:
        df_ventanas_filtrado['dt_inicio'] = pd.NaT
        df_ventanas_filtrado['dt_fin'] = pd.NaT
        return df_ventanas_filtrado
    
    df_ventanas_filtrado['dt_inicio'] = (df_ventanas_filtrado
                                         .apply(lambda fila:
                                                combina_dia_y_float_hora_en_dt(fila['dia'], fila['hora_inicio']), axis=1))
    df_ventanas_filtrado['dt_fin'] = (df_ventanas_filtrado
                                      .apply(lambda fila:
                                             combina_dia_y_float_hora_en_dt(fila['dia'], fila['hora_fin']), axis=1))

    query_asignaciones = (Asignaciones
                        .select(Asignaciones.id_trabajador, Asignaciones.fechahora_inicio_ventana,
                                Asignaciones.fechahora_fin_ventana, Asignaciones.anuladas)
                        .where((Asignaciones.fechahora_fin_ventana>fechahora_minima) & 
                                (Asignaciones.fechahora_inicio_ventana<fechahora_maxima) &
                                (Trabajadores.id_rol.in_(ids_roles_involucrados)) )
                        .join(Trabajadores, on=(Asignaciones.id_trabajador==Trabajadores.id))
                        .order_by(Asignaciones.id))
    df_asignaciones = pd.DataFrame(list(query_asignaciones.dicts()))
    # Acá elimino de las disponibilidades de los trabajadores sus asignaciones
    df_ventanas_restringido = excluye_asignaciones_de_ventanas(df_ventanas_filtrado, df_asignaciones, 'trabajadores')

    return df_ventanas_restringido


def ventanas_trabajadores_por_tipo_dia(df_disponibilidad_roles, lista_dias, dia_semana,
                                       feriado, hora_minima, hora_maxima):

    disponibilidades_dia = df_disponibilidad_roles[(df_disponibilidad_roles['dia_semana']==dia_semana)
                                                & (df_disponibilidad_roles['feriado']==feriado)]
    datos_disponibilidades = []
    for _, row in disponibilidades_dia.iterrows():
        (id_trabajador, dia_semana, hora_inicio_trabajador, hora_fin_trabajador,
        id_rol) = row['id_trabajador'], row['dia_semana'], row['hora_inicio'], row['hora_fin'], row['id_rol']
        hora_inicio = max([hora_minima, hora_inicio_trabajador])
        hora_fin = min([hora_maxima, hora_fin_trabajador])
        if hora_inicio < hora_fin:
            # Si el rango es válido agrego una entrada por cada día
            for dia in lista_dias:
                datos_disponibilidades.append({
                    'id_trabajador': id_trabajador,
                    'id_rol': id_rol,
                    'dia': dia,
                    'dia_semana': dia_semana,
                    'hora_inicio': hora_inicio,
                    'hora_fin': hora_fin,
                    'horas_ventana': hora_fin-hora_inicio,
                })
    df_disponibilidades = pd.DataFrame(datos_disponibilidades)
    return df_disponibilidades

def calcula_compras_y_consumos_servicio(df_insumos_servicio, df_consumos_servicio,asignaciones_servicio,
                                        req_materiales, id_insumo_actual, n_maquinas, fechahora_solicitud):
    df_insumos_a_usar = df_insumos_servicio.copy()
    df_consumos_a_usar = df_consumos_servicio.copy()
    df_comprar_servicio = pd.DataFrame([])
    df_consumir_servicio = pd.DataFrame([])

    for n_maquina in range(1,n_maquinas+1):
        asignaciones_atencion = asignaciones_servicio[asignaciones_servicio['n_maquina']==n_maquina]
        inicio_trabajos = min(asignaciones_atencion['fechahora_inicio_ventana']).to_pydatetime()
        fin_trabajos = max(asignaciones_atencion['fechahora_fin_ventana']).to_pydatetime()
        (df_comprar_atencion, df_consumir_atencion
         ) = calcula_compras_y_consumos_atencion(df_insumos_a_usar, df_consumos_a_usar,inicio_trabajos,
                                                 fin_trabajos, req_materiales, id_insumo_actual, fechahora_solicitud)
        
        # Ahora apendizamos el resultado al df a usar excluyendo el numero de la maquina
        if len(df_comprar_atencion)>0:
            df_insumos_a_usar = pd.concat([df_insumos_a_usar, df_comprar_atencion.iloc[:,:-1]],ignore_index=True)
        if len(df_consumir_atencion)>0:
            df_consumos_a_usar = pd.concat([df_consumos_a_usar, df_consumir_atencion.iloc[:,:-1]],ignore_index=True)
        # Luego actualizamos la id actual de la tabla insumos
        id_insumo_actual += len(df_comprar_atencion)
        # Y finalmente anexamos los resultados incluyendo el numero de la maquina al output
        df_consumir_atencion['n_maquina']=n_maquina
        df_comprar_atencion['n_maquina']=n_maquina
        if len(df_comprar_atencion)>0:
            df_comprar_servicio = pd.concat([df_comprar_servicio,df_comprar_atencion],ignore_index=True)
        if len(df_consumir_atencion)>0:
            df_consumir_servicio = pd.concat([df_consumir_servicio,df_consumir_atencion],ignore_index=True)

    return df_comprar_servicio, df_consumir_servicio


    
def calcula_compras_y_consumos_atencion(df_insumos, df_consumos,inicio_trabajos,
                                        fin_trabajos, req_materiales, id_insumo_actual, fechahora_solicitud):
    """ Calcula las compras y consumos necesarios para una atención en función de los requerimientos materiales
    los insumos que existen, los consumos y el periodo de trabajo
    """

    tipos_insumo = sorted(list(set(req_materiales['id_tipo_insumo'])))
    df_insumos_filtrado, df_consumos_filtrado = filtra_dfs_insumo_consumo(tipos_insumo, df_insumos,df_consumos,
                                                                          inicio_trabajos,fin_trabajos,'atencion')
    # Para el calculo de disponibilidad solo se cuentan los insumos que ya han llegado (porque los otros no se si me sirven)
    # pero se cuentan todos los consumos porque ya estan reservados para otros servicios
    # Esto significa que no pueden existir en las tablas consumos de insumos que no han llegado
    # porque eso estropea la cuenta

    datos_comprar = []
    datos_consumir = []

    ids_tipo_trabajo = sorted(list(set(req_materiales['id_tipo_trabajo'])))

    for id_tipo_trabajo in ids_tipo_trabajo:
        req_materiales_trabajo = req_materiales[req_materiales['id_tipo_trabajo']==id_tipo_trabajo]
        for _,row_material in req_materiales_trabajo.iterrows():
            _, id_tipo_insumo, _, cantidad_requerida, porcentaje_requerido, cantidad_ponderada, reutilizable, seguimiento_automatizado = row_material
            # Primero calculo la cantidad total de insumos
            (n_disponibles, porcentaje_disponibilidad, disponibilidad_por_id
             ) = calcula_disponibilidad_insumo_y_por_id_detallado(df_insumos_filtrado, df_consumos_filtrado,
                                                                  id_tipo_insumo, reutilizable,
                                                                  seguimiento_automatizado)


            # Ahora que tengo las disponibilidades calculo cuanto tengo que comprar y cuanto tengo que consumir de lo existente
            n_a_comprar = int(max([0, np.ceil((cantidad_ponderada-n_disponibles*porcentaje_disponibilidad/100))]))

            # Por cada material a comprar agendamos la compra en datos_comprar y actualizamos df_insumos_filtrado
            # para considerarlo disponible para el proximo trabajo
            ids_insumos_nuevos = []
            # guardo las ids de los insumos nuevos para despues ponerles disponibilidad 100% en df_nuevos_disponibles
            if n_a_comprar > 0:
                fila_df_insumos = {'id': id_insumo_actual,
                                   'id_tipo_insumo': id_tipo_insumo,
                                   'cantidad': n_a_comprar,
                                   'fechahora_adquisicion_actualizacion': fechahora_inicio_empresa}
                ids_insumos_nuevos.append(id_insumo_actual)
                if len(df_insumos_filtrado)==0:
                    df_insumos_filtrado = pd.DataFrame([fila_df_insumos])
                else:
                    indice_df = df_insumos_filtrado.index.max() + 1
                    df_insumos_filtrado.loc[indice_df] = fila_df_insumos
                fecha_real_llegada_insumo = calcula_tiempo_entrega(TiposInsumo, [id_tipo_insumo], fechahora_solicitud)
                datos_comprar.append([id_insumo_actual, id_tipo_insumo,n_a_comprar, fecha_real_llegada_insumo, id_tipo_trabajo])
                # Agregamos la nueva fila correspondiente al nuevo insumo que va a estar disponible para los otros trabajos
                id_insumo_actual += 1
            #
            #  
            # Ahora que tenemos df_insumos_filtrado actualizado por las compras necesarias podemos
            # calcular los consumos correspondientes a este material para este trabajo
            #
            #Pero esto solo se hace para insumos con seguimiento automatizado porque para los otros es muy dificil
            #rastrear el consumo real entonces en ese caso simplemente vamos al siguiente insumo
            if not seguimiento_automatizado:
                continue
            #
            # Primero determinamos las filas del material actual que incluye las nuevas compras y que por construccion
            # tiene todo lo necesario para cumplir el requerimiento y le agregamos la informacion de que tan disponible esta cada insumo
            # Este df incluye las compras necesarias entonces nunca deberia estar vacio por eso puedo hacer seleccion
            df_nuevo_disponible = df_insumos_filtrado[df_insumos_filtrado['id_tipo_insumo']==id_tipo_insumo].sort_values('id',kind="mergesort")
            # Parto agregando una columna para guardar el porcentaje de disponibilidad de cada insumo
            # que por defecto será 100 y solo cambiará para insumos viejos reutilizables
            df_nuevo_disponible['porcentaje_de_disponibilidad'] = 100.0
            # Como cantidad ahora es la cantidad disponible necesito floats en esa columna
            df_nuevo_disponible['cantidad'] = df_nuevo_disponible['cantidad'].astype(float)

            # Ahora actualizo las disponibilidades de cada id de insumo viejo
            ids_insumos_antiguos = sorted(list(disponibilidad_por_id.keys()))
            for id_insumo in ids_insumos_antiguos:
                disponibilidad_numero,disponibilidad_porc = disponibilidad_por_id[id_insumo]
                df_nuevo_disponible.loc[df_nuevo_disponible[df_nuevo_disponible['id']==id_insumo].index,'porcentaje_de_disponibilidad']=disponibilidad_porc
                df_nuevo_disponible.loc[df_nuevo_disponible[df_nuevo_disponible['id']==id_insumo].index,'cantidad']=disponibilidad_numero
            # Ahora filtro el df con los insumos del tipo correspondiente para que solo tenga insumos realmente disponibles
            df_nuevo_disponible = df_nuevo_disponible[(df_nuevo_disponible['cantidad']>1e-9) &
                                                    (df_nuevo_disponible['porcentaje_de_disponibilidad']>1e-9)]
            df_nuevo_disponible = df_nuevo_disponible.reset_index(drop=True)
            # Esto es la cantidad real de uso requerido que antes del for es todo faltante por cumplir
            cantidad_ponderada_faltante = cantidad_ponderada
            iloc_df = 0
            while cantidad_ponderada_faltante>1e-9:
                fila_df = df_nuevo_disponible.iloc[iloc_df]
                id_insumo, _ , n_insumos, _, porc_disponibilidad = fila_df
                cantidad_disponible = n_insumos * porc_disponibilidad/100
                
                # Si es desechable el porcentaje de uso es 100 y la cantidad a usar
                # el minimo entre lo disponible y lo necesario
                if not reutilizable:
                    cantidad_a_usar = min([cantidad_requerida, n_insumos])
                    porcentaje_a_usar = 100
                else:
                    # Si es reutilizable uso siempre todos los que hay
                    cantidad_a_usar = n_insumos
                    # Si el insumo no alcanza para cumplir el requisito
                    # se usa el maximo porcentaje posible (lo que queda disponible)
                    if cantidad_ponderada_faltante>cantidad_disponible:
                        porcentaje_a_usar = porc_disponibilidad
                    # Si el insumo alcanza para cumplir uso lo que me falta para cumplir el requerimiento
                    else:
                        porcentaje_a_usar = 100*cantidad_ponderada_faltante/cantidad_a_usar
                # Aqui guardamos la info del consumo
                datos_consumir.append([id_tipo_insumo, cantidad_a_usar, round(porcentaje_a_usar,9),
                                       round(cantidad_a_usar*porcentaje_a_usar/100,9), inicio_trabajos,
                                       fin_trabajos, id_insumo, reutilizable, id_tipo_trabajo])
                fila_a_agregar = {'id_tipo_insumo': id_tipo_insumo,
                                 'cantidad': cantidad_a_usar,
                                 'porcentaje_uso': round(porcentaje_a_usar,9),
                                 'uso_ponderado': round(cantidad_a_usar*porcentaje_a_usar/100,9),
                                 'fechahora_inicio_uso': inicio_trabajos,
                                 'fechahora_fin_uso': fin_trabajos,
                                 'id_insumo_si_aplica': id_insumo,
                                 'insumo_reutilizable': reutilizable}
                
                if len(df_consumos_filtrado)==0:
                    df_consumos_filtrado = pd.DataFrame([fila_a_agregar])
                else:
                    indice_df = df_consumos_filtrado.index.max() + 1            
                    # Agregamos la nueva fila correspondiente al nuevo consumo para que se considere por otros trabajos de la atencion
                    df_consumos_filtrado.loc[indice_df] = fila_a_agregar
                #Aqui actualizamos la info si hay que seguir buscando insumos
                cantidad_ponderada_faltante -= cantidad_a_usar*porcentaje_a_usar/100
                # Si es reutilizable disminuimos el porcentaje requerido
                if reutilizable:
                    porcentaje_requerido = cantidad_ponderada_faltante*100/cantidad_requerida
                # Si es desechable disminuimos la cantidad requerida
                else:
                    cantidad_requerida -= cantidad_a_usar
                # Seguimos el loop si quedan insumos por completar
                iloc_df += 1

    df_comprar = pd.DataFrame(datos_comprar,
                              columns=['id','id_tipo_insumo','cantidad',
                                       'fechahora_adquisicion_actualizacion','id_tipo_trabajo'])
    df_consumir = pd.DataFrame(datos_consumir,
                               columns=['id_tipo_insumo', 'cantidad', 'porcentaje_uso',
                                         'uso_ponderado', 'fechahora_inicio_uso', 'fechahora_fin_uso',
                                         'id_insumo_si_aplica', 'insumo_reutilizable','id_tipo_trabajo'])
    return df_comprar, df_consumir


def calcula_minimo_tiempo_entrega(req_materiales,n_maquinas, df_insumos_completo,TiposInsumo, fechahora_partida):
    """ Calcula el momento mas pronto en que podrían llegar los insumos considerando
    como si no hubiese ningun consumo
    """
    tipos_insumo = sorted(list(set(req_materiales['id_tipo_insumo'])))
    df_insumos = df_insumos_completo[df_insumos_completo['id_tipo_insumo'].isin(tipos_insumo)]
    tiempos_entrega = []
    for tipo_insumo in tipos_insumo:
        rows_requerimiento = req_materiales[req_materiales['id_tipo_insumo']==tipo_insumo]
        # Para cada insumo calculo cuanto necesito para todo el servicio
        cantidad_ponderada_necesaria = sum(rows_requerimiento['cantidad_ponderada']) * n_maquinas
        # Luego calculo cuanto hay disponibles en función del tiempo
        rows_insumos = df_insumos[df_insumos['id_tipo_insumo']==tipo_insumo]
        insumos_totales = sum(rows_insumos['cantidad'])
        # Si en todo el tiempo no hay insumos necesarios entonces el tiempo de entrega es el de compra
        if insumos_totales < cantidad_ponderada_necesaria:
            tiempo_entrega = calcula_tiempo_entrega(TiposInsumo,[tipo_insumo],fechahora_partida)
        # Si la cantidad de insumos es suficiente calculamos en que momento hay suficientes insumos
        else:
            # Para eso ordenamos por fecha y vemos cuando se cumple el requisito
            rows_insumos = rows_insumos.sort_values(by='fechahora_adquisicion_actualizacion',kind="mergesort")
            rows_insumos['cantidad_acumulada'] = rows_insumos['cantidad'].cumsum()
            tiempo_disponibilidad = min(rows_insumos[rows_insumos['cantidad_acumulada'] >= cantidad_ponderada_necesaria]
                                        ['fechahora_adquisicion_actualizacion'])
            # Si ya estan disponibles al momento de partida entonces ese es el tiempo de entrega
            if tiempo_disponibilidad<= fechahora_partida:
                tiempo_entrega = fechahora_partida
            # Si se vuelven disponibles despues del momento de partida eligo el mas temprano entre el y la compra
            else:
                tiempo_compra = calcula_tiempo_entrega(TiposInsumo,[tipo_insumo],fechahora_partida)
                tiempo_entrega = min([tiempo_disponibilidad,tiempo_compra])
        tiempos_entrega.append(tiempo_entrega)
    mayor_tiempo_entrega = max(tiempos_entrega)
    return mayor_tiempo_entrega


def calcula_tiempo_entrega(TiposInsumo, tipos_insumo_a_comprar, fechahora_partida):
    """ Calcula el momento en que llegan todos los insumos requeridos"""
    
    # Si no hay insumos que comprar la fecha de llegada es igual a la de partida y terminamos
    if len(tipos_insumo_a_comprar)==0:
        return fechahora_partida
    
    query_tiempos_insumos = (TiposInsumo.select(TiposInsumo.dias_entrega_referencia,
                                                TiposInsumo.entrega_dias_inhabiles)
                            .where(TiposInsumo.id.in_(tipos_insumo_a_comprar))
                            .order_by(TiposInsumo.id))
    df_tiempos_insumos = pd.DataFrame(list(query_tiempos_insumos.dicts()))
    # Por defecto establecemos como que la demora es 0 en que lleguen los insumos
    # y aumentamos desde ahí
    fechahora_entrega = fechahora_partida
    fecha_entrega, hora_entrega = fechahora_partida.date(), fechahora_partida.time()

    tiempos_dias_habiles = df_tiempos_insumos[~df_tiempos_insumos['entrega_dias_inhabiles']]
    if len(tiempos_dias_habiles)>0:
        fecha_entrega = desplazar_dias_habiles(fecha_entrega, int(max(tiempos_dias_habiles['dias_entrega_referencia'])))
        fechahora_entrega = datetime.datetime.combine(fecha_entrega, hora_entrega)

    tiempos_dias_corridos = df_tiempos_insumos[df_tiempos_insumos['entrega_dias_inhabiles']]
    if len(tiempos_dias_corridos)>0:
        fechahora_entrega = max([fechahora_entrega, fechahora_partida
                                + datetime.timedelta(days=max(tiempos_dias_corridos['dias_entrega_referencia']))])
    return fechahora_entrega


def genera_registros_base(Clientes, TiposServicio, Proyectos, Servicios, Cotizaciones,db, config):
    """
    Genera y carga registros históricos iniciales en las tablas Proyectos, Servicios,
    Cotizaciones e Insumos. Simula la llegada de clientes, la solicitud de servicios y 
    la creación de cotizaciones, así como el ingreso de insumos de tipo estacionamiento.

    Parameters:
    - Modelos Peewee: Clientes, TiposServicio, TiposInsumo, Proyectos, Servicios, Cotizaciones, Insumos.
    - db: conexión activa a la base de datos (con .atomic()).
    - velocidad_llegada_clientes: medido como fracción del periodo total usado en distribución RVS (random variates sample)
    - año_inicio_empresa, mes_inicio_empresa, dia_inicio_empresa: fecha de inicio.
    - n_clientes_persona, n_clientes_empresa: número de clientes por tipo.
    - ancho_dist_maquinas_personas, ancho_dist_maquinas_empresas: parámetros de distribución de número de máquinas.
    - probabilidad_periodicidad_personas, probabilidad_periodicidad_empresas: probabilidades de recurrencia.
    - multiplicador_tiempo_planificacion: multiplicador para calcular fecha límite de planificación.
    - uf_por_año: diccionario con valores UF por año.
    - dict_estacionamientos: diccionario tipo_maquinaria → cantidad.

    Returns:
    - None. Inserta directamente en la base de datos.
    """

    # Parámetros generales
    uf_por_año = config['csvs']['precios']['uf_por_año']
    total_clientes = config['csvs']['clientes']['total_clientes']
    fraccion_clientes_empresas = config['csvs']['clientes']['fraccion_clientes_empresas']
    n_clientes_empresa = int(fraccion_clientes_empresas * total_clientes)
    n_clientes_persona = total_clientes - n_clientes_empresa

    # Parámetros de proyectos y servicios
    ancho_dist_maquinas_personas = config['instancias']['proyectos']['ancho_dist_maquinas_personas']
    ancho_dist_maquinas_empresas = config['instancias']['proyectos']['ancho_dist_maquinas_empresas']
    probabilidad_periodicidad_personas = config['instancias']['proyectos']['probabilidad_periodicidad_personas']
    probabilidad_periodicidad_empresas = config['instancias']['proyectos']['probabilidad_periodicidad_empresas']
    multiplicador_tiempo_planificacion = config['instancias']['servicios']['multiplicador_tiempo_planificacion']
    numero_de_dias = (fechahora_cierre - fechahora_inicio_empresa).days
    D = numero_de_dias         # duración total en días
    C = total_clientes         # número total de clientes  

    # Cargar parámetros desde config
    clientes_cfg = config['instancias']['clientes']
    x_peak_rel = clientes_cfg['xpeak_llegada_clientes']
    y_peak = clientes_cfg['ypeak_llegada_clientes']
    y_final = clientes_cfg['yfinal_llegada_clientes']
    semilla = config['instancias']['semilla']

    # Reproducibilidad
    rng_numpy = np.random.default_rng(semilla)

    # Escalamos x_peak al dominio total
    x_peak = x_peak_rel * D

    # Dominio temporal
    t = np.linspace(0, D, 1000)

    # Densidad: dos rectas a trozos
    densidad = np.piecewise(
        t,
        [t <= x_peak, t > x_peak],
        [
            lambda t: y_peak / x_peak * t,
            lambda t: ((y_final - y_peak) / (D - x_peak)) * (t - x_peak) + y_peak
        ]
    )

    # Normalizar densidad para que área sea igual a C
    area_original = integral_trapezoide(densidad, t)
    densidad *= C / area_original

    # Obtener CDF normalizada
    cdf = np.cumsum(densidad)
    cdf /= cdf[-1]

    # Samplear momentos de llegada (naturales, no forzados en extremos)
    momentos_llegada = np.interp(rng_numpy.uniform(0, 1, C), cdf, t)

    # Convertir a fechas
    fechas_llegada = [
        fechahora_inicio_empresa + relativedelta(days=float(n_dias))
        for n_dias in momentos_llegada
    ]

    query_tipos_servicio = TiposServicio.select().order_by(TiposServicio.id)
    df_tipos_servicio = pd.DataFrame(list(query_tipos_servicio.dicts()))

    arr_ids_tipos_servicio_personas = df_tipos_servicio[df_tipos_servicio['tipo_cliente']=='personas']['id'].values
    arr_ids_tipos_servicio_empresas = df_tipos_servicio[df_tipos_servicio['tipo_cliente']=='empresas']['id'].values

    ids_tipos_servicio_personas = rng_random.choices(arr_ids_tipos_servicio_personas, k=n_clientes_persona)
    ids_tipos_servicio_empresas = rng_random.choices(arr_ids_tipos_servicio_empresas, k=n_clientes_empresa)

    n_maquinas_personas = [int(np.ceil(rng_random.expovariate(1/ancho_dist_maquinas_personas)))
                        for _ in range(n_clientes_persona)]
    n_maquinas_empresas = [int(np.ceil(rng_random.expovariate(1/ancho_dist_maquinas_empresas)))
                        for _ in range(n_clientes_empresa)]
    disposicion_periodicidad_personas =  [rng_random.random()<probabilidad_periodicidad_personas
                                        for _ in range(n_clientes_persona)]
    disposicion_periodicidad_empresas =  [rng_random.random()<probabilidad_periodicidad_empresas
                                        for _ in range(n_clientes_persona)]

    ind_persona = 0
    ind_empresa = 0

    query_clientes = (Clientes
                      .select(Clientes.id,Clientes.nombre,Clientes.es_empresa,
                              Clientes.expectativa_pago,Clientes.expectativa_tiempo)
                      .order_by(Clientes.id))
    info_clientes = list(query_clientes.dicts())


    registros_proyectos = []
    registros_servicios = []
    registros_cotizaciones = []
    id_proyecto, id_servicio, id_cotizacion = 0, 0, 0

    for ind_cliente in range(len(info_clientes)):
        id_proyecto += 1
        info_cliente = info_clientes[ind_cliente]
        id_cliente = info_cliente['id']
        nombre_cliente = info_cliente['nombre']
        es_empresa = info_cliente['es_empresa']
        expectativa_pago = info_cliente['expectativa_pago']
        expectativa_tiempo = info_cliente['expectativa_tiempo']
        fecha_llegada = fechas_llegada[id_proyecto-1]

        if es_empresa:
            id_tipo_servicio = ids_tipos_servicio_empresas[ind_empresa]
            n_maquinas = n_maquinas_empresas[ind_empresa]
            disposicion_periodicidad = disposicion_periodicidad_empresas[ind_empresa]
            nombre_completo_cliente = 'empresa '+nombre_cliente
            # La demora del pago en dias se define a nivel de cliente porque tiene mas sentido
            # para la simulacion pero se guarda a nivel de servicio porque es mejor para la ejecución
            demora_pago_dias = rng_random.randint(1,3)*30
            ind_empresa += 1
        else:
            id_tipo_servicio = ids_tipos_servicio_personas[ind_persona]
            n_maquinas = n_maquinas_personas[ind_persona]
            disposicion_periodicidad = disposicion_periodicidad_personas[ind_persona]
            nombre_completo_cliente = 'particular '+nombre_cliente
            demora_pago_dias = 0
            ind_persona += 1
        query_tipos_servicio = (TiposServicio
                                .select(TiposServicio.nombre,
                                        TiposServicio.horas_trabajo_estimados,
                                        TiposServicio.dias_totales_entrega_insumos,
                                        TiposServicio.dias_habiles_entrega_insumos,
                                        TiposServicio.precio_uf_estimado,
                                        TiposServicio.periodicidad_tipica_meses,)
                                .where(TiposServicio.id==id_tipo_servicio)
                                .order_by(TiposServicio.id))
        info_tipo_servicio = query_tipos_servicio.dicts().get()
        if disposicion_periodicidad:
            periodicidad = info_tipo_servicio['periodicidad_tipica_meses']
        else:
            periodicidad = 0
        nombre_servicio = info_tipo_servicio['nombre']
        precio_uf_por_maquina = info_tipo_servicio['precio_uf_estimado']
        dias_totales_entrega_insumos = info_tipo_servicio['dias_totales_entrega_insumos']
        dias_habiles_entrega_insumos = info_tipo_servicio['dias_habiles_entrega_insumos']
        horas_trabajo_por_maquina = info_tipo_servicio['horas_trabajo_estimados']
        if dias_habiles_entrega_insumos is None:
            dias_habiles_totales_insumos = dias_totales_entrega_insumos*(5/7)
        elif dias_totales_entrega_insumos is None:
            dias_habiles_totales_insumos = dias_habiles_entrega_insumos
        else:
            dias_habiles_totales_insumos = np.max([dias_habiles_entrega_insumos,
                                                dias_totales_entrega_insumos*(5/7)])
        precio_uf_por_servicio_ofrecido = n_maquinas * precio_uf_por_maquina
        precio_uf_por_servicio_esperado = precio_uf_por_servicio_ofrecido * expectativa_pago
        dias_habiles_por_servicio_referencia = (n_maquinas * horas_trabajo_por_maquina / 8  
                                                + dias_habiles_totales_insumos)
        dias_habiles_por_servicio_esperado = dias_habiles_por_servicio_referencia * expectativa_tiempo
        fechas_solicitudes = [fecha_llegada]
        if periodicidad>0:
            nueva_fecha = fecha_llegada + relativedelta(months=periodicidad)
            while nueva_fecha < fechahora_cierre:
                fechas_solicitudes.append(nueva_fecha)
                nueva_fecha = nueva_fecha + relativedelta(months=periodicidad)
        # Ahora por cada servicio definimos los parametros que son propios 
        # de cada servicio y no comunes para todo el proyecto
        
        registros_proyectos.append({
            'id': id_proyecto,
            'id_cliente': id_cliente,
            'nombre': f'{nombre_servicio}, para {n_maquinas} maquinas para {nombre_completo_cliente}',
            'fecha_inicio': fecha_llegada,
        })

        for fecha_solicitud in fechas_solicitudes:
            id_servicio += 1
            id_cotizacion += 1
            fecha_esperada = desplazar_dias_habiles(fecha_solicitud, dias_habiles_por_servicio_esperado)
            fecha_limite_planificacion = desplazar_dias_habiles(fecha_solicitud, multiplicador_tiempo_planificacion*
                                                                dias_habiles_por_servicio_esperado)
            precio_servicio_ofrecido = precio_uf_por_servicio_ofrecido * uf_por_año[str(fecha_solicitud.year)]
            precio_servicio_esperado = precio_uf_por_servicio_esperado * uf_por_año[str(fecha_solicitud.year)]
            registros_servicios.append({
                'id': id_servicio,
                'id_proyecto': id_proyecto,
                'ids_tipo_servicio': id_tipo_servicio,
                'unidad_tipo_servicio': n_maquinas,
                'estado': 'planificado',
                'fecha_actualizacion_estado': fecha_solicitud,
                'fecha_solicitud': fecha_solicitud,
                'fecha_esperada': fecha_esperada,
                'fecha_propuesta': None,
                'fecha_limite_planificacion': fecha_limite_planificacion,
                'demora_pago_dias': demora_pago_dias
            })
            registros_cotizaciones.append({
                'id': id_cotizacion,
                'id_servicio': id_servicio,
                'fecha_cotizacion': fecha_solicitud,
                'descripcion': f'precio_esperado={int(precio_servicio_esperado)}',
                'total_estimado': precio_servicio_ofrecido,
                'nombre_archivo': f'Cotizacion{id_cotizacion:05}_Servicio{id_servicio:05}_Proyecto{id_proyecto:05}_Cliente{id_cliente:05}.pdf',
                'estado': 'no_enviada',
            })


    with db.atomic():
        print('Insertando Registros en Tabla proyectos')
        for batch in chunked(registros_proyectos, 100):
            Proyectos.insert_many(batch).execute()
        print('Insertando Registros en Tabla servicios')
        for batch in chunked(registros_servicios, 100):
            Servicios.insert_many(batch).execute()
        print('Insertando Registros en Tabla cotizaciones')
        for batch in chunked(registros_cotizaciones, 100):
            Cotizaciones.insert_many(batch).execute()

print('Paso 5', flush=True)

def crea_df_eventos(df_servicios, MovimientosRecurrentes, fechahora_inicio, fechahora_fin):
    """
    Crea un dataframe con todos los eventos (servicios + movimientos recurrentes) dentro del rango dado.
    Usa el modelo MovimientosRecurrentes directamente para generar los eventos recurrentes.
    """

    # 1. Filtrar servicios por rango
    df_eventos_servicios = df_servicios[
        (df_servicios['fecha_solicitud'] >= fechahora_inicio) & 
        (df_servicios['fecha_solicitud'] <= fechahora_fin)
    ][['id','fecha_solicitud','id_proyecto']].copy()

    df_eventos_servicios['tipo_evento'] = 'servicio'
    df_eventos_servicios.columns = ['id_en_tabla', 'fecha_evento','id_proyecto_servicios', 'tipo_evento']

    # 2. Generar eventos desde movimientos recurrentes
    lista_eventos = []
    for mov in MovimientosRecurrentes.select().order_by(MovimientosRecurrentes.id):
        # Extraer fechas y recurrencia
        # Le agregamos una hora random para que no haya ambigüedad al ordenar los eventos por fecha
        fechahora_inicio_mov = datetime.datetime.combine(mov.fecha_inicio, datetime.time(0,0)) + relativedelta(seconds=rng_random.random()*(3600*24-1))
        f_inicio = max(fechahora_inicio, fechahora_inicio_mov)
        if mov.fecha_fin is None:
            f_fin = fechahora_fin
        else:
            fechahora_fin_mov = datetime.datetime.combine(mov.fecha_fin+datetime.timedelta(days=1),datetime.time(0,0))
            f_fin = min(fechahora_fin, fechahora_fin_mov)

        # Determinar salto entre pagos
        unidad = mov.unidad_periodo  # 'días', 'semanas' 'meses', 'años'
        valor = mov.valor_periodo    # int
        delta = define_delta_tiempo(unidad, valor)

        # Generar fechas dentro del rango
        fecha = f_inicio
        while fecha <= f_fin:
            # la id_proyecto_servicios es -1 porque solo aplica para servicios
            lista_eventos.append({
                'id_en_tabla': mov.id,
                'fecha_evento': fecha,
                'tipo_evento': 'movimiento_recurrente',
                'id_proyecto_servicios': -1
            })
            fecha += delta

    df_eventos_movimientos_recurrentes = pd.DataFrame(lista_eventos)

    # 3. Unir y ordenar
    df_eventos = pd.concat([df_eventos_servicios, df_eventos_movimientos_recurrentes], ignore_index=True)
    df_eventos = df_eventos.sort_values('fecha_evento',kind="mergesort").reset_index(drop=True)

    return df_eventos


def calcula_saldo(MovimientosFinancieros, fechahora_saldo):
    query_movimientos_financieros = (MovimientosFinancieros
                                     .select(MovimientosFinancieros.fechahora_movimiento,
                                             MovimientosFinancieros.categoria,
                                             MovimientosFinancieros.monto)
                                     .where(MovimientosFinancieros.fechahora_movimiento<=fechahora_saldo)
                                     .order_by(MovimientosFinancieros.id))
    df_movimientos_financieros = pd.DataFrame(list(query_movimientos_financieros.dicts()))
    if len(df_movimientos_financieros)==0:
        saldo = 0
    else:
        ingresos = sum(df_movimientos_financieros[df_movimientos_financieros['categoria']=='ingreso']['monto'])
        egresos = sum(df_movimientos_financieros[df_movimientos_financieros['categoria']=='egreso']['monto'])
        saldo = ingresos - egresos
    return saldo

def escribe_finanzas_en_log(MovimientosFinancieros,Servicios,logobj, desgloza_por_año=False):
    """Escribe el resumen del balance financiero en el log"""

    tabla_servicios=pd.DataFrame(list(Servicios.select().dicts()))
    servicios_totales = len(tabla_servicios)
    maquinas_totales = sum(tabla_servicios['unidad_tipo_servicio'])

    logobj.info("")
    logobj.info('#'*30)
    logobj.info('El balance financiero es: ')

    query_movimientos = MovimientosFinancieros.select()
    df_movimientos = pd.DataFrame(list(query_movimientos.dicts()))
    df_inversion = df_movimientos[(df_movimientos['tipo']=='inyección de capital')]
    df_ingresos = df_movimientos[(df_movimientos['tipo']=='pago de servicio')]
    numero_servicios = len(df_ingresos)
    numero_maquinas = int(df_ingresos['descripcion'].str.extract(r'(\d+)\s+máquinas?').astype(float).sum().values[0])
    df_egresos = df_movimientos[df_movimientos['categoria']=='egreso']

    total_inversion = sum(df_inversion['monto'])
    total_ingresos = sum(df_ingresos['monto'])
    total_egresos = sum(df_egresos['monto'])
    total_ganancia = total_ingresos - total_egresos

    logobj.info(f'Inversion Total: ${total_inversion:,.0f}')
    logobj.info(f'Ingresos Total: ${total_ingresos:,.0f} ({numero_servicios} de {servicios_totales} '
                f'servicios con {numero_maquinas} de {maquinas_totales} máquinas)')
    logobj.info(f'Egresos Total: ${total_egresos:,.0f}')
    logobj.info(f'Ganancia Total: ${total_ganancia:,.0f}')
    logobj.info("")
    primer_año = fechahora_inicio_empresa.year
    ultimo_año = fechahora_cierre.year
    if desgloza_por_año is True:
        logobj.info('Desgloce por año')
        for año in range(primer_año,ultimo_año+1):
            df_inversion_año = df_inversion[df_inversion['numero_año_balance']==año]
            df_ingresos_año = df_ingresos[df_ingresos['numero_año_balance']==año]
            numero_servicios_año = len(df_ingresos_año)
            numero_maquinas_año = int(df_ingresos_año['descripcion'].str.extract(r'(\d+)\s+máquinas?').astype(float).sum().values[0])
            # Sumo a los registros los servicios que se solicitaron ese año y no se realizaron
            faltantes_año = tabla_servicios[(tabla_servicios['estado']!='finalizado') & (tabla_servicios['fecha_solicitud'].dt.year==año)]
            servicios_faltantes_año = len(faltantes_año)
            maquinas_faltantes_año = sum(faltantes_año['unidad_tipo_servicio'])
            servicios_totales_año = numero_servicios_año + servicios_faltantes_año
            maquinas_totales_año = numero_maquinas_año + maquinas_faltantes_año
            df_egresos_año = df_egresos[df_egresos['numero_año_balance']==año]
            inversion_año = sum(df_inversion_año['monto'])
            ingresos_año = sum(df_ingresos_año['monto'])
            egresos_año = sum(df_egresos_año['monto'])

            logobj.info("")
            logobj.info(f'Inversion {año}: ${inversion_año:,.0f}')
            logobj.info(f'Ingresos {año}: ${ingresos_año:,.0f} ({numero_servicios_año} de {servicios_totales_año} '
                        f'servicios con {numero_maquinas_año} de {maquinas_totales_año} máquinas)')
            logobj.info(f'Egresos {año}: ${egresos_año:,.0f}')
    logobj.info("")


print('Paso 6', flush=True)
class ClServicio:
    def __init__(self, row_servicio, TSATT, RequerimientosTrabajadores, Asignaciones, Trabajadores,
                 RequerimientosMateriales, TiposInsumo, Trabajos, TiposServicio, df_insumos, df_consumos,
                 solo_trabajadores_fijos=True):
        
        # Atributos asignados desde el principio
        (self.df_ventanas_trabajadores, self.df_ventanas_estacionamientos, self.req_trabajadores,
         self.fechahora_minima, self.fechahora_maxima, self.ids_tipo_servicio, self.ids_tipo_trabajo,
         self.req_materiales, self.numero_maquinas, self.max_id_insumos_antes_servicio,
         self.fechahora_solicitud, self.fechahora_esperada, self.df_insumos_servicio,
         self.df_consumos_servicio, self.horas_trabajo_por_maquina, self.precio_esperado,
         self.precio_total, self.id_proyecto, self.id_servicio, self.nombre_servicio,
         self.demora_pago_dias, self.tipo_maquinaria
         ) = determina_parametros_servicio(row_servicio, TSATT, RequerimientosTrabajadores,
                                           Asignaciones, Trabajadores, RequerimientosMateriales,
                                           TiposInsumo, Trabajos, TiposServicio, df_insumos,
                                           df_consumos, solo_trabajadores_fijos)

        # Atributos que se establecerán al procesar
        self.df_asignaciones_servicio = None
        self.inicio_trabajos = None
        self.fin_trabajos = None
        self.df_comprar_servicio = None
        self.df_consumir_servicio = None
        self.exito_asignacion = None
        self.exito_compra = None
        self.servicio_aprobado = None
        self.probabilidad_aceptacion = None
        self.razon_rechazo = None
        self.intentos_compra_realizados = None
        self.momento_llegada_insumos = None

    def procesa_servicio(self, TiposInsumo, max_intentos_compra=max_intentos_compra):
        """ Procesa un servicio ya creado como objeto asignando los trabajadores evaluando
        los consumos y compras, determinando si la llegada de insumos ocurre a tiempo,
        y simulando la negociacion con los clientes.

        La dependencia de TiposInsumo es para determinar la fecha de llegada de los insumos
        """
        self.servicio_aprobado = False
        exito_compra = False
        exito_asignacion = True
        df_asignaciones_servicio = pd.DataFrame([])
        df_comprar_servicio = pd.DataFrame([])
        df_consumir_servicio = pd.DataFrame([])

        intentos_compra = 0

        while ((not exito_compra) and (intentos_compra < max_intentos_compra) and
               ((self.fechahora_maxima - self.fechahora_minima).total_seconds() / 3600 > self.horas_trabajo_por_maquina) and
               exito_asignacion):
            intentos_compra += 1
            hash_trabajadores = hash_dataframe(self.df_ventanas_trabajadores)
            hash_estacionamientos = hash_dataframe(self.df_ventanas_estacionamientos)
            exito_asignacion, df_asignaciones_servicio = determina_asignaciones_servicio(
                self.df_ventanas_trabajadores, self.df_ventanas_estacionamientos,
                self.req_trabajadores, self.numero_maquinas)
            hash_asignaciones = hash_dataframe(df_asignaciones_servicio)
            logging.info(f'Para el intento de compra {intentos_compra} determina_asignaciones_servicio uso de entrada df_ventanas_trabajadores de hash {hash_trabajadores} y df_ventanas_estacionamientos de hash {hash_estacionamientos} ')
            logging.info(f'Y se obtuvo las asignaciones de hash {hash_asignaciones}')
            if exito_asignacion:
                df_comprar_servicio, df_consumir_servicio = calcula_compras_y_consumos_servicio(
                    self.df_insumos_servicio, self.df_consumos_servicio, df_asignaciones_servicio,
                    self.req_materiales, self.max_id_insumos_antes_servicio + 1, self.numero_maquinas,
                    self.fechahora_solicitud)
                if len(df_comprar_servicio)>0:
                    tipos_insumo_a_comprar = sorted(set(df_comprar_servicio['id_tipo_insumo']))
                else:
                    tipos_insumo_a_comprar = []
                self.momento_llegada_insumos = calcula_tiempo_entrega(
                    TiposInsumo, tipos_insumo_a_comprar, self.fechahora_solicitud)

                inicio_trabajos = min(df_asignaciones_servicio['fechahora_inicio_ventana']).to_pydatetime()
                # Si los insumos llegan antes que empiecen los trabajos entonces determinamos la compra como exitosa
                # le damos un pequeño margen para que no haya problemas con los milisegundos
                if self.momento_llegada_insumos < (inicio_trabajos+relativedelta(seconds=1)):
                    exito_compra = True
                # Si los insumos no llegan a tiempo para el inicio de los trabajos intentamos de nuevo pero con
                # fechahora minima para las ventanas de trabajadores como el momento de llegada de los insumos
                else:
                    self.fechahora_minima = self.momento_llegada_insumos
                    exito_restriccion, self.df_ventanas_trabajadores = restringir_rangos(self.df_ventanas_trabajadores,
                                                                                         [[self.fechahora_minima,
                                                                                           self.fechahora_maxima]])
                    # Si fallo la restriccion de ventanas cortamos todo el loop
                    if not exito_restriccion:
                        exito_asignacion = False



        self.exito_asignacion = exito_asignacion
        self.exito_compra = exito_compra
        self.df_asignaciones_servicio = df_asignaciones_servicio
        if len(df_asignaciones_servicio)>0:
            self.inicio_trabajos = min(ObServicio.df_asignaciones_servicio['fechahora_inicio_ventana']).to_pydatetime()
            self.fin_trabajos = max(ObServicio.df_asignaciones_servicio['fechahora_fin_ventana']).to_pydatetime()
        self.df_comprar_servicio = df_comprar_servicio
        self.df_consumir_servicio = df_consumir_servicio
        self.intentos_compra_realizados = intentos_compra



        if exito_compra:
            fin_trabajos = max(df_asignaciones_servicio['fechahora_fin_ventana']).to_pydatetime()
            self.servicio_aprobado, self.probabilidad_aceptacion,self.razon_rechazo = simular_negociacion(
                self.precio_total, self.precio_esperado, self.fechahora_solicitud,
                self.fechahora_esperada, fin_trabajos)
        else:
            self.probabilidad_aceptacion = 0.0


    def crea_dfs_insumos_consumos_tabla(self):
        df_comprar = self.df_comprar_servicio
        df_consumir = self.df_consumir_servicio
        # Si no hay nada que comprar simplemente copiamos los dataframes
        # porque no hay nada que modificar
        if len(df_comprar)==0:
            self.df_insumos_a_agregar = df_comprar
            self.df_consumos_a_agregar = df_consumir
            return

        # Paso 1: ID mínima
        id_inicio = df_comprar['id'].min()

        # Paso 2: Agrupar por id_tipo_insumo
        df_agrupado = (
            df_comprar.groupby('id_tipo_insumo', as_index=False)
            .agg({
                'cantidad': 'sum',
                'fechahora_adquisicion_actualizacion': 'first',
                'id_tipo_trabajo': 'first',
                'n_maquina': 'first'
            })
        ).sort_values('id_tipo_insumo',kind="mergesort").reset_index(drop=True)

        # Paso 3: Asignar nuevas IDs
        nuevas_ids = list(range(id_inicio, id_inicio + len(df_agrupado)))
        df_agrupado.insert(0, 'id', nuevas_ids)

        # Paso 4: Generar diccionario de reemplazo
        dict_reemplazo_ids = dict()
        for _, row_insumo in df_agrupado.iterrows():
            id_nueva = row_insumo['id']
            id_tipo_insumo = row_insumo['id_tipo_insumo']
            ids_viejas = df_comprar[df_comprar['id_tipo_insumo'] == id_tipo_insumo]['id'].to_list()
            for id_vieja in ids_viejas:
                dict_reemplazo_ids[id_vieja] = id_nueva

        # Paso 5: Actualizar df_consumir
        df_consumir_actualizado = df_consumir.copy()
        df_consumir_actualizado['id_insumo_si_aplica'] = (df_consumir_actualizado['id_insumo_si_aplica']
                                                          .apply(lambda x: dict_reemplazo_ids.get(x, x)))

        # Paso 6: Guardar resultados como nuevos atributos
        self.df_insumos_a_agregar = df_agrupado
        self.df_consumos_a_agregar = df_consumir_actualizado


    def actualiza_tablas_servicio_aprobado(self, db, Asignaciones, Consumos, Insumos, Cotizaciones,
                                           Servicios, Trabajos, MovimientosFinancieros, TiposInsumo):
        """ Actualiza las tablas de los modelos input (meons TiposInsumo) despues de aprobar un servicio
        La mayoria agregando las filas correspondientes y actualizando valores en Servicios y Cotizaciones
        """

        #Primero revisamos en que id van las tablas
        id_actual_trabajos = id_actual_modelo(Trabajos)
        id_actual_asignaciones = id_actual_modelo(Asignaciones)
        id_actual_movimientos_financieros = id_actual_modelo(MovimientosFinancieros)
        id_actual_consumos = id_actual_modelo(Consumos)

        # Luego tengo que hacer es generar las ids para cada trabajo para usarlas
        # para cada combinación n_maquina/id_tipo_trabajo del servicio

        datos_info_trabajos = []
        df_info_trabajos = pd.DataFrame(datos_info_trabajos)
        serie_info_trabajo = (
            self.df_asignaciones_servicio
            .groupby(['id_tipo_trabajo', 'n_maquina'])
            .agg({'horas_hombre_asignadas': 'sum',
                'estacionamiento': 'first',
                'fechahora_inicio_ventana': 'min',
                'fechahora_fin_ventana': 'max',}))
        lista_n_maquinas = list(range(1,self.numero_maquinas+1))
        for n_maquina in lista_n_maquinas:
            for ind_id_tipo_trabajo in range(len(self.ids_tipo_trabajo)):
                id_tipo_trabajo = self.ids_tipo_trabajo[ind_id_tipo_trabajo]
                info_trabajo = serie_info_trabajo.loc[(id_tipo_trabajo,n_maquina)]
                horas_trabajo,estacionamiento, fechahora_inicio_ventana, fechahora_fin_ventana = info_trabajo
                datos_info_trabajos.append(
                    {'n_maquina':n_maquina,
                    'id_tipo_trabajo':self.ids_tipo_trabajo[ind_id_tipo_trabajo],
                    'id_trabajo':(n_maquina-1)*len(self.ids_tipo_trabajo) + (ind_id_tipo_trabajo+1) + id_actual_trabajos,
                    'horas_trabajo':horas_trabajo,
                    'estacionamiento':estacionamiento,
                    'fechahora_inicio_ventana':fechahora_inicio_ventana,
                    'fechahora_fin_ventana':fechahora_fin_ventana}
                )
        df_info_trabajos = pd.DataFrame(datos_info_trabajos)


        # Ahora creamos la lista de diccionarios para instertar en la tabla asignaciones
        datos_insertar_asignaciones= []
        n_asignacion = 1
        for _, row_asignacion in self.df_asignaciones_servicio.iterrows():
            id_asignacion = id_actual_asignaciones + n_asignacion
            (id_trabajador, fechahora_inicio_ventana, fechahora_fin_ventana, horas_hombre_asignadas, _,
            id_tipo_trabajo, estacionamiento, n_maquina) = row_asignacion
            row_trabajo = df_info_trabajos[(df_info_trabajos['id_tipo_trabajo']==id_tipo_trabajo)
                                        & (df_info_trabajos['n_maquina']==n_maquina)].iloc[0]
            id_trabajo, horas_trabajo = row_trabajo['id_trabajo'], row_trabajo['horas_trabajo']
            horas_trabajadas_total = horas_hombre_asignadas
            horas_trabajadas_extra, porcentaje_df_avance, observaciones, anuladas = 0.0,100.0,'asignacion modelada', False
            porcentaje_de_trabajo = 100 * (horas_hombre_asignadas/horas_trabajo)
            datos_insertar_asignaciones.append({
                'id': id_asignacion,
                'id_trabajo': id_trabajo,
                'id_trabajador': id_trabajador,
                'fechahora_inicio_ventana': fechahora_inicio_ventana.to_pydatetime(),
                'fechahora_fin_ventana': fechahora_fin_ventana.to_pydatetime(),
                'horas_hombre_asignadas': horas_hombre_asignadas,
                'horas_trabajadas_total': horas_trabajadas_total,
                'horas_trabajadas_extra': horas_trabajadas_extra,
                'porcentaje_de_trabajo': porcentaje_de_trabajo,
                'porcentaje_de_avance': porcentaje_df_avance,
                'observaciones': observaciones,
                'anuladas': anuladas
            })
            n_asignacion += 1
            
        query_tipos_trabajo = TiposTrabajo.select().order_by(TiposTrabajo.id)
        df_tipos_trabajo = pd.DataFrame(list(query_tipos_trabajo.dicts()))

        datos_insertar_trabajos = []

        for _,row_trabajo in df_info_trabajos.iterrows():
            (n_maquina, id_tipo_trabajo, id_trabajo, horas_trabajo, estacionamiento,
            fechahora_inicio_ventana, fechahora_fin_ventana) = row_trabajo
            row_tipo_trabajo = df_tipos_trabajo[df_tipos_trabajo['id']==id_tipo_trabajo].iloc[0]
            nombre_trabajo, descripcion_trabajo = row_tipo_trabajo[['nombre','descripcion']]
            datos_insertar_trabajos.append({
                'id':id_trabajo,
                'nombre':nombre_trabajo,
                'id_tipo_trabajo':id_tipo_trabajo,
                'id_servicio':self.id_servicio,
                'n_maquina':n_maquina,
                'estacionamiento':estacionamiento,
                'orden_en_ot':id_trabajo-id_actual_trabajos,
                'descripcion': descripcion_trabajo,
                'horas_hombre_asignadas': horas_trabajo,
                'fechahora_inicio': fechahora_inicio_ventana.to_pydatetime(),
                'fechahora_fin': fechahora_fin_ventana.to_pydatetime()
            })

        datos_insertar_insumos = []
        id_movimiento_financiero = id_actual_movimientos_financieros
        datos_insumos_movimientos = []
        fecha_solicitud = self.fechahora_solicitud.date()
        query_precios_insumos = (PreciosInsumos
                                 .select()
                                 .where((PreciosInsumos.fecha_precio<=fecha_solicitud)
                                        & ((PreciosInsumos.fecha_vigencia is None) |
                                           (PreciosInsumos.fecha_vigencia>=fecha_solicitud)))
                                 .order_by(PreciosInsumos.id))
        df_precios_insumos = pd.DataFrame(query_precios_insumos.dicts())
        for _, row_insumo in  self.df_insumos_a_agregar.iterrows():
            # Aumentamos la id de movimientos financieros por cada insumo ya que van a ser los
            # primeros en agregarse a esa tabla
            id_movimiento_financiero += 1
            id, id_tipo_insumo, cantidad_minima, fechahora_adquisicion_actualizacion, id_tipo_trabajo, n_maquina = row_insumo
            descripcion = ''
            
            df_precios_insumo_actual = df_precios_insumos[df_precios_insumos['id_tipo_insumo']==id_tipo_insumo]
            cantidad_a_comprar, precio_insumos,_ = decide_compra(df_precios_insumo_actual,cantidad_minima)
            datos_insertar_insumos.append({'id': id,
                                        'id_tipo_insumo': id_tipo_insumo,
                                        'cantidad': cantidad_a_comprar,
                                        'descripcion': descripcion,
                                        'fechahora_adquisicion_actualizacion': fechahora_adquisicion_actualizacion,
                                        'fecha_caducidad': None,
                                        'id_movimiento_financiero_si_aplica': id_movimiento_financiero})

            datos_insumos_movimientos.append({'id_tipo_insumo':id_tipo_insumo, 'cantidad': cantidad_a_comprar,
                                            'fechahora_compra': self.fechahora_solicitud,
                                            'id_movimiento_financiero': id_movimiento_financiero,
                                            'precio_insumos': precio_insumos})
        df_insumos_movimientos = pd.DataFrame(datos_insumos_movimientos)

        datos_insertar_consumos = []
        id_consumo = id_actual_consumos
        for _, row_consumo in  self.df_consumos_a_agregar.iterrows():
            # Aumentamos la id de movimientos financieros por cada insumo ya que van a ser los
            # primeros en agregarse a esa tabla
            (id_tipo_insumo, cantidad, porcentaje_uso, uso_ponderado, fechahora_inicio_uso,
            fechahora_fin_uso, id_insumo_si_aplica, insumo_reutilizable, id_tipo_trabajo, n_maquina) = row_consumo
            row_trabajo = df_info_trabajos[(df_info_trabajos['id_tipo_trabajo']==id_tipo_trabajo) &
                                        (df_info_trabajos['n_maquina']==n_maquina)].iloc[0]
            id_trabajo = row_trabajo['id_trabajo']
            id_consumo += 1
            datos_insertar_consumos.append({
                'id': id_consumo,
                'id_tipo_insumo': id_tipo_insumo,
                'item_especifico': None,
                'cantidad': cantidad,
                'porcentaje_de_uso': porcentaje_uso,
                'uso_ponderado': uso_ponderado,
                'fechahora_inicio_uso': fechahora_inicio_uso,
                'fechahora_fin_uso': fechahora_fin_uso,
                'validado': False,
                'id_trabajo_si_aplica': id_trabajo,
                'descontado_en_insumos': False,
                'id_insumo_si_aplica': id_insumo_si_aplica
            })

        datos_insertar_movimientos_financieros = []
        for _, row_movimientos in df_insumos_movimientos.iterrows():
            id_tipo_insumo, cantidad, timestamp_compra, id_movimiento_financiero, precio_insumos = row_movimientos
            fechahora_compra = timestamp_compra.to_pydatetime()
            query_tipos_insumo = TiposInsumo.select(TiposInsumo.id,TiposInsumo.nombre).where(TiposInsumo.id==id_tipo_insumo).order_by(TiposInsumo.id)
            nombre_insumo = list(query_tipos_insumo.dicts())[0]['nombre']

            dic_movimiento = crea_dict_para_movimiento_financiero(id_movimiento_financiero, fechahora_compra,'egreso',
                                                                'compra insumos',precio_insumos,
                                                                f'Compra de {nombre_insumo} ({cantidad})',True,True)

            datos_insertar_movimientos_financieros.append(dic_movimiento)
        
        precio_total_insumos = 0
        if len(df_insumos_movimientos)>0:
            # Actualizamos la id de movimientos financieros para que considere la compra de insumos
            id_actual_movimientos_financieros = max(df_insumos_movimientos['id_movimiento_financiero'])
            precio_total_insumos = sum(df_insumos_movimientos['precio_insumos'])

        # Luego agregamos el movimiento del pago del servicio
        id_actual_movimientos_financieros += 1
        fechahora_pago = self.fin_trabajos + relativedelta(days=int(self.demora_pago_dias))

        dic_movimiento = crea_dict_para_movimiento_financiero(id_actual_movimientos_financieros, fechahora_pago,
                                                            'ingreso', 'pago de servicio',self.precio_total,
                                                            f'Pago de servicio {self.nombre_servicio} para {self.numero_maquinas} máquinas',
                                                            True,True)

        datos_insertar_movimientos_financieros.append(dic_movimiento)

        # Ahora agregamos el movimiento correspondiente a la inversion si es que falta dinero
        saldo = calcula_saldo(MovimientosFinancieros, self.fechahora_solicitud)           
        
        if saldo < precio_total_insumos:
            id_actual_movimientos_financieros += 1
            aporte_inversion = precio_total_insumos - saldo
            dic_movimiento = crea_dict_para_movimiento_financiero(id_actual_movimientos_financieros, fechahora_compra,
                                                                'ingreso','inyección de capital',aporte_inversion,
                                                                f'Aporte de capital para compra de insumos de servicio {self.nombre_servicio} para {self.numero_maquinas} máquinas',
                                                                False,False)
            datos_insertar_movimientos_financieros.append(dic_movimiento)


        with db.atomic():
            Trabajos.insert_many(datos_insertar_trabajos).execute()
            Asignaciones.insert_many(datos_insertar_asignaciones).execute()
            Insumos.insert_many(datos_insertar_insumos).execute()
            Consumos.insert_many(datos_insertar_consumos).execute()
            MovimientosFinancieros.insert_many(datos_insertar_movimientos_financieros).execute()
            # Actualizamos tambien el estado del servicio
            (Servicios
            .update({Servicios.estado: 'finalizado',
                     Servicios.fecha_actualizacion_estado: self.fin_trabajos,
                     Servicios.fecha_inicio_trabajos: self.inicio_trabajos,
                     Servicios.fecha_fin_trabajos: self.fin_trabajos,
                     Servicios.nombre_orden_trabajo: f'OT01_Servicio{self.id_servicio:05}_Proyecto{self.id_proyecto:05}.pdf',
                     Servicios.fecha_propuesta: self.fin_trabajos,
                     Servicios.total_precio_ot: self.precio_total})
            .where(Servicios.id == self.id_servicio).execute())
            # Y finalmente actualizamos la tabla cotizaciones 
            Cotizaciones.update({Cotizaciones.estado: 'aceptada',
                                 Cotizaciones.fecha_entrega: self.fin_trabajos.date()}).where(Cotizaciones.id == self.id_servicio).execute()


    def actualiza_tablas_servicio_rechazado(self, db, Cotizaciones, Servicios, Proyectos, cliente_perdido):
        # Si se pudo hacer las asignaciones de trabajadores e insumos significa
        # que el problema fue la negociación
        if self.exito_compra is True:
            estado_servicio = 'rechazado'
            estado_cotizacion = f'rechazada por {self.razon_rechazo}'
            fecha_propuesta = max(self.df_asignaciones_servicio['fechahora_fin_ventana']).to_pydatetime()
        else:
            estado_servicio = estado_cotizacion = 'inviable'
            fecha_propuesta = None

        tiempo_servicio = self.fechahora_solicitud + relativedelta(days=1)

        with db.atomic():
            # Actualizar el estado del servicio actual
            (Servicios
            .update({Servicios.estado: estado_servicio,
                    Servicios.fecha_actualizacion_estado: tiempo_servicio,
                    Servicios.fecha_propuesta: fecha_propuesta})
            .where(Servicios.id == self.id_servicio)
            .execute())

            # Actualizar la cotización asociada
            (Cotizaciones
            .update({Cotizaciones.estado: estado_cotizacion})
            .where(Cotizaciones.id == self.id_servicio)
            .execute())

            if cliente_perdido is True:
                # Actualizar los servicios futuros del mismo proyecto
                (Servicios
                .update({Servicios.estado: 'cliente perdido',
                        Servicios.fecha_actualizacion_estado: tiempo_servicio,
                        Servicios.fecha_propuesta: fecha_propuesta})
                .where((Servicios.id_proyecto == self.id_proyecto) &
                        (Servicios.estado == 'planificado'))
                .execute())


                # Actualizar el proyecto
                (Proyectos
                .update({Proyectos.descripcion: f'Cortado porque el servicio {self.id_servicio} fue declarado {estado_servicio}',
                        Proyectos.fecha_fin: tiempo_servicio.date()})
                .where(Proyectos.id == self.id_proyecto)
                .execute())

print('Paso 7', flush=True)
# Lo primero que hago es llenar las tablas Clientes, Proyectos, Servicios y 
# Cotizaciones según los parámetros del archivo de configuracion
genera_registros_base(Clientes, TiposServicio, Proyectos, Servicios, Cotizaciones, db, config)

print('Paso 8', flush=True)

# Ahora defino todos los datafrmaes que no cambian entre servicio y servicio
(df_estacionamientos_totales, df_servicios, df_disponibilidad_trabajadores, df_trabajadores,
 df_ventanas_trabajadores_totales
 ) = crea_dataframes_fijos(Servicios, Cotizaciones,
                           Proyectos, Clientes, TiposServicio,
                           DisponibilidadesTrabajadores, Trabajadores,config,
                           trabajadores_fijos=solo_trabajadores_fijos)

horas_totales_disponibles_trabajo = sum(df_ventanas_trabajadores_totales['horas_ventana'])
años_funcionamiento =  (fechahora_cierre - fechahora_inicio_empresa).days/365.25
n_trabajadores = len(set(df_trabajadores['id']))
logging.info("")
logging.info(f"En {años_funcionamiento:.2f} años de funcionamiento tenemos un total de "
             f"{horas_totales_disponibles_trabajo} horas disponibles entre {n_trabajadores} trabajadores")
logging.info("")

df_eventos = crea_df_eventos(df_servicios, MovimientosRecurrentes,
                             fechahora_inicio_empresa, fechahora_cierre)
df_movimientos_recurrentes = pd.DataFrame(list(MovimientosRecurrentes.select().order_by(MovimientosRecurrentes.id).dicts()))
set_indices_df_a_saltar = set()
ti =time.time()
n_servicios = 0

print('Paso 9', flush=True)
Objetos = {} # Este diccionario es para guardar todos los objetos de servicios
for idx, row_evento in df_eventos.iterrows():
    # Si el indice del df esta en la lista de los que hay que saltar
    # (servicios nunca solicitados por perdida de cliente) entonces saltamos
    if idx in set_indices_df_a_saltar:
        continue
    # Acá creo los dataframes que son específicos para el servicio revisado
    df_insumos, df_consumos = crea_dataframes_variables(Insumos, Consumos)

    id_en_tabla, fecha_evento, id_proyecto_servicios, tipo_evento = row_evento
    
    if tipo_evento=='servicio':
        t_actual = time.time()
        dt = t_actual - ti
        if n_servicios % 10 ==0 and n_servicios>0:
            print(f'Habiendo transcurrido {dt/60:6.2f} minutos hemos recorrido {n_servicios} servicios', flush=True)
        n_servicios += 1
        row_servicio = df_servicios[df_servicios['id']==id_en_tabla].iloc[0]

        # Luego creo el objeto con los parametros iniciales
        ObServicio = ClServicio(row_servicio, TSATT, RequerimientosTrabajadores, Asignaciones, Trabajadores,
                                RequerimientosMateriales, TiposInsumo, Trabajos, TiposServicio,
                                df_insumos, df_consumos, solo_trabajadores_fijos = solo_trabajadores_fijos)
        logging.info(f'Empezando la evaluación del servicio {ObServicio.id_servicio}')
        # Y proceso el servicio
        ObServicio.procesa_servicio(TiposInsumo)
        # Luego crea los dataframes para llenar las tablas insumos y consumos
        ObServicio.crea_dfs_insumos_consumos_tabla()

        # Ahora actualizo las tablas segun el resultado del procesamiento del servicio
        if ObServicio.servicio_aprobado:
            asignaciones_servicio = ObServicio.df_asignaciones_servicio
            horas_asignadas = sum(asignaciones_servicio['horas_hombre_asignadas'])
            str_insumos = ObServicio.momento_llegada_insumos.strftime("%Y-%m-%d %H:%M:%S")
            str_solicitud = ObServicio.fechahora_solicitud.strftime("%Y-%m-%d %H:%M:%S")
            str_inicio = ObServicio.inicio_trabajos.strftime("%Y-%m-%d %H:%M:%S")
            str_fin = ObServicio.fin_trabajos.strftime("%Y-%m-%d %H:%M:%S")

            logging.info(f'Servicio {ObServicio.id_servicio} aprobado con fecha_solocitud={str_solicitud}, '
                         f'llegada_insumos={str_insumos}, inicio_trabajos={str_inicio}, ' 
                         f'fin_trabajos={str_fin} y horas_asignadas={horas_asignadas}')
            ObServicio.actualiza_tablas_servicio_aprobado(db, Asignaciones, Consumos, Insumos, Cotizaciones,
                                                          Servicios, Trabajos, MovimientosFinancieros, TiposInsumo)



        else:
            if ObServicio.exito_compra:
                logging.info(f'Servicio {ObServicio.id_servicio} rechazado por el cliente')
            else:
                logging.info(f'Servicio {ObServicio.id_servicio} inviable')
            
            # Primero vemos si perdemos el cliente por haber perdido el servicio
            cliente_perdido = (rng_random.random()<probabilidad_perdida_por_rechazo)
            ObServicio.actualiza_tablas_servicio_rechazado(db, Cotizaciones, Servicios, Proyectos,cliente_perdido)
            # Si perdemos el cliente
            if cliente_perdido is True:
                # Aquí se remueven los servicios rechazados de df_eventos que pertenezcan
                # al mismo proyecto pero sean futuros porque ni siquiera se van a planificar ya que al 
                # no tener exito el servicio se cancela el proyecto completo
                df_filas_a_eliminar = (df_eventos[(df_eventos['tipo_evento']=='servicio')
                                                & (df_eventos['id_proyecto_servicios']==ObServicio.id_proyecto)
                                                & (df_eventos['fecha_evento']>ObServicio.fechahora_solicitud)])
                lista_indices_a_eliminar_actual = df_filas_a_eliminar.index.to_list()
                set_indices_df_a_saltar.update(lista_indices_a_eliminar_actual)
                servicios_a_eliminar = sorted(list(set(df_filas_a_eliminar['id_en_tabla'])))
                logging.info(f'Cortamos además los servicios {servicios_a_eliminar} porque se perdió el cliente')
        Objetos[ObServicio.id_servicio] = ObServicio

    elif tipo_evento == 'movimiento_recurrente':
        row_movimiento = df_movimientos_recurrentes[df_movimientos_recurrentes['id']==id_en_tabla].iloc[0]
        (id_gasto_recurrente, nombre, descripcion, categoria, tipo, divisa, valor_periodo, unidad_periodo,
         info_extra_recurrencia, modo_calculo_monto, valor_fijo, valor_por_servicio, _, _, _) =  row_movimiento
        fecha_final_periodo = fecha_evento.to_pydatetime()
        fecha_inicial_periodo = fecha_final_periodo - define_delta_tiempo(unidad_periodo, valor_periodo)
        # Contamos los servicios que se realizaron en el periodo involucrado en el cobro
        n_servicios_periodo = len(df_servicios[(df_servicios['fecha_inicio_trabajos']<fecha_final_periodo) &
                                               (df_servicios['fecha_fin_trabajos']>=fecha_inicial_periodo)])
        if modo_calculo_monto=='fijo':
            valor = valor_fijo
        elif modo_calculo_monto=='por_servicio_mas_fijo':
            valor = valor_fijo + n_servicios_periodo * valor_por_servicio
        elif modo_calculo_monto=='por_servicio':
            valor = n_servicios_periodo * valor_por_servicio
        elif modo_calculo_monto=='fijo_en_uf':
            precio_uf = uf_por_año[str(fecha_final_periodo.year)]
            valor = valor_fijo * precio_uf

        # Si el valor del movimiento financiero es 0 simplemente pasamos al siguiente evento
        if valor==0:
            continue
        
        if tipo=='salario':
            deducible=False
            tipo_mov='salario'
        else:
            deducible=True
            tipo_mov = 'cuentas'

        # Ahora agregamos el movimiento correspondiente a la inversion si es que falta dinero
        saldo = calcula_saldo(MovimientosFinancieros, fecha_final_periodo)           
        id_actual_movimientos_financieros = id_actual_modelo(MovimientosFinancieros)
        datos_insertar_movimientos_financieros = []
        if saldo < valor:
            id_actual_movimientos_financieros += 1
            aporte_inversion = valor - saldo
            dic_movimiento = crea_dict_para_movimiento_financiero(id_actual_movimientos_financieros, fecha_final_periodo,
                                                                'ingreso','inyección de capital',aporte_inversion,
                                                                f'Aporte de capital para pagar {nombre}',
                                                                False,False,id_gasto_recurrente=id_gasto_recurrente)
            datos_insertar_movimientos_financieros.append(dic_movimiento)

        # Luego agregamos el movimiento financiero del gasto recurrente
        id_actual_movimientos_financieros += 1
        dic_movimiento = crea_dict_para_movimiento_financiero(id_actual_movimientos_financieros, fecha_final_periodo,
                                                            'egreso',tipo_mov,valor,
                                                            f'{nombre}',
                                                            True,deducible,id_gasto_recurrente=id_gasto_recurrente)
        datos_insertar_movimientos_financieros.append(dic_movimiento)
        # Y finalmente agregamos la 1 o 2 entradas a movimientos financieros
        MovimientosFinancieros.insert_many(datos_insertar_movimientos_financieros).execute()

print('Paso 10', flush=True)

# Finalmente escribimos el resumen financiero en el log
# usando la tabla servicios ahora que ya fue actualizada
escribe_finanzas_en_log(MovimientosFinancieros,Servicios,logging,desgloza_por_año=True)

query_movimientos = MovimientosFinancieros.select()
df_movimientos = pd.DataFrame(list(query_movimientos.dicts()))
df_inversion = df_movimientos[(df_movimientos['tipo']=='inyección de capital')]
df_ingresos = df_movimientos[(df_movimientos['tipo']=='pago de servicio')]
df_egresos = df_movimientos[df_movimientos['categoria']=='egreso']

total_inversion = sum(df_inversion['monto'])
total_ingresos = sum(df_ingresos['monto'])
total_egresos = sum(df_egresos['monto'])
total_ganancia = total_ingresos - total_egresos