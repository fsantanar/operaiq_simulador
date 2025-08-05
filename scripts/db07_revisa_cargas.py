import pandas as pd
import numpy as np
import yaml
import datetime
import pandas as pd
from peewee import fn
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.modelos import (Roles, Trabajadores, DisponibilidadesTrabajadores,
                          TiposServicio, Servicios, Trabajos, Asignaciones,
                          RequerimientosTrabajadores)
from src.modelos import TiposServicioATiposTrabajo as TSATT
from src.utils import contar_dias_semana




def calcula_horas_totales(df_disponibilidades_por_dia,fecha_in,fecha_final):
    horas_totales = 0
    for dia_semana in range(1,5+1):
        disponibilidades_dia = df_disponibilidades_por_dia[df_disponibilidades_por_dia['dia_semana']==dia_semana]
        horas_por_dia = disponibilidades_dia['hora_fin'].values[0] - disponibilidades_dia['hora_inicio'].values[0]
        horas_totales += horas_por_dia * contar_dias_semana(fecha_in, fecha_final, dia_semana)
    return horas_totales


def calcular_uso_estacionamiento(grupo):
    intervalos = grupo[['fechahora_inicio_ventana', 'fechahora_fin_ventana']].sort_values(by='fechahora_inicio_ventana').values
    total = pd.Timedelta(0)
    inicio_actual, fin_actual = intervalos[0]

    for inicio, fin in intervalos[1:]:
        if inicio <= fin_actual:
            fin_actual = max(fin_actual, fin)
        else:
            total += fin_actual - inicio_actual
            inicio_actual, fin_actual = inicio, fin
    total += fin_actual - inicio_actual
    return total.total_seconds() / 3600  # horas


def calcula_horas_por_estacionamiento(df_asignaciones,info_estacionamientos, fecha_inicial,fecha_final):
    df_asignaciones_periodo = df_asignaciones[(df_asignaciones['fecha']>=fecha_inicial) & (df_asignaciones['fecha']<=fecha_final)]
    tipos_estacionamiento = sorted([key+str(n) for key, el in info_estacionamientos.items() for n in range(1,el+1)])
    horas_por_estacionamiento = dict([])
    for tipo_estacionamiento in tipos_estacionamiento:
        df_asignaciones_estacionamiento = df_asignaciones_periodo[df_asignaciones_periodo['estacionamiento']==tipo_estacionamiento]
        if len(df_asignaciones_estacionamiento)==0:
            horas_totales = 0
        else:
            # Agrupar asignaciones por fecha
            uso_estacionamiento_por_dia = (
                df_asignaciones_estacionamiento.groupby(['fecha'])
                .apply(calcular_uso_estacionamiento)
                .reset_index(name='horas_usadas'))
        
            # Opción: sumar todo
            horas_totales = uso_estacionamiento_por_dia['horas_usadas'].sum()
        horas_por_estacionamiento[tipo_estacionamiento] =  horas_totales
    
    return horas_por_estacionamiento



# Cargar la configuración desde el archivo YAML
with open('../config.yml', 'r') as file:
    config = yaml.safe_load(file)




query_req_servicios = (Servicios
                       .select(Servicios.id, Servicios.unidad_tipo_servicio, Servicios.fecha_solicitud, Servicios.estado,
                               TSATT.id_tipo_servicio, TSATT.id_tipo_trabajo,
                               RequerimientosTrabajadores.id_rol,
                               (Servicios.unidad_tipo_servicio*RequerimientosTrabajadores.horas_hombre_requeridas).alias('horas_hombre_requeridas'))
                       .join(TSATT,on=(Servicios.ids_tipo_servicio.cast('int')==TSATT.id_tipo_servicio))
                       .join(RequerimientosTrabajadores, on=(TSATT.id_tipo_trabajo==RequerimientosTrabajadores.id_tipo_trabajo)))
# Este dataframe tiene una entrada por cada requerimiento laboral total (ya multiplicado por el numero de maquinas)
# por cada trabajo/rol de cada servicio
df_req_servicios = pd.DataFrame(list(query_req_servicios.dicts()))

query_asignaciones = (Asignaciones
                      .select(Asignaciones.id_trabajador, Asignaciones.fechahora_inicio_ventana,
                              Asignaciones.fechahora_fin_ventana, Asignaciones.horas_hombre_asignadas,
                              Trabajadores.id_rol, Trabajos.estacionamiento, TiposServicio.tipo_maquinaria)
                      .join(Trabajadores, on=(Asignaciones.id_trabajador==Trabajadores.id))
                      .join(Trabajos, on=(Asignaciones.id_trabajo==Trabajos.id))
                      .join(Servicios, on=(Trabajos.id_servicio==Servicios.id))
                      .join(TiposServicio, on=(Servicios.ids_tipo_servicio.cast('int')==TiposServicio.id)))
df_asignaciones = pd.DataFrame(list(query_asignaciones.dicts()))
# Crear columna de fecha (día calendario)
df_asignaciones['fecha'] = df_asignaciones['fechahora_inicio_ventana'].dt.date



query_trabajadores_fijos = (Trabajadores.
                            select(Trabajadores,Roles.nombre.alias('nombre_rol'), Roles.hh_en_uf_fijo,
                                   fn.SUM(DisponibilidadesTrabajadores.horas_dia).alias('horas_por_semana'))
                            .join(Roles, on=(Trabajadores.id_rol==Roles.id))
                            .join(DisponibilidadesTrabajadores,
                                  on=(Trabajadores.id==DisponibilidadesTrabajadores.id_trabajador))
                            .where(Trabajadores.modalidad_contrato=='fijo')
                            .group_by(Trabajadores.id, Roles.id))
df_trabajadores_fijos = pd.DataFrame(list(query_trabajadores_fijos.dicts()))


uf_por_año = config['csvs']['precios']['uf_por_año']
fraccion_de_hh_en_sueldo = config['csvs']['trabajadores']['fraccion_de_hh_en_sueldo']
str_fecha_inicio_empresa = config['csvs']['fecha_inicio_empresa']
fecha_inicio = datetime.datetime.strptime(str_fecha_inicio_empresa, '%Y/%m/%d').date()
str_fecha_cierre = config['csvs']['fecha_cierre']
fecha_cierre = datetime.datetime.strptime(str_fecha_cierre, '%Y/%m/%d').date()
ultima_asignacion = max(df_asignaciones['fechahora_fin_ventana']).to_pydatetime().date()
# Aignamos el ultimos dia como el maximo entre el cierre y la asignacion para ser consistentes en requerimientos y asignaciones
fecha_fin = max([fecha_cierre, ultima_asignacion])
solo_trabajadores_fijos = config['instancias']['solo_trabajadores_fijos']
info_estacionamientos = config['instancias']['estacionamientos']
primer_año = int(str_fecha_inicio_empresa.split('/')[0])
ultimo_año = fecha_fin.year


if solo_trabajadores_fijos is True:
    condicion = (Trabajadores.modalidad_contrato == 'fijo')
else:
    condicion = True


query_disponibilidades = (
    DisponibilidadesTrabajadores
    .select(
        Trabajadores.id.alias('id_trabajador'),
        DisponibilidadesTrabajadores.dia_semana,
        DisponibilidadesTrabajadores.hora_inicio,
        DisponibilidadesTrabajadores.hora_fin
    )
    .join(Trabajadores, on=(DisponibilidadesTrabajadores.id_trabajador == Trabajadores.id))
    .where( (DisponibilidadesTrabajadores.feriado == False) & condicion))

df_disponibilidades = pd.DataFrame(list(query_disponibilidades.dicts()))

df_disponibilidades_totales = (df_disponibilidades.groupby('dia_semana')
                               .agg(hora_inicio=('hora_inicio', 'min'),
                                    hora_fin=('hora_fin', 'max'))
                                .reset_index())


horas_estacionamiento_totales = calcula_horas_totales(df_disponibilidades_totales,fecha_inicio,fecha_fin)
print()
print()
print(f'En todo el periodo de funcionamiento hay {horas_estacionamiento_totales:.1f} horas por estacionamiento disponibles')
print(f'Y el uso por estacionamiento es:')
horas_por_estacionamiento = calcula_horas_por_estacionamiento(df_asignaciones, info_estacionamientos, fecha_inicio, fecha_fin)
estacionamientos = sorted(list(horas_por_estacionamiento.keys()))
for estacionamiento in estacionamientos:
    horas_usadas = horas_por_estacionamiento[estacionamiento]
    print(f'Para estacionamiento {estacionamiento} hay {horas_usadas:.1f} horas usadas que equivalen '
          f'al {horas_usadas/horas_estacionamiento_totales*100:.1f}% del total')

print()
print()
print('Desglozando por año obtenemos que')
for año in range(primer_año,ultimo_año+1):
    inicio_año = max(datetime.date(año,1,1), fecha_inicio)
    fin_año = min(datetime.date(año,12,31), fecha_fin)
    horas_estacionamiento_año = calcula_horas_totales(df_disponibilidades_totales,inicio_año,fin_año)
    print()
    print(f'En el año {año} hay {horas_estacionamiento_año:.1f} horas por estacionamiento disponibles')
    print(f'Y el uso por estacionamiento es:')
    horas_por_estacionamiento_año = calcula_horas_por_estacionamiento(df_asignaciones, info_estacionamientos, inicio_año, fin_año)
    estacionamientos = sorted(list(horas_por_estacionamiento_año.keys()))
    for estacionamiento in estacionamientos:
        horas_usadas_año = horas_por_estacionamiento_año[estacionamiento]
        print(f'Para estacionamiento {estacionamiento} hay {horas_usadas_año:.1f} horas usadas que equivalen '
            f'al {horas_usadas_año/horas_estacionamiento_año*100:.1f}% del total')


print()
print()
ids_rol = sorted(list(set(df_asignaciones['id_rol'])))
horas_trabajadas_rol_año = {f'rol{id_rol}_año{año}':0 for año in range(primer_año,ultimo_año+1) for id_rol in ids_rol}
horas_disponibles_rol_año = {f'rol{id_rol}_año{año}':0 for año in range(primer_año,ultimo_año+1) for id_rol in ids_rol}
for id_rol in ids_rol:
    df_req_servicios_rol = df_req_servicios[df_req_servicios['id_rol']==id_rol]
    horas_requeridas_rol = sum(df_req_servicios_rol['horas_hombre_requeridas'])
    asignaciones_rol = df_asignaciones[df_asignaciones['id_rol']==id_rol]
    horas_trabajadas_rol = sum(asignaciones_rol['horas_hombre_asignadas'])
    rows_trabajadores = df_trabajadores_fijos[df_trabajadores_fijos['id_rol']==id_rol]
    n_trabajadores = len(rows_trabajadores)
    nombre_rol = rows_trabajadores['nombre_rol'].values[0]
    hh_en_uf = rows_trabajadores['hh_en_uf_fijo'].values[0]
     # tomamos 2022 porque es el mas cercano al promedio
    sueldo_promedio = hh_en_uf * fraccion_de_hh_en_sueldo * 40 * 52 / 12 * uf_por_año['2022']
    horas_disponibles_rol = 0
    for _,row_trabajador in rows_trabajadores.iterrows():
        id_trabajador = row_trabajador['id']
        df_disponibilidades_trabajador = df_disponibilidades[df_disponibilidades['id_trabajador']==id_trabajador]
        asignaciones_trabajador = asignaciones_rol[asignaciones_rol['id_trabajador']==id_trabajador]
        horas_por_semana_trabajador = row_trabajador['horas_por_semana']
        inicio = row_trabajador['iniciacion']
        fin = row_trabajador['termino'] if row_trabajador['termino'] is not None else fecha_fin
        dias_disponibles_trabajador = np.busday_count(inicio, fin + datetime.timedelta(days=1))
        for año in range(primer_año,ultimo_año+1):
            llave_rol_año = f'rol{id_rol}_año{año}'
            primer_dia_año = datetime.date(año,1,1)
            ultimo_dia_año = datetime.date(año,12,31)
            # Si el trabajador no trabajó ese año simplemente asignamos 0 a las horas
            if inicio>ultimo_dia_año or fin<primer_dia_año:
                horas_trabajadas_trabajador_año, horas_disponibles_trabajador_año = 0,0
            else:
                inicio_año = max(primer_dia_año, inicio)
                fin_año = min(ultimo_dia_año, fin)
                horas_disponibles_trabajador_año = calcula_horas_totales(df_disponibilidades_trabajador,inicio_año,fin_año)
                fechahora_inicio_año = datetime.datetime.combine(inicio_año, datetime.time(0,0))
                fechahora_fin_año = datetime.datetime.combine(fin_año, datetime.time(23,59,59,999999))
                asignaciones_trabajador_año = asignaciones_trabajador[(asignaciones_trabajador['fechahora_inicio_ventana']>=fechahora_inicio_año) &
                                                                      (asignaciones_trabajador['fechahora_fin_ventana']<=fechahora_fin_año)]
                horas_trabajadas_trabajador_año = sum(asignaciones_trabajador_año['horas_hombre_asignadas'])
            # aca sumamos las horas disponibles y trabajadas al rol/año correspondiente
            horas_trabajadas_rol_año[llave_rol_año] += horas_trabajadas_trabajador_año
            horas_disponibles_rol_año[llave_rol_año] += horas_disponibles_trabajador_año
            horas_disponibles_rol += horas_disponibles_trabajador_año
    print()
    print()
    print(f'Para el rol {nombre_rol} hay {n_trabajadores} trabajadores con sueldo de ${sueldo_promedio:,.0f} '
          f'cada uno y un total de {horas_trabajadas_rol:.1f} horas asignadas de {horas_requeridas_rol} '
          f'horas requeridas y {horas_disponibles_rol:.1f} disponibles totales')
    print()
    print('Desglozando por año')    
    for año in range(primer_año,ultimo_año+1):
        # Sumo a las horas realmente asignadas para cada año aquellos requerimientos que corresponden
        # a servicios solicitados ese año que no fueron realizados
        llave_rol_año = f'rol{id_rol}_año{año}'
        
        df_req_servicios_rol_año_faltantes = df_req_servicios_rol[(df_req_servicios_rol['estado']!='finalizado') &
                                                                  (df_req_servicios_rol['fecha_solicitud'].dt.year==año)]
        horas_requeridas_rol_año_faltantes = sum(df_req_servicios_rol_año_faltantes['horas_hombre_requeridas'])
        horas_requeridas_rol_año = horas_trabajadas_rol_año[llave_rol_año] + horas_requeridas_rol_año_faltantes
        
        print(f'En el año {año} hubo {horas_trabajadas_rol_año[llave_rol_año]:.1f} horas trabajadas de '
              f'{horas_requeridas_rol_año:.1f} requeridas y {horas_disponibles_rol_año[llave_rol_año]:.1f} '
              f'disponibles')
    



horas_requeridas_totales = sum(df_req_servicios['horas_hombre_requeridas'])
horas_asignadas_totales = sum(df_asignaciones['horas_hombre_asignadas'])
horas_disponibles_totales = sum(horas_disponibles_rol_año.values())
print()
print(f'En total se asignaron {horas_asignadas_totales:.1f} de {horas_requeridas_totales:.1f} requeridas y '
      f'{horas_disponibles_totales:.1f} disponibles')
print()

