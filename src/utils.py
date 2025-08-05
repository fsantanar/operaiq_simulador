import json
import math
import hashlib
from dateutil.relativedelta import relativedelta
import datetime
import pandas as pd
import numpy as np


###################################################
###                                             ###
###  Al principio defino funciones básicas que  ###
###  pueden ser útiles para otros proyectos     ###
###                                             ###
###################################################

def lee_json(nombre_y_ruta_archivo):
    with open(nombre_y_ruta_archivo, 'r', encoding='utf-8') as f:
        output = json.load(f)
    return output


def calcular_dv(rut):
    """
    Calcula el dígito verificador del RUT chileno.
    Args:
        rut (int): El RUT sin el dígito verificador.
    Returns:
        str: El dígito verificador (0-9 o 'K').
    """
    rut = str(rut)[::-1]  # Invierte el RUT para usar la serie multiplicadora
    multiplicadores = [2, 3, 4, 5, 6, 7]  # Serie multiplicadora estándar
    suma = 0
    
    for i, digito in enumerate(rut):
        multiplicador = multiplicadores[i % len(multiplicadores)]
        suma += int(digito) * multiplicador
    
    resto = suma % 11
    dv = 11 - resto
    
    if dv == 11:
        return '0'
    elif dv == 10:
        return 'K'
    else:
        return str(dv)


def hash_dataframe(df):
    """ Convierte un dataframe en un hash para poder
    usar por ejemplo en un log y asegurar reproducibilidad
    """
    # Ordenar columnas y filas para evitar diferencias por orden
    df_sorted = df.sort_index(axis=0).sort_index(axis=1)
    # Convertir a string
    df_bytes = df_sorted.to_csv(index=False).encode('utf-8')
    # Calcular hash
    return hashlib.md5(df_bytes).hexdigest()


def es_feriado(date):
    ### WARNING
    # Por ahora simplemente los hago todos no feriados pero despues puede convenir relamente
    # crear la función
    return False

def id_actual_modelo(Modelo):
    """Obtiene la mayor id registrada en el modelo"""
    df_tabla = pd.DataFrame(list(Modelo.select(Modelo.id).dicts()))
    if len(df_tabla)==0:
        id_actual = 0
    else:
        id_actual = max(df_tabla['id'])
    return int(id_actual)


def define_delta_tiempo(unidad, valor):
    """Define un intervalo de tiempo según el valor y la unidad de entrada"""
    if unidad == 'días':
        delta = relativedelta(days=valor)
    elif unidad == 'semanas':
        delta = relativedelta(weeks=valor)
    elif unidad == 'meses':
        delta = relativedelta(months=valor)
    elif unidad == 'años':
        delta = relativedelta(years=valor)
    
    return delta

def desplazar_dias_habiles(fecha, n_dias):
    """ Toma un datetime y lo desplaza n_dias habiles"""
    dias_enteros = int(n_dias)
    fraccion = n_dias - dias_enteros
    fecha_actual = fecha

    # Avanzar los días hábiles enteros
    while dias_enteros > 0:
        fecha_actual += datetime.timedelta(days=1)
        if fecha_actual.weekday() < 5:
            dias_enteros -= 1

    # Sumar la fracción como días reales
    if fraccion > 0:
        fecha_actual += datetime.timedelta(days=fraccion)
        # Si cae en sábado (5) o domingo (6), ajustar al lunes siguiente
        dia_semana = fecha_actual.weekday()
        if dia_semana >= 5:  # sábado o domingo
            fecha_actual += datetime.timedelta(days=7-dia_semana)
    return fecha_actual


def restar_rangos(rango_base, rango_a_restar):
    min_base, max_base = rango_base
    min_resta, max_resta = rango_a_restar

    # No hay intersección
    if max_resta <= min_base or min_resta >= max_base:
        return [rango_base]

    # El rango a restar cubre completamente al rango base
    if min_resta <= min_base and max_resta >= max_base:
        return []

    # Hay intersección parcial
    resultado = []
    if min_resta > min_base:
        resultado.append((min_base, min(min_resta, max_base)))
    if max_resta < max_base:
        resultado.append((max(max_resta, min_base), max_base))

    return resultado


def float_horas_a_tupla_hms(nhoras):
    """Recibe un float y entrega una tupla con los float redondeados (hora, minutos, segundos)."""
    total_segundos = nhoras * 3600
    horas, resto = divmod(total_segundos, 3600)
    minutos, segundos = divmod(resto, 60)
    return round(horas), round(minutos), round(segundos)

def combina_dia_y_float_hora_en_dt(dia, float_hora):
    """Toma un objeto date y un float hora y devuelve ambos combinados en un """
    horas, minutos, segundos = float_horas_a_tupla_hms(float_hora)
    dt = datetime.datetime.combine(dia, datetime.time(horas, minutos, segundos))
    return dt

def fechahora_a_float_hora(fecha_hora):
    """
    Convierte un datetime (o una Serie de datetime) a un número float con la hora decimal.
    """
    if isinstance(fecha_hora, pd.Series):
        return fecha_hora.dt.hour + fecha_hora.dt.minute / 60 + fecha_hora.dt.second / 3600

    # Convertir numpy.datetime64 a Timestamp
    if isinstance(fecha_hora, np.datetime64):
        fecha_hora = pd.to_datetime(fecha_hora)

    return fecha_hora.hour + fecha_hora.minute / 60 + fecha_hora.second / 3600


def obtener_intervalos_dia(inicio: datetime.datetime, fin: datetime.datetime):
    """
    Divide un intervalo de tiempo en tramos diarios.

    Parámetros:
    - inicio (datetime.datetime): Fecha y hora de inicio del intervalo.
    - fin (datetime.datetime): Fecha y hora de término del intervalo.

    Retorna:
    - pd.DataFrame: Un DataFrame con una fila por día involucrado en el intervalo. Cada fila contiene:
        - 'dia_semana': Número del día de la semana (1 = lunes, ..., 7 = domingo).
        - 'dia': Fecha correspondiente al tramo.
        - 'hora_inicio': Hora de inicio del tramo expresada como número flotante (por ejemplo, 13.5 para 13:30).
        - 'hora_fin': Hora de término del tramo expresada como número flotante (24 si el término es medianoche).

    Notas:
    - Si el intervalo comienza y termina el mismo día, se retorna un único tramo.
    - Si el fin es exactamente a medianoche (00:00), ese día no se incluye como tramo.
    - Requiere que la función `fechahora_a_float_hora` esté definida para convertir datetime a hora flotante.
    """
    resultado = []
    dia_actual = inicio.date()
    ultimo_dia = fin.date()

    while dia_actual <= ultimo_dia:
        if dia_actual == inicio.date() and dia_actual == fin.date():
            dt_inicio = inicio
            dt_fin = fin
        elif dia_actual == inicio.date():
            dt_inicio = inicio
            dt_fin = datetime.datetime.combine(dia_actual + datetime.timedelta(days=1), datetime.time(0, 0))
        elif dia_actual == fin.date():
            if fin.time() == datetime.time(0, 0):
                break
            dt_inicio = datetime.datetime.combine(dia_actual, datetime.time(0, 0))
            dt_fin = fin
        else:
            dt_inicio = datetime.datetime.combine(dia_actual, datetime.time(0, 0))
            dt_fin = datetime.datetime.combine(dia_actual + datetime.timedelta(days=1), datetime.time(0, 0))

        hora_inicio = fechahora_a_float_hora(dt_inicio)
        hora_fin = fechahora_a_float_hora(dt_fin)
        if hora_fin == 0:
            hora_fin = 24

        resultado.append({
            'dia_semana': dt_inicio.weekday() + 1,
            'dia': dt_inicio.date(),
            'hora_inicio': hora_inicio,
            'hora_fin': hora_fin
        })

        dia_actual += datetime.timedelta(days=1)

    return pd.DataFrame(resultado)


def contar_dias_semana(fecha_inicio, fecha_fin, dia_semana) -> int:
    """
    Cuenta cuántas veces aparece un día de la semana entre dos fechas (inclusive).

    Parámetros:
    - fecha_inicio, fecha_fin: objetos datetime.date
    - dia_semana: int de 1 (lunes) a 7 (domingo)

    Retorna:
    - número de ocurrencias del día indicado
    """
    if fecha_inicio > fecha_fin:
        fecha_inicio, fecha_fin = fecha_fin, fecha_inicio

    # Día de la semana de la fecha de inicio
    dia_inicio = fecha_inicio.weekday()

    # Días hasta el primer día deseado (puede ser 0 si ya cae en ese día)
    dias_hasta_deseado = (dia_semana - 1 - dia_inicio) % 7
    primera_ocurrencia = fecha_inicio + datetime.timedelta(days=dias_hasta_deseado)

    if primera_ocurrencia > fecha_fin:
        return 0

    total_dias = (fecha_fin - primera_ocurrencia).days
    return 1 + total_dias // 7

####################################################
###                                              ###
###  Aquí defino funciones propias del proyecto  ###
###  que se usan en múltiples scripts            ###
###                                              ###
####################################################


def decide_compra(df_precios,cantidad_minima):
    """
    Función que toma un dataframe con valorizaciones de tipos de compra
    con restricciones de unidades por paquete y minimo de paquetes por compra
    y devuelve la cantidad a comprar y el precio asociado.

    La estrategia es gastar lo menos posible para conseguir la cantidad_minima
    """

    dict_ids_precios_a_pagar = {}
    for _,row in df_precios.iterrows():
        UPP, MPPC = row['unidades_por_paquete'], row['minimo_paquetes_por_compra']
        PPP = row['precio_por_paquete']
        paquetes_a_comprar = math.ceil(max([(cantidad_minima/UPP),MPPC]))
        precio_a_pagar = PPP * paquetes_a_comprar
        unidades_compradas = paquetes_a_comprar * UPP
        dict_ids_precios_a_pagar[row['id']] = {'precio_a_pagar':precio_a_pagar, 'unidades_a_comprar':unidades_compradas}
    
    id_precio_menor_gasto = min(dict_ids_precios_a_pagar, key=lambda k: dict_ids_precios_a_pagar[k]['precio_a_pagar'])
    precio_pagado = dict_ids_precios_a_pagar[id_precio_menor_gasto]['precio_a_pagar']
    unidades_compradas = dict_ids_precios_a_pagar[id_precio_menor_gasto]['unidades_a_comprar']
    precio_usado = precio_pagado * cantidad_minima / unidades_compradas
    return unidades_compradas, precio_pagado,precio_usado