import time
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.conexion import db
from src.modelos import (Roles, Trabajadores, DisponibilidadesTrabajadores, Clientes, Contactos,
                         TiposInsumo, TiposTrabajo, PreciosInsumos,PreciosTrabajos, TiposServicio,
                         Proyectos, MovimientosFinancieros, MovimientosRecurrentes, Insumos,
                         Consumos, Servicios, Cotizaciones, TiposServicioATiposTrabajo, Trabajos,
                         Asignaciones, RequerimientosTrabajadores, RequerimientosMateriales)



def resetear_tablas(base, modelos_a_crear, tiempo):
    print("⚠️  ADVERTENCIA: Esto eliminará TODAS las tablas y sus datos.")
    print("")
    print("Presiona Ctrl+C ahora si quieres cancelar.")
    print("")
    print(f"Esperando {tiempo} segundos antes de continuar...")

    try:
        time.sleep(tiempo)
    except KeyboardInterrupt:
        print("\n❌ Operación cancelada por el usuario.")
        return

    with base:
        base.drop_tables(modelos_a_crear)
        base.create_tables(modelos_a_crear)
        print("✅ Tablas eliminadas y recreadas con éxito.")


db.connect()

modelos = [Roles, Trabajadores, DisponibilidadesTrabajadores, Clientes, Contactos, TiposInsumo, TiposTrabajo, PreciosInsumos,
           PreciosTrabajos, TiposServicio, Proyectos, MovimientosFinancieros, MovimientosRecurrentes,
           Insumos, Consumos, Servicios, Cotizaciones, TiposServicioATiposTrabajo, Trabajos,
           Asignaciones, RequerimientosTrabajadores, RequerimientosMateriales]


resetear_tablas(db, modelos, 0)

db.close()

