import subprocess
import time
import os
from pathlib import Path
from dotenv import load_dotenv

# Carga el .env desde la carpeta base, sin importar desde dónde se corra el script
base_dir = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=base_dir / '.env')

# Leer variables del entorno
db_name = os.getenv('db_name')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
db_admin_user = os.getenv('db_admin_user')
db_admin_db = os.getenv('db_admin_db')

# Por ahora estas variables estan solo definidas en el entorno de GitHub no en el local
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Función auxiliar para construir argumentos psql

def psql_args():
    """Agrega las opciones y argumentos a usar en comandos psql"""
    args = ['psql','-h', db_host, '-p', db_port, '-U', db_admin_user, '-d', db_admin_db]
    return args

def crear_usuario(user, password):
    check_user_sql = f"SELECT 1 FROM pg_roles WHERE rolname='{user}';"
    create_user_sql = f"CREATE USER {user} WITH PASSWORD '{password}';"

    # Verificar si el usuario existe
    result = subprocess.run(psql_args() + ['-tAc', check_user_sql],capture_output=True, text=True)

    if result.stdout.strip() == '1':
        print(f"El usuario {user} ya existe. No se necesita crear.")
    else:
        subprocess.run( psql_args() + ['-c', create_user_sql], check=True)
        print(f"Usuario {user} creado exitosamente.")


def crear_base_datos(db_name, db_user):
    # Solicitar confirmación del usuario
    print(f"Estás a punto de borrar la base de datos '{db_name}' si existe.")
    print(f"Tienes 5 segundos para cancelar la operación... (Ctrl+C)")
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print("\nOperación cancelada por el usuario.")
        return

    # Borrar la base de datos si existe
    print(f"Eliminando la base de datos '{db_name}' si existe...")
    subprocess.run(psql_args() + ['-c', f"DROP DATABASE IF EXISTS {db_name};"],check=False)
    print(f"Base de datos '{db_name}' eliminada o no existía.")

    # Intentar crear la base de datos con un owner específico
    try:
        print(f"Creando la base de datos '{db_name}' con propietario '{db_user}'...")
        resultado = subprocess.run(psql_args() + ['-c', f"CREATE DATABASE {db_name} OWNER {db_user};"],
                                   text=True, capture_output=True, check=True)
        if resultado.returncode == 0:
            print(f"La base de datos '{db_name}' fue creada exitosamente con propietario '{db_user}'.")
        else:
            print(f"Hubo un error al crear la base de datos: {resultado.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar el comando: {e.stderr or e}")
        return

    # Configurar permisos en la base de datos
    print(f"Configurando permisos en la base de datos '{db_name}'...")
    try:
        revoke_public_sql = f"REVOKE CONNECT ON DATABASE {db_name} FROM PUBLIC;"
        grant_user_sql = f"GRANT CONNECT ON DATABASE {db_name} TO {db_user};"
        resultado_revoke = subprocess.run(psql_args() + ['-c', revoke_public_sql], text=True,
                                          capture_output=True, check=True)
        resultado_grant = subprocess.run(psql_args() + ['-c', grant_user_sql],
                                         text=True, capture_output=True, check=True)
        print(f"Permisos configurados correctamente. Acceso restringido a '{db_user}'.")
    except subprocess.CalledProcessError as e:        
        print(f"Error al configurar permisos: {e.stderr or e}")
        print(f"Resultado Revoke: {resultado_revoke}")
        print(f"Resultado Grant: {resultado_grant}")

    print()
    print("🔐 Nota importante para la autenticación de usuarios en PostgreSQL:")
    print("Por defecto, PostgreSQL puede permitir conexiones sin contraseña (modo 'peer' o 'trust').")
    print("Si realmente quieres exigir que se use contraseña al conectar a la base de datos,")
    print(f"debes agregar la siguiente línea al archivo `pg_hba.conf`:")
    print()
    print(f"  local   {db_name}   {db_user}   md5")
    print()
    print("🔄 Alternativamente, puedes usar 'all' para aplicarlo a todos los usuarios y bases:")
    print("  local   all         all         md5")
    print()
    print("⚠️ Si prefieres permitir conexiones sin contraseña durante el desarrollo local,")
    print("puedes dejar la configuración por defecto (peer/trust), pero **no es recomendable en producción**.")
    print()
    print("Después de modificar `pg_hba.conf`, reinicia PostgreSQL:")
    print("  - En macOS (Homebrew): brew services restart postgresql@15")
    print("  - En Linux (systemd):  sudo systemctl restart postgresql")
    print()
    print("✅ ¡Configuración completada!")
    print()


crear_usuario(db_user, db_password)
crear_base_datos(db_name, db_user)
