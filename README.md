[![Build Status](https://img.shields.io/github/actions/workflow/status/fsantanar/operaiq_simulador/python-ci.yml?branch=main)](https://github.com/fsantanar/operaiq_simulador/actions)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![GitHub last commit](https://img.shields.io/github/last-commit/fsantanar/operaiq_simulador)
![Repo Size](https://img.shields.io/github/repo-size/fsantanar/operaiq_simulador)


# OperaIQ – Simulador y Generador de Base de Datos Operativa

**OperaIQ** es una herramienta para simular la operación de una empresa de servicios, generando una base de datos estructurada con reglas lógicas de negocio. Está diseñada para reproducir escenarios realistas y servir como base de análisis, reportería, visualización y pruebas de modelos de optimización.

---

## 📌 Características principales

- Generación reproducible de datos con lógica realista
- Modelado completo de trabajadores, insumos, clientes, servicios, roles, cotizaciones y más
- Simulación de flujo temporal de eventos con reglas predefinidas
- Exportación del modelo ERD como imagen
- Modularización del código con uso de `peewee` y configuración por `.env`

---

## 🧠 Estructura general del proyecto

```text
operaiq_simulador/
│
├── scripts/                # Scripts principales de ejecución paso a paso
├── src/                    # Módulos reutilizables: modelos, conexión, utils, constantes
├── contenido_tablas/       # CSV generados automáticamente
├── inputs/                 # Archivos .json con parámetros de entrada
├── logs/                   # Archivos de log del proceso
├── erd/                    # Imagen del modelo entidad-relación
├── docs/                   # Documentación detallada en PDF y DOCX
├── .env.example            # Variables de entorno de ejemplo
├── README.md               # Este archivo
└── requirements.txt        # Modulos de python necesarios
```

## 🖼️ Diagrama ERD
<img src="erd/diagrama_entidad_relacion.png" alt="ERD del Sistema" width="900">

## ⚙️ Requisitos

- Python 3.11 o superior

- Paquetes: ver requirements.txt

- No requiere una base de datos preexistente: toda la información se genera y simula localmente

- Graphviz (sistema): además del paquete graphviz de Python, se requiere tener instalado Graphviz en el sistema operativo para poder generar el diagrama ERD como imagen.

Puedes instalarlo con:

En Ubuntu/Debian:

```bash
sudo apt-get install graphviz
```

En Mac (con Homebrew):

```bash
brew install graphviz
```

En Windows: Descárgalo desde https://graphviz.org/download/ y asegúrate de agregarlo al PATH.

## 🚀 Cómo usar

### **1. Clona este repositorio:**

```bash
git clone https://github.com/fsantanar/operaiq_simulador.git
cd operaiq_simulador
```

### **2. Instala las dependencias:**

Asegúrate de tener Python 3.11 o superior y luego instala los paquetes requeridos:


```bash
pip install -r requirements.txt
```

### **3. Configura tus variables de entorno a partir de .env.example**

Crea un archivo .env basado en el archivo de ejemplo:

```bash
cp .env.example .env
```

Luego abre tu nuevo archivo .env y reemplaza los valores de ejemplo por los reales:

```env

# Base de datos del proyecto
DB_NAME=operaiq_simulador

# Usuario para que sea dueño de la nueva base de datos con su clave asociada.
# Si eliges un usuario nuevo se creará con la clave que indiques acá.
# Si quieres usar un usuario existente simplemente usa su nombre como DB_USER y se
# mantendrá su clave actual (los códigos no modifican usuarios existentes).
# Solo asegurate de que el usuario existente cuente con clave o que tu archivo pg_hba
# le permita conectarse a la base sin clave con una linea como:
# local   operaiq_simulador  usuario_existente              trust
DB_USER=usuario
DB_PASSWORD=clave_usuario

# Usuario administrador (por defecto llamado postgres) con permisos para crear bases de datos y usuarios.
# DB_ADMIN_PASSWORD debe llevar la clave de este usuario administrador.
# La base puede ser la que viene por defecto (generalmente llamada postgres) o cualquier a la que tenga
# acceso el usuario administrador
DB_ADMIN_USER=postgres
DB_ADMIN_PASSWORD=clave_superusuario
DB_ADMIN_DB=postgres

# Host y puerto de conexión
# Estos puedes dejarlos como están si no quieres personalizarlo
DB_HOST=localhost
DB_PORT=5432


# Finalmente cambia este boolean a true si quieres que las pruebas de GitHub actions
# incluyan los dos ultimos scripts que demoran alrededor de 10 minutos.
RUN_FINAL_SCRIPTS=true
```


### **4. Corre los scripts en orden desde scripts/db01_...py hasta scripts/db07_...py**

Ejecuta los scripts en orden (por ejemplo desde la carpeta base con)

```bash
python scripts/db01_crea_base_de_datos.py
python scripts/db02_crea_tablas.py
python scripts/db03_crea_erd.py
python scripts/db04_crea_csvs_para_tablas_base.py
python scripts/db05_llena_tablas_base.py
python scripts/db06_crea_instancias_tablas.py
python scripts/db07_revisa_cargas.py
```


## 📄 Documentación
Consulta los siguientes archivos en la carpeta docs/:

📘 [Documentación de la base de datos](docs/documentacion_base_de_datos.pdf)

📗 [Reglas y Simplificaciones](docs/reglas_y_simplificaciones.pdf)

## 📬 Contacto
Proyecto desarrollado por Felipe Santana
✉️ contacto: fsantanar@gmail.com

## 📝 Licencia
Este proyecto no cuenta actualmente con una licencia de uso. Para uso comercial o redistribución, contactar al autor
