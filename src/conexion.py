import os
from dotenv import load_dotenv
from peewee import PostgresqlDatabase, Model
from pathlib import Path

# Carga el .env desde la carpeta base, sin importar desde dónde se corra el script
base_dir = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=base_dir / '.env')

db_name = os.getenv('db_name')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')

db = PostgresqlDatabase(db_name, user=db_user, password=db_password, host='localhost')

class BaseModel(Model):
    class Meta:
        database = db

