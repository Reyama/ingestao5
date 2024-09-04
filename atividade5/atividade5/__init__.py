from dagster import Definitions, load_assets_from_modules, EnvVar
from .assets import bancos, empregados, reclamacoes
from .resources.database import SQLAlchemyResource

all_assets = load_assets_from_modules([bancos, empregados, reclamacoes])

sqlalchemy_resource = SQLAlchemyResource(db_url=f"postgresql://{EnvVar('DB_USER').get_value()}:{EnvVar('DB_PASS').get_value()}@{EnvVar('DB_HOST').get_value()}:{EnvVar('DB_PORT').get_value()}/{EnvVar('DATABASE').get_value()}")

defs = Definitions(
    assets=all_assets,
    resources={
        "db_connection": sqlalchemy_resource
    }
)
