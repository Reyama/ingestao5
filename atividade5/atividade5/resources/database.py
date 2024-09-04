from dagster import ConfigurableResource, EnvVar
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

class SQLAlchemyResource(ConfigurableResource):
    db_url: str 
    _engine: Engine = None
    
    def get_engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.db_url)
        return self._engine