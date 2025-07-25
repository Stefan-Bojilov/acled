from dagster import ConfigurableResource
import psycopg


class PostgreSQLResource(ConfigurableResource): 
    host: str 
    dbname: str 
    user: str 
    password: str

    def get_connection(self): 
        return psycopg.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
    
