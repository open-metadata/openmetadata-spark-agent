from sqlalchemy import create_engine, text

database_url = "mysql+pymysql://openmetadata_user:openmetadata_password@localhost:3306/openmetadata_db"

# Create an SQLAlchemy engine
engine = create_engine(database_url)


def execute_query(query, connection):
    sql_query = text(query)
    connection.execute(sql_query)


# Establish a connection
with engine.connect() as connection:
    # create tables and populate data
    execute_query(
        "create table IF NOT EXISTS employee(id int, name varchar(30))", connection
    )
    execute_query(
        "create table IF NOT EXISTS new_employee(id int, name varchar(30))", connection
    )
    execute_query(
        "insert into employee(id, name) values (1,'Mayur'),(2,'Shailesh'),(3,'Onkar'),(4,'Ashish');",
        connection,
    )
