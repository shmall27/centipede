
import os
from typing import Any, Dict, Union
from datetime import datetime
import duckdb
from pydantic import BaseModel
from .github import GitHubProfile
from contextlib import contextmanager


def pydantic_to_duckdb_type(python_type: Any) -> str:
    """Convert Pydantic/Python types to DuckDB types"""
    type_mapping = {
        str: 'VARCHAR',
        int: 'INTEGER',
        bool: 'BOOLEAN',
        datetime: 'TIMESTAMP',
        list: 'VARCHAR[]',  # Assuming list of strings for languages
        'HttpUrl': 'VARCHAR'  # Special case for Pydantic's HttpUrl
    }

    # Handle Optional types
    if hasattr(python_type, '__origin__'):
        if python_type.__origin__ is Union:
            python_type = python_type.__args__[0]  # Get the first type arg

    # Get the base type
    base_type = getattr(python_type, '__origin__', python_type)
    return type_mapping.get(base_type, 'VARCHAR')


def generate_create_table_sql(model: type[BaseModel], table_name: str) -> str:
    """Generate CREATE TABLE SQL from a Pydantic model"""
    # Start with auto-incrementing primary key using DuckDB syntax
    fields = [
        'id BIGINT PRIMARY KEY DEFAULT nextval(\'' + table_name + '_id_seq\')']

    # Add all model fields
    for name, field in model.model_fields.items():
        sql_type = pydantic_to_duckdb_type(field.annotation)
        nullable = field.is_required
        fields.append(f"{name} {sql_type}{'' if nullable else ' NULL'}")

    # Add timestamp for tracking
    fields.append("fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    # Create sequence for auto-incrementing ID
    create_sequence = f"""
        CREATE SEQUENCE IF NOT EXISTS {table_name}_id_seq;
    """

    # Create table
    create_table = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {','.join(fields)}
        );
    """

    return create_sequence + create_table


def model_to_dict(profile: GitHubProfile) -> Dict[str, Any]:
    """Convert Pydantic model to dict with proper type handling"""
    data = profile.model_dump()
    # Convert HttpUrl to string
    for field_name, field in profile.model_fields.items():
        if pydantic_to_duckdb_type(field.annotation) == 'VARCHAR':
            value = data.get(field_name)
            if value is not None:
                data[field_name] = str(value)
    return data


def init_database(con: duckdb.DuckDBPyConnection):
    """Initialize the DuckDB database with schema from Pydantic model"""
    sql = generate_create_table_sql(GitHubProfile, 'github_profiles')

    for statement in sql.split(';'):
        if statement.strip():  # Skip empty statements
            con.execute(statement)


def save_profile_to_db(con: duckdb.DuckDBPyConnection, profile: GitHubProfile):
    """Save a GitHubProfile to the database"""
    profile_data = model_to_dict(profile)
    all_fields = list(profile_data.keys())

    # Exclude ID and fetched_at from the INSERT
    fields = [f for f in all_fields if f not in ['id', 'fetched_at']]
    placeholders = [f'${i+1}' for i in range(len(fields))]
    values = [profile_data[field] for field in fields]

    sql = f"""
        INSERT INTO github_profiles (
            {', '.join(fields)}
        ) VALUES (
            {', '.join(placeholders)}
        )
    """

    con.execute(sql, values)


@contextmanager
def get_db_connection(db_path: str = 'hiring.db'):
    """Context manager for database connections"""
    con = duckdb.connect(db_path)
    try:
        yield con
    finally:
        con.close()


def init_database_if_needed(db_path: str = 'hiring.db'):
    """Initialize database if it doesn't exist"""
    if not os.path.exists(db_path):
        with get_db_connection(db_path) as con:
            init_database(con)


def export_to_csv(db_path: str, output_path: str):
    """
    Export the github_profiles table from DuckDB to a CSV file.

    Args:
        db_path (str): Path to the DuckDB database file
        output_path (str): Path where the CSV file should be saved

    Returns:
        bool: True if export was successful, False otherwise
    """
    try:
        with get_db_connection(db_path) as con:
            # Get all columns from the table
            columns_query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'github_profiles'
                ORDER BY ordinal_position;
            """
            columns = [row[0] for row in con.execute(columns_query).fetchall()]

            # Export data to CSV
            export_query = f"""
                COPY (
                    SELECT {', '.join(columns)}
                    FROM github_profiles
                    ORDER BY id
                ) TO '{output_path}'
                WITH (HEADER TRUE, DELIMITER ',', QUOTE '"');
            """
            con.execute(export_query)

            return True

    except Exception as e:
        print(f"Error exporting to CSV: {str(e)}")
        return False
