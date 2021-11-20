import sqlite3
import pandas as pd


def dedupe_query_run_1(conn, tbl_nm):
    # Create cursor
    cur = conn.cursor()

    # Create temp table name
    temp_tbl_nm = "temp_" + tbl_nm

    # Delete old temp table
    query = """
    drop table if exists "{temp_tbl_nm_str}";
    """
    conn.execute(query.format(temp_tbl_nm_str=temp_tbl_nm))

    # Create new temp table
    query = """
    create table "{temp_tbl_nm_str}" as
    select 
        dirpath,
        filenames,
        file_path,
        max(scantime) as scantime
    from 
        "{tbl_nm_str}"
    group by
        dirpath,
        filenames;
    """
    conn.execute(query.format(tbl_nm_str=tbl_nm, temp_tbl_nm_str=temp_tbl_nm))

    # Delete old incomplete table
    query = """
    drop table if exists "{tbl_nm_str}";
    """
    conn.execute(query.format(tbl_nm_str=tbl_nm))

    # Recreate incomplete table
    query = """
    create table "{tbl_nm_str}" as
    select 
        *
    from
        "{temp_tbl_nm_str}";
    """
    conn.execute(query.format(tbl_nm_str=tbl_nm, temp_tbl_nm_str=temp_tbl_nm))

    # Delete temp table
    query = """
    drop table if exists "{temp_tbl_nm_str}";
    """
    conn.execute(query.format(temp_tbl_nm_str=temp_tbl_nm))


def incomplete_table_query_run_1(conn, tbl_nm):
    # Query table
    query = """
    select 
        *
    from
        "{tbl_nm_str}";
    """
    return pd.read_sql_query(query.format(tbl_nm_str=tbl_nm), con=conn)

