import sqlite3
import pandas as pd


def dedupe_query_run_1(conn_func, tbl_nm):
    # Create temp table name
    temp_tbl_nm = "temp_" + tbl_nm

    # Delete old temp table
    query = """
    drop table if exists "{temp_tbl_nm_str}";
    """
    conn_func.execute(query.format(temp_tbl_nm_str=temp_tbl_nm))

    # Create new temp table
    query = """
    create table "{temp_tbl_nm_str}" as
    select 
        dirpath,
        filenames,
        file_path,
        hash,
        max(scantime) as scantime
    from 
        "{tbl_nm_str}"
    group by
        dirpath,
        filenames,
        file_path,
        hash;
    """
    conn_func.execute(query.format(tbl_nm_str=tbl_nm,
                                   temp_tbl_nm_str=temp_tbl_nm))

    # Delete old incomplete table
    query = """
    drop table if exists "{tbl_nm_str}";
    """
    conn_func.execute(query.format(tbl_nm_str=tbl_nm))

    # Recreate incomplete table
    query = """
    create table "{tbl_nm_str}" as
    select 
        *
    from
        "{temp_tbl_nm_str}";
    """
    conn_func.execute(query.format(tbl_nm_str=tbl_nm,
                                   temp_tbl_nm_str=temp_tbl_nm))

    # Delete temp table
    query = """
    drop table if exists "{temp_tbl_nm_str}";
    """
    conn_func.execute(query.format(temp_tbl_nm_str=temp_tbl_nm))

    # Commit Changes
    conn_func.commit()


def incomplete_table_query_run_1(conn_func, tbl_nm):
    # Query table
    query = """
    select 
        *
    from
        "{tbl_nm_str}";
    """
    return pd.read_sql_query(query.format(tbl_nm_str=tbl_nm), con=conn_func)


def delete_row_query_run_2(conn_func, tbl_nm_delete_func, tbl_nm_from_func, where_col_1, where_col_2):
    # Query to delete values from one table to another
    query = """delete from "{}" WHERE "{}" in (select "{}" from "{}")""".format(tbl_nm_delete_func,
                                                                                where_col_1,
                                                                                where_col_2,
                                                                                tbl_nm_from_func)
    # return query
    conn_func.execute(query)

    # Commit Changes
    conn_func.commit()


def drop_query_run_1(conn_func, tbl_nm_delete_func):
    # Query to delete values from one table to another
    query = """drop table if exists "{}";""".format(tbl_nm_delete_func)

    # return query
    conn_func.execute(query)

    # Commit Changes
    conn_func.commit()


def limit_to_scan_difference_query_run_1(conn_func, incmplt_tbl_func, cmplt_tbl_func, scn_tm_lmt):
    # Create temp table name
    temp_tbl_nm = "temp_scan_limit_" + incmplt_tbl_func

    # Delete old temp table
    query = """
    drop table if exists "{temp_tbl_nm_str}";
    """
    conn_func.execute(query.format(temp_tbl_nm_str=temp_tbl_nm))

    # Create new temp table
    query = """
    create table "{temp_tbl_nm_str}" as
    SELECT
        a.dirpath,
        a.filenames,
        a.file_path,
        a.hash,
        b.scantime,
        b.time_since_last_scan
    FROM
        "{incmplt_tbl_str}" a
    JOIN
        (SELECT 
            hash,
            scantime,
            time_since_last_scan
        FROM
            (SELECT
                a.hash,
                a.scantime,
                a.scantime - b.report_time as time_since_last_scan
            FROM
                (SELECT
                    a.hash,
                    max(scantime) as scantime
                FROM
                    "{incmplt_tbl_str}" a
                GROUP BY
                    a.hash)a
            LEFT JOIN
                (SELECT
                    b.hash,
                    max(report_time) as report_time
                FROM
                    "{cmplt_tbl_str}" b
                GROUP BY
                    b.hash) b
            ON
                a.hash = b.hash)
        WHERE
            time_since_last_scan IS NULL
                OR
            time_since_last_scan > {scn_tm_lmt_str}) b
    ON
        a.hash = b.hash
            AND
        a.scantime = b.scantime;
    """
    conn_func.execute(query.format(incmplt_tbl_str=incmplt_tbl_func,
                                   temp_tbl_nm_str=temp_tbl_nm,
                                   cmplt_tbl_str=cmplt_tbl_func,
                                   scn_tm_lmt_str=str(scn_tm_lmt)))

    # Delete old incomplete table
    query = """
    drop table if exists "{incmplt_tbl_str}";
    """
    conn_func.execute(query.format(incmplt_tbl_str=incmplt_tbl_func))

    # Recreate incomplete table
    query = """
    create table "{incmplt_tbl_str}" as
    select 
        *
    from
        "{temp_tbl_nm_str}";
    """
    conn_func.execute(query.format(incmplt_tbl_str=incmplt_tbl_func,
                                   temp_tbl_nm_str=temp_tbl_nm))

    # Delete temp table
    query = """
    drop table if exists "{temp_tbl_nm_str}";
    """
    conn_func.execute(query.format(temp_tbl_nm_str=temp_tbl_nm))

    # Commit Changes
    conn_func.commit()


def create_current_view_run_1(conn_func, view_tbl_nm, cmplt_tbl_func, scn_tm_lmt, crnt_tm):
    # Drop old view
    query = """
    drop view if exists "{view_tbl_nm_str}";
    """

    # return query
    conn_func.execute(query.format(
        view_tbl_nm_str=view_tbl_nm))

    # create new view
    query = """
    create view "{view_tbl_nm_str}" as
    SELECT
        b.*
    FROM
        (select
            hash,
            max(report_time) as last_report_time
        from
            "{cmplt_tbl_str}"
        group by
            hash
        ) a
    JOIN
        "{cmplt_tbl_str}" b
    ON
        a.hash = b.hash
            AND
        a.last_report_time = b.report_time
    WHERE
        a.last_report_time >= {crnt_tm_str} - {scn_tm_lmt_str};
    """

    # return query
    conn_func.execute(query.format(
        view_tbl_nm_str=view_tbl_nm,
        cmplt_tbl_str=cmplt_tbl_func,
        scn_tm_lmt_str=str(scn_tm_lmt),
        crnt_tm_str=str(crnt_tm)))

    # Commit Changes
    conn_func.commit()


def query_tbl_run_1(conn_func, tbl_nm):
    # Query table
    query = """
    select 
        *
    from
        "{tbl_nm_str}";
    """
    return pd.read_sql_query(query.format(tbl_nm_str=tbl_nm), con=conn_func)


def list_of_tables(conn_func):
    query = """
    SELECT 
        name 
    FROM 
        sqlite_master 
    WHERE 
        type='table';
    """
    return pd.read_sql_query(query, con=conn_func)['name'].to_list()
