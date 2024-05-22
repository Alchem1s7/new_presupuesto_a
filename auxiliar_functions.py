import re
import gc
import pandas as pd
import csv
from io import StringIO

def optimize_columns(df):
        pattern = re.compile('aprobado|modificado|pre-comprometido|comprometido|devengado|saldo|ejercido|pagado')
        columns_to_convert = [col for col in df.columns if pattern.search(col)]

        for col in columns_to_convert:

            df[col] = df[col].str.replace(',', '').astype(float)
            df[col] = pd.to_numeric(df[col], errors='raise', downcast="float")

        return df


def map_columns_using_dict(dim_df, df, dim_cols_dict, df_cols_dict):

    dim_index_col = list(dim_cols_dict.keys())[0]
    dim_value_col = dim_cols_dict[dim_index_col]

    new_col_name =  list(df_cols_dict.keys())[0]
    map_in_col = df_cols_dict[new_col_name]


    dict_to_map = dim_df.set_index(dim_index_col)[dim_value_col].to_dict()

    new_series = df[map_in_col].map(dict_to_map)
    gc.collect()
    return new_series


def informe_de_gobierno(row, mapping_dicts):
    
    result = mapping_dicts['ff'].get(row['ff'])
    
    if result is None:
        result = mapping_dicts['cve_nue'].get(row['cve_nue'])
    
    if result is None:
        dict_nombre_srio = mapping_dicts["nue_sin_oya"]
        nombre_srio = dict_nombre_srio.get(row["nue_sin_oya"])
        result = mapping_dicts['nombre_dependencia'].get(nombre_srio)

    if result is None:
        extracted_char = row['cve_a/s']
        result = mapping_dicts['cve_a/s'].get(extracted_char)
    
    if result is None:
        result = mapping_dicts["informe_cc_hist"].get(row["cve_nue"])
    
    return result


def rename_numeric_columns(new_df_cols):
    
    name_mapping = {}
    conceptos = [
        "aprobado", "modificado", "pre_comprometido", "comprometido",
        "devengado", "saldo", "ejercido", "pagado", "pre_modificado"
    ]

    for column in new_df_cols:
        
        # Check if the column has any concept:
        if any(concepto in column for concepto in conceptos):
            original_col = column  
            
            if "pre_" in column:
                column = column.replace("pre_", "pre-")
            
            # Split the column name and re organize
            col_parts = column.split("_")

            if len(col_parts) == 2:
                new_name = col_parts[1] + "_" + col_parts[0]
            elif len(col_parts) > 2:
                new_name = "_".join([col_parts[1], col_parts[0]] + col_parts[2:])
            else:
                new_name = column  
            
            # Store the new name belonging to the original one
            name_mapping[original_col] = new_name
        else:
            # If we have non-numeric cols (Those who doesn't have conceptos)
            # Then we save it like it is
            name_mapping[column] = column
    
    return name_mapping



def psql_insert_copy(table, conn, keys, data_iter):

    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)