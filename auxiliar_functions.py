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


def map_columns_using_dict(index_df, target_df, dim_cols_dict, target_cols_dict):

    # Dimension dataframe columns
    dim_index_col = next(iter(dim_cols_dict))
    value_to_map_col = dim_cols_dict[dim_index_col]
    # Create a dictionary to use in the mapping
    dict_to_map = index_df.set_index(dim_index_col)[value_to_map_col].to_dict()

    # Column names in target dataframe
    name_in_df = next(iter(target_cols_dict))
    map_in_col = target_cols_dict[name_in_df]

    # Map the values in the target dataframe
    new_series = target_df[map_in_col].map(dict_to_map)
    gc.collect()

    return new_series

# Define the improved mapping function
def map_columns_using_dict_v2(
        index_df: pd.DataFrame, 
        index_cols: tuple[str, str], 
        target_df: pd.DataFrame, 
        target_col: str
    ) -> pd.Series:
    """
    Maps values from one DataFrame to another using a dictionary.

    Args:
        index_df: DataFrame containing the mapping columns.
        index_cols: Tuple of (index_column, value_column) in index_df.
        target_df: DataFrame containing the target column to map values to.
        target_col: Name of the column in target_df to map values into.

    Returns:
        A pandas Series with the mapped values.
    """
    # Input validation
    if not isinstance(index_cols, tuple) or len(index_cols) != 2:
        raise ValueError("index_cols must be a tuple of two column names.")
    
    index_col, value_col = index_cols
    if index_col not in index_df.columns or value_col not in index_df.columns:
        raise ValueError(f"Columns {index_col} or {value_col} not found in index_df.")
    
    if target_col not in target_df.columns:
        raise ValueError(f"Column {target_col} not found in target_df.")

    # Create mapping dictionary
    mapping_dict = index_df.set_index(index_col)[value_col].to_dict()

    # Map values
    new_series = target_df[target_col].map(mapping_dict)
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