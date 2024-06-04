# Author: Francisco Daniel Lugo
# Import libraries
import pandas as pd
import os
import re
import time
import gc
import logging

from sqlalchemy import (
    create_engine, 
    MetaData, 
    Table, 
    Column, 
    Integer, 
    String, 
    Float, 
    DateTime, 
    ForeignKey, 
    text
)
from auxiliar_functions import (
    optimize_columns,
    map_columns_using_dict,
    informe_de_gobierno,
    rename_numeric_columns,
    psql_insert_copy
)
logging.basicConfig(level=logging.INFO)

# Functions for the ETL:

def connect_to_data_sources():
    
    # Main paths
    project_path = "/app/project_folder"
    #postgres_pass = input("Hello. Please enter the db password: \n")
    main_sources_path = os.path.join(project_path, "main_sources")
    hist_path = os.path.join(main_sources_path, "historic_data_2018_2023.parquet")
    new_path = os.path.join(main_sources_path, "update.csv")
    dimensions_path = os.path.join(project_path, "dimension_sources")
    logging.info("\nStarting: Connecting to data sources...")

    # Dimension paths
    rules_path = os.path.join(dimensions_path, "reglas_transicion.csv")
    clasif_urg_path = os.path.join(dimensions_path, "clasificacion_urg.txt")
    estructura_ff_path = os.path.join(dimensions_path, "estructura_ff.csv")
    nue_path = os.path.join(dimensions_path, "nue.csv")
    desc_prep_path = os.path.join(dimensions_path, "desc_prep.csv")
    clasif_og_path = os.path.join(dimensions_path, "clasif_og.csv")
    nup_path = os.path.join(dimensions_path, "nup.csv")
    nue_new_path = os.path.join(dimensions_path, "nue_dimension_new.csv")
    clas_admin_path = os.path.join(dimensions_path, "clas_admin.csv")
    clasiff_path = os.path.join(dimensions_path, "clasif_ff.csv")
    informe_path = os.path.join(dimensions_path, "informe_de_gobierno.csv")
    informe_cc_hist_path = os.path.join(dimensions_path, "cc_informe.csv")
    rubro_path = os.path.join(dimensions_path, "dim_rubro_nup.csv")

    # data base

    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    dbname = os.getenv('DB_NAME')
    port = os.getenv('DB_PORT') 

    # URL de conexión
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    

    # Gather all paths in one dict
    connections_dict = {
        "main":main_sources_path,
        "rules": rules_path,
        "clas_urg": clasif_urg_path,
        "estructuraff": estructura_ff_path,
        "nue": nue_path,
        "desc_prep": desc_prep_path,
        "clas_og": clasif_og_path,
        "nup": nup_path,
        "nue_new": nue_new_path,
        "hist": hist_path,
        "new": new_path,
        "clas_admin":clas_admin_path,
        "clasiff":clasiff_path,
        "url_engine":url,
        "informe":informe_path,
        "informe_cc_hist":informe_cc_hist_path,
        "rubro_nup":rubro_path
    }

    logging.info("Finished: Connecting to data sources")
    return connections_dict


def main_data_consolidation(connections_dict:dict):

    logging.info("\nStarting: Loading big tables...")
    # Historical data
    hist_df = pd.read_parquet(connections_dict["hist"])
    hist_df.columns = [i.lower().replace(" ","_").replace(".","_") for i in hist_df.columns]
    hist_df.rename(
        columns={
            "proyecto_2":"rubro_nup",
            "proyecto":"nombre_nup"
        }, inplace=True
    )

    # New data version
    new_df = pd.read_csv(connections_dict["new"], dtype=str)
    new_df = new_df.iloc[:-1,:].copy()
    new_df.columns = [col.lower().replace(" ", "_").replace(".","_") for col in new_df.columns]
    new_df.rename(columns={"origen":"origen_ff"}, inplace=True)

    # Optimize the new dataframe
    new_df = optimize_columns(new_df)

    # Free memory
    gc.collect()
    logging.info("Finished: Loading big tables")

    return hist_df, new_df

def columns_expander(rules_df, new_df):
    logging.info("\nStarting: Working with columns expansion...")
    # Rules for the columns that are the combination of two columns
    two_cols_rules_df = rules_df[rules_df.Filtros == 1].reset_index(drop=True).copy()
    # Split the column and append those as new columns
    two_cols_rules_df[["new_col1","new_col2"]] = two_cols_rules_df["Nueva base"].str.split(",", expand=True)

    
    two_cols_rules_df["Antigua"] = two_cols_rules_df["Antigua"].astype(str).str.lower().replace(r"[.\s]", "_", regex=True)
    two_cols_rules_df["new_col1"] = two_cols_rules_df["new_col1"].astype(str).str.lower().str.strip().replace(r"[.\s]", "_", regex=True)
    two_cols_rules_df["new_col2"] = two_cols_rules_df["new_col2"].astype(str).str.lower().str.strip().replace(r"[.\s]", "_", regex=True)
    
    # Check all the columns from two_cols_rules_df are present in new_df
    two_cols_rules_set = set(two_cols_rules_df.new_col1.values) | set(two_cols_rules_df.new_col2.values)
    
    try:
        all_cols_new_set = set(new_df.columns)
        if len(two_cols_rules_set - all_cols_new_set) != 0:

            raise Exception("The pair columns in the transformation df doesn't match the cols from the new_df")
    except Exception as e:

        logging.info(e)
    
    # Create a dict to map names
    dict_pair_cols = (
        two_cols_rules_df
        .set_index('Antigua')
        .apply(lambda row: [row['new_col1'], row['new_col2']], axis=1)
        .to_dict()
    )

    # Create the new columns in new_df
    # Specific case for finalidad column:
    new_df["cfu_n1"] = new_df["cfu_n1"] + "00" # We do this to match the data in historic dataframe
    new_columns = {}
    for old, pair in dict_pair_cols.items():
        try:
            if old in ["concepto", "finalidad"]:
                old = old + "1"
            new_columns[old] = new_df[pair[0]].astype(str).replace(r'\.0+', "", regex=True) + " " + new_df[pair[1]].astype(str)
        except KeyError as e:
            logging.info(f"Error: {e}. Column pair: {pair}")
            raise
    
    new_df = pd.concat([new_df, pd.DataFrame(new_columns)], axis=1)
    
    gc.collect()
    logging.info("Finished: Working with columns expansion")
    return new_df
    


def change_columns_names(rules_df, new_df, hist_df):

    logging.info("\nStarting: Changing column names...")
    names_df = rules_df[rules_df.Filtros == 0].copy()

    names_df.loc[:,"Nueva base"] = names_df["Nueva base"].str.lower().replace(r"[.\s]", "_", regex=True)
    names_df.loc[:,"Antigua"] = names_df["Antigua"].str.lower().replace(r"[.\s]", "_", regex=True)

    # Check that all the columns in the names_df match with the columns of the dataframes
    # create sets for the colum names
    new_names_set = set(names_df["Nueva base"].values)
    old_names_set = set(names_df["Antigua"].values)

    # The same for the columns in both dataframes:
    all_cols_new_set = set(new_df.columns)
    all_cols_old_set = set(hist_df.columns)

    try:
        # Check that all column names of the names_df are present in new_df
        if len(new_names_set - all_cols_new_set) != 0:
            raise Exception("Las columnas de la nueva base no cuadran con la tabla de reglas.")

        # Check that all column names of the names_df are present in hist_df
        if len(old_names_set - all_cols_old_set) != 0:
            raise Exception("Las columnas de la base histórica no cuadran con la tabla de reglas.")
            
    except Exception as e:
        logging.info(e)

    
    # Create a dict to map the col names
    dict_to_map_column_names = names_df.set_index("Nueva base")["Antigua"].to_dict()

    to_append_colnames = []

    # Iterate over each column in new_df
    for col in new_df.columns:
        # Check if the column name exists in the dictionary mapping
        if col in dict_to_map_column_names:
            # If the column name is a key in the dictionary, append the corresponding value (new name)
            to_append_colnames.append(dict_to_map_column_names[col])
        else:
            # If the column name is not in the dictionary, keep the original name
            to_append_colnames.append(col)

    # Set the new column names
    new_df.columns = to_append_colnames

    gc.collect()
    logging.info("Finished: Changing column names")
    return new_df


def mapping_columns(new_df, connections_dict):
    
    logging.info("\nStarting: Mapping columns with external data...")
    
    # Load of external data sources to map columns
    external_data_dict = {
        "dim_clasif_urg" : pd.read_csv(connections_dict["clas_urg"], dtype=str, sep="\t", usecols=["CVE U.R.G","ENTIDAD"]),
        "dim_ff" : pd.read_csv(connections_dict["estructuraff"], dtype=str, usecols=["CVE ORIGEN","ORIGEN","CVE RAMO","RAMO2"]),
        "dim_nue" : pd.read_csv(connections_dict["nue"], dtype=str, usecols=["Depto.","Agrupa dep","DESCRIPCION","Agrupado en:"]),
        "new_dim_nue" : pd.read_csv(connections_dict["nue_new"], usecols=["NUE5","Afectable","DEP. EJECUTORA"]),
        "dim_prep" : pd.read_csv(connections_dict["desc_prep"], dtype=str, usecols=["Agrupador","Sector","Nombre reporte Srio."]),
        "dim_clasf_og" : pd.read_csv(connections_dict["clas_og"], dtype=str, usecols=["COG2","CONCEPTO EF"]),
        "dim_nup" : pd.read_csv(connections_dict["nup"], dtype=str, usecols=["Clave Rubro","Rubro","Clave NUP","NUP"]),
        "clas_admin_dim" : pd.read_csv(connections_dict["clas_admin"],dtype=str, usecols=["CLAVE","ENTIDAD"]),
        "clasifff_dim" : pd.read_csv(connections_dict["clasiff"], dtype=str, usecols=["FUENTE DE FINANCIAMIENTO","Nombre FF SIAFEQ"]),
        "informe_dim" : pd.read_csv(connections_dict["informe"], dtype=str).dropna().reset_index(drop=True),
        "informe_cc_hist" : pd.read_csv(connections_dict["informe_cc_hist"], dtype=str),
        "rubro_nup_dim":pd.read_csv(connections_dict["rubro_nup"], dtype=str)
    }

    
    # Transformations using a dictionary
    independient_columns_dict = {}

    # Federal/estatal
    federal_estatal_mask = new_df.conac2.str.lower().str.contains("federal", na=False)
    independient_columns_dict["federal/estatal"] = federal_estatal_mask.map({True:"FEDERAL", False:"ESTATAL"})

    # cve año
    independient_columns_dict["cve_año"] = new_df["año_ff"].str[-2:]
    
    #cve ramo
    independient_columns_dict["cve_ramo"] = new_df["cff_n4"].str[-2:]

    # cve fondo
    independient_columns_dict["cve_fondo"] = new_df.cff_n5.str[-3:]

    # cve origen
    independient_columns_dict["cve_origen"] = new_df.ff.str[-1]

    # estatus
    pe9_mask = new_df.cve_prog.str.lower().str.startswith("pe9")
    independient_columns_dict["estatus"] = pe9_mask.map({True:"NO ASIGNADO",False:"ASIGNADO"})

    # dirección

    first_dict_direccion = external_data_dict["dim_nue"].set_index("Depto.")["Agrupado en:"].to_dict()
    second_dict_direccion = external_data_dict["dim_nue"].set_index("Depto.")["DESCRIPCION"].to_dict()

    independient_columns_dict["dirección"] = new_df["cve_nue"].map(first_dict_direccion)
    independient_columns_dict["dirección"] = independient_columns_dict["dirección"].map(second_dict_direccion)

    # entidad gs
    mask_entidadgs = new_df.cadena_siafeq.str.startswith("03")
    independient_columns_dict["entidad_gs"] = new_df.cve_adm.copy()
    independient_columns_dict["entidad_gs"].loc[mask_entidadgs] = "OBRAS Y ACCIONES"

    dict_to_map_entidad_gs = external_data_dict["clas_admin_dim"].set_index("CLAVE")["ENTIDAD"].to_dict()
    independient_columns_dict["entidad_gs"].loc[~mask_entidadgs] = independient_columns_dict["entidad_gs"].map(dict_to_map_entidad_gs)

    #rubro
    mask_cadena_siafeq = new_df.cadena_siafeq.str.startswith("03")
    mask_cogn2 = new_df.cog_n2.str.startswith("45")

    independient_columns_dict["rubro"] = pd.Series(pd.NA, index=new_df.index)
    independient_columns_dict["rubro"].loc[mask_cadena_siafeq] = "OBRAS Y ACCIONES"
    independient_columns_dict["rubro"].loc[mask_cogn2] = "JUBILADOS Y PENSIONADOS"

    dict_to_map_rubro = external_data_dict["dim_nup"].set_index("Clave Rubro")["Rubro"].to_dict()
    independient_columns_dict["rubro"].loc[~mask_cadena_siafeq & ~mask_cogn2] = new_df.cve_nup.str[2].map(dict_to_map_rubro)


    # año
    independient_columns_dict["año"] = pd.Series("2024", index=new_df.index)

    
    # entidad
    independient_columns_dict["entidad_"] = map_columns_using_dict(
        external_data_dict["dim_clasif_urg"], 
        new_df,
        {"CVE U.R.G":"ENTIDAD"},
        {"entidad_":"cve_urg"}
    )

    # nombre nue
    independient_columns_dict["nombre_nue"] = map_columns_using_dict(
        external_data_dict["dim_nue"], 
        new_df,
        {"Depto.":"Agrupa dep"},
        {"nombre_nue":"cve_nue"}
    )

    # nue sin oya
    independient_columns_dict["nue_sin_oya"] = map_columns_using_dict(
        external_data_dict["new_dim_nue"], 
        new_df,
        {"NUE5":"DEP. EJECUTORA"},
        {"nue_sin_oya":"cve_nue"}
    )
    
    if independient_columns_dict["nue_sin_oya"].hasnans:

        nues_without_oya = new_df.loc[independient_columns_dict["nue_sin_oya"].isna(), "cve_nue"].unique()
        
        for nue in nues_without_oya:

            aff = external_data_dict["new_dim_nue"][external_data_dict["new_dim_nue"]["NUE5"] == nue]["Afectable"].values[0]
            dep = external_data_dict["new_dim_nue"][external_data_dict["new_dim_nue"]["NUE5"] == aff]["DEP. EJECUTORA"].values[0]
            #new_df.loc[(new_df.nue_sin_oya.isna()) & (new_df.cve_nue == nue), "nue_sin_oya"] = dep
            independient_columns_dict["nue_sin_oya"].loc[(independient_columns_dict["nue_sin_oya"].isna()) & (new_df["cve_nue"] == nue)] = dep

    # concepto ef
    independient_columns_dict["concepto_ef"] = map_columns_using_dict(
        external_data_dict["dim_clasf_og"], 
        new_df,
        {"COG2":"CONCEPTO EF"},
        {"concepto_ef":"cog_n2"}
    )

    # proyecto 3
    independient_columns_dict["rubro_nup"] = map_columns_using_dict(
        external_data_dict["rubro_nup_dim"],
        new_df,
        {"NUP":"PROYECTO 2"},
        {"rubro_nup":"cve_nup"}
    )
    independient_columns_dict["rubro_nup"] = independient_columns_dict["rubro_nup"].str.strip()

    # proyecto new
    independient_columns_dict["proyecto_new"] = map_columns_using_dict(
        external_data_dict["dim_nup"],
        new_df,
        {"Clave NUP":"NUP"},
        {"proyecto_new":"cve_nup"}
    )
    independient_columns_dict["proyecto_new"].loc[independient_columns_dict["proyecto_new"].isna()] = independient_columns_dict["rubro_nup"]
    independient_columns_dict["proyecto_new"] = independient_columns_dict["proyecto_new"].str.strip()

    # nombre ff
    independient_columns_dict["nombre_ff"] = map_columns_using_dict(
        external_data_dict["clasifff_dim"],
        new_df,
        {"FUENTE DE FINANCIAMIENTO":"Nombre FF SIAFEQ"},
        {"nombre_ff":"ff"}
    )

    independient_cols_df = pd.DataFrame(independient_columns_dict)
    new_df = pd.concat([new_df, independient_cols_df], axis=1)

    # Second dataframe concatenation
    # We use the same approach but now, with the dependent columns ------------------------------------

    dependent_columns_dict = {}

     # origen ramo
    dependent_columns_dict["ramo2"] = map_columns_using_dict(
        external_data_dict["dim_ff"], 
        new_df,
        {"CVE RAMO":"RAMO2"},
        {"ramo2":"cve_ramo"}
    )

    # origen 
    dependent_columns_dict["origen1"] = map_columns_using_dict(
        external_data_dict["dim_ff"], 
        new_df,
        {"CVE ORIGEN":"ORIGEN"},
        {"origen1":"cve_origen"}
    )

    # Sector
    sector_mask = new_df.cve_nue.str.startswith("PE9")
    sector_series = pd.Series(pd.NA, index=new_df.index)
    sector_series.loc[sector_mask] = "BOLSA"

    dict_to_map_nue_sector = external_data_dict["dim_prep"].set_index("Agrupador")["Sector"].to_dict()
    sector_series.loc[~sector_mask] = new_df["nue_sin_oya"].map(dict_to_map_nue_sector)
    dependent_columns_dict["sector1"] = sector_series

    # Nombre dependencia
    #dependent_columns_dict["nombre_dependencia"] = map_columns_using_dict(
    #    external_data_dict["dim_prep"], 
    #    new_df,
    #    {"Agrupador":"Nombre reporte Srio."},
    #    {"nombre_dependencia":"nombre_nue"}
    #)
    
    # Informe
    mapping_dicts = {
        'ff': external_data_dict["informe_dim"].iloc[86:104].set_index('Clasificador')['Informe'].to_dict(), 
        'cve_nue': external_data_dict["informe_dim"].iloc[104:].set_index('Clasificador')['Informe'].to_dict(),
        'nue_sin_oya': external_data_dict["dim_prep"].set_index("Agrupador")["Nombre reporte Srio."].to_dict(),
        'nombre_dependencia': external_data_dict["informe_dim"].set_index('Clasificador')['Informe'].to_dict(),
        'cve_a/s': external_data_dict["informe_dim"].iloc[84:86].set_index('Clasificador')['Informe'].to_dict(),
        "informe_cc_hist" : external_data_dict["informe_cc_hist"].set_index('CC')['Informe'].to_dict()
    }

    dependent_columns_dict['informe'] = new_df.apply(informe_de_gobierno, axis=1, mapping_dicts=mapping_dicts)
    dependent_columns_dict["informe"] = dependent_columns_dict["informe"].apply(lambda x: x.split(".")[1] if len(x.split(".")) > 1 else x)

    # concatenate new columns all at once
    dependent_columns_df = pd.DataFrame(dependent_columns_dict)
    new_df = pd.concat([new_df, dependent_columns_df], axis=1)

    gc.collect()
    logging.info("Finished: Mapping columns with external data")
    return new_df, external_data_dict
    
    
def consolidate_final_df(new_df, hist_df):

    logging.info("\nStarting: Melting dataframe...")

    
    change_new_column_names = {

        "entidad_": "ENTIDAD 2",
        "federal/estatal":"FEDERAL / ESTATAL",
        "ramo1": "RAMO",
        "ramo2": "ORIGEN RAMO",
        "origen1": "ORIGEN",
        "sector1": "SECTOR",
        "proyecto_new": "NOMBRE NUP",
        "finalidad1":"finalidad",
        "concepto1":"concepto"
    }
    
    new_df.drop(columns=["sector","finalidad","concepto"], inplace=True)

    # Change the column names of the columns created by mapping
    # We change the column names by the old ones
    new_names_list = []

    for col in new_df.columns:

        # If we match the column name in the dict keys:
        if col in change_new_column_names:
            
            old_column_name = change_new_column_names[col].lower().replace(" ","_").replace(".","_")
            new_names_list.append(old_column_name)
        else:
            new_names_list.append(col)

    # Stablish the new column names
    new_df.columns = new_names_list
    # Change column names of numeric columns
    name_mapping = rename_numeric_columns(new_names_list)
    # Stablish the new column names for second time
    # By doing this, the numeric columns in new_df and hist_df can match
    new_df.rename(columns=name_mapping, inplace=True)

    extra_columns_for_new_df = [
        col for col in new_df.columns 
        if ("pagado" in col or "ejercido" in col or "pre-modificado" in col) 
        and ("acum" not in col) 
    ] + ["origen_ff","dependencia"] 

    cols_for_new_df = list(hist_df.columns) + extra_columns_for_new_df 

    # Filter empty values
    # Before doing melt delete empty records in both dataframes
    numeric_pattern = 'aprobado|modificado|pre-comprometido|comprometido|devengado|saldo|ejercido|pagado|pre-modificado'
    numeric_columns = [col for col in new_df.columns if re.search(numeric_pattern, col)]
    new_df = new_df[new_df[numeric_columns].apply(lambda row: row.notna().any() and row.sum() != 0, axis=1)]

    numeric_columns = [col for col in hist_df.columns if re.search(numeric_pattern, col)]
    hist_df = hist_df[hist_df[numeric_columns].apply(lambda row: row.notna().any() and row.sum() != 0, axis=1)]
    
    
    # Concatenate both dataframes
    full_df = pd.concat(
        [hist_df, new_df[cols_for_new_df]],
        ignore_index=True
    )
    
    # Create a melt dataframe
    # The id vars are the non-numeric columns
    pattern = re.compile(numeric_pattern)
    id_vars = [col for col in full_df.columns if not pattern.search(col)]
    value_vars = list(set(full_df.columns) - set(id_vars))

    df_melted = full_df.melt(
        id_vars=id_vars,
        value_vars=value_vars, 
        var_name='mes_y_momento', 
        value_name='cantidad'
    )
    del full_df

    # More transformations in the melted df
    df_melted[['mes', 'momento']] = df_melted['mes_y_momento'].str.split('_', expand=True)

    dict_to_map_months = {
        "enero":"01",
        "febrero":"02",
        "marzo":"03",
        "abril":"04",
        "mayo":"05",
        "junio":"06",
        "julio":"07",
        "agosto":"08",
        "septiembre":"09",
        "octubre":"10",
        "noviembre":"11",
        "diciembre":"12"
    }
    
    df_melted["cantidad"] = pd.to_numeric(df_melted["cantidad"], errors="raise")
    df_melted = df_melted[(df_melted.cantidad != 0) & (df_melted.cantidad.notna())].copy()
    df_melted["año"] = pd.to_numeric(df_melted["año"], errors="raise")

    df_melted["fecha"] = pd.NA
    df_melted["mes"] = df_melted["mes"].str.strip()
    df_melted["fecha"] = "01/" + df_melted["mes"].map(dict_to_map_months) + "/" + df_melted["año"].astype(str)
    df_melted["fecha"] = pd.to_datetime(df_melted["fecha"], dayfirst=True)
    
    df_melted.loc[df_melted['sector'].str.startswith("SECTOR"), "sector"] = df_melted["sector"].replace("SECTOR", "", regex=True).str.strip()
    df_melted.loc[df_melted['dependencia'].str.startswith("SECTOR",na=False), "dependencia"] = df_melted["dependencia"].replace("SECTOR", "", regex=True).str.strip()

    dict_to_replace_sector = {
        "Desarrollo Urbano Y Obras Públicas": "Urbano y OP",
        "De Planeación Y Participación Ciudadana":"Plan y Part Ciudadana",
        "De La Juventud": "Juventud"
    }
    df_melted["sector"] = df_melted["sector"].replace(dict_to_replace_sector)

    for column in ["part__genérica","part__específica","capítulo","concepto"]:
        df_melted[column] = df_melted[column].str.replace(r'(?<=\d)\.\s+', " ", regex=True)
    
    df_melted.drop(columns=["mes", "mes_y_momento"], inplace=True)
    
    gc.collect()
    logging.info("Finishing: Melting dataframe")
    return df_melted


def dimensional_creator(df_melted):
    logging.info("\nStarting: Creation of dimensional dataframes...")
    
    dim_defs = {
        "dim_tipogasto": ['cve_cog','capítulo','concepto','part__genérica','part__específica','cve_tg','tipo_de_gasto','rubro'],
        "dim_fuente": [
            'ff','cve_conac1','conac1','cve_conac2','conac2','federal_/_estatal','cve_año','año_ff','cve_ramo','ramo',
            'origen_ramo','cve_fondo','fondo','nombre_ff','cve_origen','origen','cve_ltp','ltp',"origen_ff"],
        "dim_programa": ['cve_prog','prog','cve_fun','finalidad','función','sub__función','cve_a/s','a/s','cve_pedq','pedq'],
        "dim_entidad": ['cve_urg', 'urg', 'cve_adm', 'adm', 'entidad', 'entidad_2', 'entidad_gs'],
        "dim_sector": ['cve_nue', 'estatus', 'nombre_nue', 'nue_sin_oya', 'sector', 'dirección',"dependencia"],
        "dim_proyecto": ["cve_nup","nombre_nup","rubro_nup","concepto_ef","informe"]
    }
    
    # dim_momento
    unique_momento = df_melted["momento"].unique()

    dict_momento = {
        momento: id_ for momento, id_ in zip(
            unique_momento, 
            range(1, len(unique_momento) + 1)
        )
    }

    dim_tables_dict = {"dim_momento": pd.DataFrame(dict_momento.items(), columns=["momento", "id_momento"])}
    df_melted["id_momento"] = df_melted["momento"].map(dict_momento)
    df_melted.drop(columns=["momento"], inplace=True)

    df_melted = df_melted.apply(lambda col: col.str.strip().str.title() if col.dtype == 'object' else col)
    # Bucle principal
    for dim_name, cols in dim_defs.items():

        logging.info(f"Iniciando: {dim_name}")
        dim_table = df_melted[cols].drop_duplicates().reset_index(drop=True)


        # Tabla hash para asignar IDs enteros
        id_map = {tuple(row): i + 1 for i, row in dim_table.iterrows()}
        id_col = f"id_{dim_name.split('_')[1]}"  

        # Crear la tabla dimensional con IDs enteros
        dim_table[id_col] = dim_table.apply(tuple, axis=1).map(id_map)
        dim_tables_dict[dim_name] = dim_table[[id_col] + cols]  

        # Mapeo de IDs al DataFrame original
        df_melted[id_col] = df_melted[cols].apply(tuple, axis=1).map(id_map)
        df_melted.drop(columns=cols, inplace=True)

        logging.info(f"Finalizado: {dim_name}")
    
    logging.info("Finalizado: Creación de dataframes dimensionales")
    gc.collect()
    return dim_tables_dict, df_melted


def get_tables(engine):
    """Fetches and returns a sorted list of tables from the database."""
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return metadata.sorted_tables


def drop_all_tables(conn_dict):
    logging.info("Starting: Dropping all tables in the database...")
    try:
        engine_url = conn_dict['url_engine']
        engine = create_engine(engine_url)

        sorted_tables = get_tables(engine)
        if not sorted_tables:
            logging.info("No tables found to drop.")
            return engine

        for table in reversed(sorted_tables):
            logging.info(f"Dropping table: {table.name}")
            try:
                table.drop(engine, checkfirst=True)
            except Exception as e:
                logging.error(f"Error dropping table {table.name}: {e}")
        
        # Final verification
        sorted_tables = get_tables(engine)
        if sorted_tables:
            logging.error("Not all tables were dropped successfully.")
            raise Exception("Table drop operation failed.")
        else:
            logging.info("All tables have been dropped successfully.")

        return engine

    except Exception as e:
        logging.critical(f"An error occurred: {e}")
        raise 


def dimensionals_creator_on_sql(conn_dict):


    engine = drop_all_tables(conn_dict)
    metadata = MetaData()

    logging.info("\nStarting: Creation of the schema in the database...")
    logging.info(engine)

    with engine.begin() as connection:
        metadata.reflect(bind=engine)

        for table in reversed(metadata.sorted_tables):
            logging.info(f"Dropping table: {table.name}")
            connection.execute(text(f"DROP TABLE IF EXISTS {table.name} CASCADE"))
            logging.info(f"The table {table.name} has been dropped successfully")

    
        connection.commit()

    dim_tipogasto_sql = Table(
        'dim_tipogasto',
        metadata,
        Column('id_tipogasto', Integer, primary_key=True),
        Column('cve_cog', String(255)),
        Column('capítulo', String(255)),
        Column('concepto', String(255)),
        Column('part__genérica', String(255)),
        Column('part__específica', String(255)),
        Column('cve_tg', String(255)),
        Column('tipo_de_gasto', String(255)),
        Column('rubro', String(255)),
        extend_existing=True
    )


    dim_momento_sql = Table(
        'dim_momento', 
        metadata,
        Column('id_momento', Integer, primary_key=True),
        Column('momento', String(25)),
        extend_existing=True,
    )

    dim_proyecto_sql = Table(
        'dim_proyecto', 
        metadata,
        Column('id_proyecto', Integer, primary_key=True),
        Column('cve_nup', String(255)),
        Column('nombre_nup', String(255)),
        Column('rubro_nup', String(255)),
        Column('concepto_ef', String(255)),
        Column('informe', String(255)),
        extend_existing=True
    )

    dim_sector_sql = Table(
        'dim_sector', 
        metadata,
        Column('id_sector', Integer, primary_key=True),
        Column('cve_nue', String(255)),
        Column('estatus', String(255)),
        Column('nombre_nue', String(255)),
        Column('nue_sin_oya', String(255)),
        Column('sector', String(255)),
        Column('dirección', String(255)),
        Column('dependencia', String(255)),
        extend_existing=True
    )

    dim_entidad_sql = Table(
        'dim_entidad', 
        metadata,
        Column('id_entidad', Integer, primary_key=True),
        Column('cve_urg', String(255)),
        Column('urg', String(255)),
        Column('cve_adm', String(255)),
        Column('adm', String(255)),
        Column('entidad', String(255)),
        Column('entidad_2', String(255)),
        Column('entidad_gs', String(255)),
        extend_existing=True
    )

    dim_programa_sql = Table(
        'dim_programa', 
        metadata,
        Column('id_programa', Integer, primary_key=True),
        Column('cve_prog', String(255)),
        Column('prog', String(255)),
        Column('cve_fun', String(255)),
        Column('finalidad', String(255)),
        Column('función', String(255)),
        Column('sub__función', String(255)),
        Column('cve_a/s', String(255)),
        Column('a/s', String(255)),
        Column('cve_pedq', String(255)),
        Column('pedq', String(255)),
        extend_existing=True
    )

    dim_fuente_sql = Table(
        'dim_fuente', 
        metadata,
        Column('id_fuente', Integer, primary_key=True),
        Column('ff', String(255)),
        Column('cve_conac1', String(255)),
        Column('conac1', String(255)),
        Column('cve_conac2', String(255)),
        Column('conac2', String(255)),
        Column('federal_/_estatal', String(255)),
        Column('cve_año', String(255)),
        Column('año_ff', String(255)),
        Column('cve_ramo', String(255)),
        Column('ramo', String(255)),
        Column('origen_ramo', String(255)),
        Column('cve_fondo', String(255)),
        Column('fondo', String(255)),
        Column('nombre_ff', String(255)),
        Column('cve_origen', String(255)),
        Column('origen', String(255)),
        Column('cve_ltp', String(255)),
        Column('ltp', String(255)),
        Column('origen_ff', String(255)),
        extend_existing=True
    )

    fact_gasto_sql = Table(
        'fact_gasto', 
        metadata,
        Column('cadena_siafeq', String(100)),
        Column('cantidad', Float),
        Column('fecha', DateTime),
        Column('año', Integer),
        Column('id_momento', Integer, ForeignKey('dim_momento.id_momento')),
        Column('id_tipogasto', Integer, ForeignKey('dim_tipogasto.id_tipogasto')),
        Column('id_fuente', Integer, ForeignKey('dim_fuente.id_fuente')),
        Column('id_programa', Integer, ForeignKey('dim_programa.id_programa')),
        Column('id_entidad', Integer, ForeignKey('dim_entidad.id_entidad')),
        Column('id_sector', Integer, ForeignKey('dim_sector.id_sector')),
        Column('id_proyecto', Integer, ForeignKey('dim_proyecto.id_proyecto')),
        extend_existing=True
    )



    with engine.begin() as connection:
        metadata.create_all(engine)
    
    logging.info("Finished: Creation of the schema in the database")
    
    metadata.clear()
    gc.collect()

    return engine


def loading_data(engine, dimension_tables_dict):
    
    logging.info("\nStarting: Loading of data tables in the database...")

    for table_name, data in dimension_tables_dict.items():
        
        logging.info(f"Starting: Loading of {table_name} in the database")
        if table_name != "fact_gasto":
            data.to_sql(table_name, con=engine, if_exists="append", index=False)
        else:
            data.to_sql(table_name, con=engine, if_exists="append", index=False, method=psql_insert_copy)
        logging.info(f"Finished: Loading of {table_name} in the database")


    logging.info("ALL DONE BABY")


def workflow():

    start_time = time.time()
    # Connect to data sources
    conn_dict = connect_to_data_sources()

    # Main data consolidation
    hist_df, new_df = main_data_consolidation(
        connections_dict=conn_dict
    )

    # Loading of rules_df
    rules_df = pd.read_csv(conn_dict["rules"])

    # Basic transformations in new_df:
    # Creation of new columns based on another ones
    new_df = columns_expander(
        new_df=new_df,
        rules_df=rules_df
    )

    # Columns with a change name
    new_df = change_columns_names(
        rules_df=rules_df, 
        new_df=new_df, 
        hist_df=hist_df
    )

    # Map in columns:
    new_df, external_data_dict = mapping_columns(
        new_df=new_df,
        connections_dict=conn_dict
    )
    
    # Consolidate final df
    df_melt = consolidate_final_df(
        new_df=new_df,
        hist_df=hist_df
    )
    
    del new_df # Free memory

    # Creation of dimensional tables 
    dim_tables_dict, df_melt = dimensional_creator(
        df_melted=df_melt
    )

    # Creation of dimensionals in the SQL database
    engine_postgres = dimensionals_creator_on_sql(conn_dict=conn_dict)
     
    # Appending the melt_df in the dictionary of tables to be loaded:
    dim_tables_dict["fact_gasto"] = df_melt

    

    # Load the tables
    loading_data(
        engine=engine_postgres,
        dimension_tables_dict=dim_tables_dict
    )
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"\nTotal execution time: {elapsed_time / 60:.2f} minutes")

if __name__ == "__main__":
    workflow()