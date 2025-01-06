import os
import pandas as pd
from configparser import ConfigParser
from configparser import Error as ConfigError
from sqlalchemy.orm import Session
from sqlalchemy.orm import DeclarativeBase # Imported for typehints
from sqlalchemy.exc import IntegrityError
from abc import ABC, abstractmethod

import cmti_tools.tools as tools
from cmti_tools.tables import *
from cmti_tools.idmanager import ProvID
from cmti_tools.idmanager import ID_Manager

# Bulk import functions

class converter_factory:
  """
  A class that generates converters for use in pandas, based on expected column datatypes and default values.

  ...

  Attributes
  ----------
  types_table: pandas.DataFrame
    A DataFrame with columns "Column", "Type", and "Default". Column must have unique values.

  Methods
  ----------
  create_converter(column):
    Uses types_table to create a converter for input column. Returns a function based on column dtype.

  create_converter_dict():
    Calls create_converter for each Column value in types_table and returns a dictionary of converter functions.
  """
  def __init__(self, types_table):
    """
    :param types_table: A DataFrame with columns "Column", "Type", and "Default". Column values must be unique.
    :type types_table: pandas.DataFrame.
    """
    self.types_table = types_table

  def create_converter(self, column:str):
    """
    Creates a function for the input column that either returns the default or performs some cleanup action.

    :param column: The Column value from types_table.
    :type column: str.

    :return: Function
    """
    dtype = self.types_table.loc[self.types_table.Column == column, 'Type'].values[0]
    default = self.types_table.loc[self.types_table.Column == column, 'Default'].values[0]
    if dtype.startswith('u') or dtype.startswith('i') or dtype.startswith('I'):
      def get_int(val):
        if pd.isna(val):
          return default
        if isinstance(val, str):
          return tools.get_digits(val, 'int')
        return val
      return get_int
    if dtype.startswith('f'):
      def get_float(val):
        if pd.isna(val):
          return default
        if isinstance(val, str):
          return tools.get_digits(val, 'float')
        return val
      return get_float
    if dtype == 'U':
      def get_str(val):
        if pd.isna(val):
          return default
        if isinstance(val, str):
          return val.strip()
        return val
      return get_str
    if dtype == 'datetime64[ns]':
      def get_datetime(val):
        if pd.isnull(val):
          return datetime.now()
        return val
      return get_datetime

  def create_converter_dict(self):
    """
    Runs create_converter for all columns in types_table.Column.

    :return: dict.
    """
    converters = {}
    for i, row in self.types_table.iterrows():
      converters[row.Column] = self.create_converter(row.Column)
    return converters

def clean_table_data(in_table:pd.DataFrame, types_table:pd.DataFrame, drop_NA_columns:list=None) -> pd.DataFrame:
  """
  Enforces dtypes for in_table columns and inserts default values.

  :param in_table: The table being cleaned.
  :type in_table: Pandas DataFrame.

  :param types_table: A DataFrame with columns "Column", "Type", and "Default".
  :type types_table: Pandas DataFrame.

  :param drop_NA_columns: Columns where row should be dropped if value is missing. Provides a way of removing rows that lack required values before committing to database. Default: None.
  :type drop_NA_columns: list.
  """

  pd.set_option('future.no_silent_downcasting', True)

  # Drop rows with NA values in critical columns
  if drop_NA_columns is not None:
    in_table.dropna(subset=drop_NA_columns, how='any', inplace=True)
  # Fill NAs with defaults
  na_dict = dict(zip(types_table.Column, types_table.Default))
  dtype_dict = dict(zip(types_table.Column, types_table.Type))
  out_table = in_table.fillna(na_dict)
  # Enforce type
  # Numeric columns use to_numeric instead of astype.
  out_table.select_dtypes(include='number').apply(func=lambda col: pd.to_numeric(col, errors='coerce', dtype_backend='numpy_nullable'), axis=0)

  #TODO: If needed - isolate string columns and apply defaults/NA. isolate bad numeric values and insert default.

  return out_table

# Abstract Classes implementation

class DataImporter(ABC):
  """
  An abstract base class for importing data sources.
  Manages data initialization, record creation, and database ingestion.
  """
  def __init__(self, name_convert_dict:str|dict|None=None, cm_list:str|dict|None=None, metals_dict:str|dict|None=None):
    """
    Initializes the DataImporter class with optional configurations for 
    name conversion, critical minerals, and metals classification.
    """
    self.id_manager = ID_Manager()

    # Use ConfigParser to get data files if not provided
    config = ConfigParser()
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config.toml"))
    config.read(cfg_path)

    # TODO: Use create_module_variables here
    if name_convert_dict == 'config':
      with open(config.get('sources', 'elements'), mode='r') as elements_file:
        self.name_convert_dict = tools.create_name_dict(elements_file)
    elif name_convert_dict is not None:
        self.name_convert_dict = name_convert_dict
    
    # Load critical minerals list
    if cm_list == 'config':
      with open(config.get('sources', 'critical_minerals'), mode='r') as critical_minerals_file:
        critical_minerals = pd.read_csv(critical_minerals_file, header=0)
        self.cm_list = critical_minerals
    elif cm_list is not None:
      self.cm_list = cm_list
    
    # Load metals dict
    if metals_dict == 'config':
      with open(config.get('sources', 'metals'), mode='r') as metals_file:
        metals_csv = pd.read_csv(metals_file, header=0, encoding='utf-8')
        self.metals_dict = dict(zip(metals_csv['Commodity'], metals_csv['Type']))
    elif metals_dict is not None:
      self.metals_dict = metals_dict

  @abstractmethod
  def create_row_records(self, row: pd.Series) -> list[object]:
    """
    Process a single row and generates a DeclarativeBase objects based on inputs. 
    Implemented by child classes.
    """
    pass

  @abstractmethod
  def clean_input_table(self, input_table:pd.DataFrame) -> pd.DataFrame:
    """
    Clean values in the input table and make column data types consistent.
    """
    pass

  def generate_records(self, dataframe:pd.DataFrame) -> list[object]:
    """
    Converts a DataFrame into a list of database records using create_row_records().
    
    :param dataframe: The input data as a pandas DataFrame.
    :type dataframe: pd.DataFrame 
    """
    session_records = []
    for _, row in dataframe.iterrows():
      row_records = self.create_row_records(row)
      session_records = session_records + row_records
    return session_records

  def ingest_records(self, record_list:list[object], session:Session) -> None:
    """
    Commits all generated records to the database in a single transaction.

    :param record_list: List of ORM objects to be inserted.
    :type record_list: list

    :param session: SQLAlchemy session for committing records.
    :type session: Session 
    """
    session.add_all(record_list)
    session.commit()

  def commit_object(self, obj, session:Session):
    """
    Commit an object to session. Deprecated: each DataImporter child class has its own commit method. Will be removed eventually.

    :param obj: An SQL Alchemy ORM object
    :type obj: sqlalchemy.ORM.DeclarativeBase
    """

    try:
      session.add(obj)
      session.commit()
    except IntegrityError as e:
      print(e)
      session.rollback()

class WorksheetImporter(DataImporter):
  """
  Imports worksheet data into the database.
  """
  def __init__(self, name_convert_dict = 'config', cm_list = 'config', metals_dict = 'config', auto_generate_cmti_ids:bool=False):
    super().__init__(name_convert_dict, cm_list, metals_dict)

    # ID Manager currently relies on a session query to initialize IDs. Leave this out for now.
    # if auto_generate_cmti_ids:
    #   self.id_manager = ID_Manager()
  
  def create_row_records(self, row, cm_list:list=None, metals_dict:dict=None, name_convert_dict:dict=None, comm_col_count:int=8, source_col_count:int=4):
    """
    Processes a worksheet row based on its 'Site_Type' and creates database records.

    :param row: A pandas Series containing data from a worksheet row.
    :type row: pd.Series

    :param cm_list: Critical Minerals list
    :type cm_list: list

    :param metals_dict: Metals dictionary
    :type metals_dict: dict

    :param name_convert_dict: Name Convert dictionary
    :type name_convert_dict: dict

    :param comm_col_count: Commodity Column count to indicate amount of commodities in record
    :type comm_col_count: int

    :param source_col_count: Source Column count to indicate amount of sources in record
    :type source_col_count: int

    :return list: row_records
    """
    # Data tables will default to WorksheetImporter attributes but can be overridden
    if cm_list is None:
      cm_list = self.cm_list
    if metals_dict is None:
      metals_dict = self.metals_dict
    if name_convert_dict is None:
      name_convert_dict = self.name_convert_dict
 
    self.row_records = []
      
    # The worksheet is based on 3 types of records. The imported data will change based on record type:
    site_type = row['Site_Type']
    if site_type == "Mine":
      mine = self.process_mine(row, comm_col_count, source_col_count)
      self.row_records.append(mine)
    # elif site_type == "TSF":
    #   self.process_tsf(row)
    # elif site_type == "Impoundment":
    #   self.process_impoundment(row)
    return self.row_records
  
  def clean_input_table(self, input_table, drop_NA_columns=['Site_Name', 'Site_Type', 'CMIM_ID', 'Latitude', 'Longitude'], calculate_UTM=True):
      
    cmti_dtypes = {'Site_Name':'U', 'Site_Type':'U', 'CMIM_ID':'U', 'Site_Aliases': 'U', 'Last_Revised': 'datetime64[ns]',
      'NAD': 'Int64', 'UTM_Zone':'Int64', 'Easting':'Int64', 'Northing':'Int64', 'Latitude': 'f',
      'Longitude': 'f', 'Country':'U','Province_Territory': 'U', 'NTS_Area':'U', 'Mining_District': 'U', 'Parent': 'U', 'Parent_ID': 'U',
      'Commodity1':'U', 'Commodity2':'U', 'Commodity3': 'U', 'Commodity4': 'U', 'Commodity5': 'U', 'Commodity6':'U',
      'Commodity7':'U', 'Commodity8':'U', 'Mine_Type':'U',  'Mining_Method':'U', 'Mine_Status': 'U',
      'Owner_Operator': 'U', 'Past_Owners': 'U', 'Dev_Stage': 'U', 'DS_Comments': 'U', 'Site_Access': 'U',
      'SA_Comments': 'U',  'Shaft_Depth':'f', 'Construction_Year': 'Int64', 'Year_Opened': 'Int64', 'Year_Closed': 'Int64',
      'Reserves_Resources': 'f', 'SEDAR': 'U', 'Source_1': 'U', 'Source_1_ID': 'U', 'Source_1_Link': 'U',
      'Source_2': 'U', 'Source_2_ID': 'U', 'Source_2_Link': 'U', 'Source_3': 'U', 'Source_3_ID': 'U', 'Source_3_Link': 'U',
      'Source_4': 'U', 'Source_4_ID': 'U', 'Source_4_Link': 'U', 'Notes': 'U', 'Orebody_Type':'U', 'Orebody_Class':'U',
      'Ore_Minerals':'U', 'Processing_Method':'U',  'Ore_Processed': 'f', 'Ore_Processed_Unit':'U',
      'Other_Mineralization': 'U', 'Spectral_Mineralization': 'U', 'Forcing_Features': 'U', 'Feature_References': 'U',
      'NOAMI_Status': 'U', 'NOAMI_Site_Class': 'U', 'Hazard_Class':'U', 'Hazard_System':'U', 'PRP_Rating':'U',
      'Rehab_Plan':'U', 'EWS':'U', 'EWS_Rating':'U', 'Raise_Type':'U', 'History_Stability_Concerns':'U',
      'Rating_Index':'U', 'Acid_Generating':'U',  'Treatment':'U', 'Current_Max_Height': 'f', 'Tailings_Storage_Method': 'U',
      'Tailings_Volume': 'f', 'Tailings_Capacity':'f', 'Tailings_Area':'f', 'Tailings_Area_From_Images':'f',
      'Tailings_Area_Notes': 'U'}
    grades = ['Au_Grade', 'Au_Contained', 'Au_Produced', 'Ag_Grade', 'Ag_Contained', 'Ag_Produced', 'Barite_Grade',
      'Barite_Contained', 'Barite_Produced', 'Bi_Grade', 'Bi_Contained', 'Bi_Produced', 'Cd_Grade', 'Cd_Contained',
      'Cd_Produced', 'Coal_Type', 'Coal_Rank', 'Coal_Production', 'Coal_Produced', 'Co_Grade', 'Co_Contained',
      'Co_Produced', 'Cu_Grade', 'Cu_Contained', 'Cu_Produced', 'Diamond', 'Diamond_Grade', 'Fe_Grade', 'Fe_Produced',
      'Fe_Ore_Extracted', 'Fe_Ore_Smelted', 'Flourspar_Grade', 'Flourspar_Contained', 'Graphite_Grade', 'Graphite_Contained',
      'Gypsum_Produced', 'In_Grade', 'In_Contained', 'In_Produced', 'Mo_Grade', 'Mo_Contained', 'Mo_Produced',
      'Ni_Grade', 'Ni_Contained', 'Ni_Produced', 'Pb_Grade', 'Pb_Contained', 'Pb_Produced', 'Pd_Grade', 'Pd_Contained',
      'Pd_Produced', 'Potash_Grade', 'Potash_Contained', 'Potash_Produced', 'Pt_Grade', 'Pt_Contained', 'Pt_Produced',
      'Sb_Grade', 'Sb_Contained', 'Sb_Produced', 'Sn_Grade', 'Sn_Contained', 'Sn_Produced', 'U_Grade', 'U_Contained',
      'U_Produced', 'W_Grade', 'W_Contained', 'W_Produced', 'Zn_Grade', 'Zn_Contained', 'Zn_Produced']

    for grade in grades:
      cmti_dtypes[grade] = 'f'
    cmti_defaults = {}
    for key, val in cmti_dtypes.items():
      if val[0] in ['i','I','u','f']:
        cmti_defaults[key] = pd.NA
      elif val == 'NAD':
        cmti_defaults[key] = 83
      elif val == 'Site_Aliases':
        cmti_defaults[key] = None
      elif val == 'U':
        cmti_defaults[key] = 'Unknown'
      elif val == 'datetime64[ns]':
        cmti_defaults[key] = pd.NaT   
        
    cmti_types_table = pd.DataFrame(columns=['Column', 'Type', 'Default'])
    converters = converter_factory(cmti_types_table).create_converter_dict()
    # UTM_Zone gets a special converter
    if calculate_UTM:
      def get_UTM(val):
        if pd.isna(val):
          return tools.lon_to_utm_zone(val)
        else:
          return int(val)
      converters['UTM_Zone'] = get_UTM

    cmti_df = pd.read_excel(input_table, header=0, converters=converters)
    # Drop rows that are missing values in the drop_NA_columns list
    cmti_df = cmti_df.dropna(subset=drop_NA_columns)

    # Final type coercion
    for column in cmti_df.columns:
      try:
        dtype = cmti_dtypes[column]
        if dtype.startswith('u') or dtype.startswith('i') or dtype.startswith('I'):
          cmti_df[column] = pd.to_numeric(cmti_df[column], errors='coerce').astype('Int64')
        elif dtype.startswith('f'):
          cmti_df[column] = pd.to_numeric(cmti_df[column], errors='coerce').astype('float')
      except Exception as e:
        print(f"Failed on column: {column}")
        print(e)
      # Lastly, fill blank "last revised" with today's date. 
        # Note: This should have been done in the converters but I couldn't get it to work. Probably a better option would be to allow Nulls for times.
      cmti_df.Last_Revised = cmti_df.Last_Revised.fillna(datetime.now().date())
    return cmti_df

  def process_mine(self, row:pd.Series, comm_col_count, source_col_count):
    """
    Processes mine-specific data and creates Mine, Owner, Alias, 
    Commodity, Reference, and default TSF and Impoundment records.
    """
    mine = Mine(
      cmdb_id = row.CMIM_ID,
      name = row.Site_Name,
      prov_terr = row.Province_Territory,
      last_revised = row.Last_Revised,
      nad = row.NAD,
      utm_zone = row.UTM_Zone,
      easting = row.Easting,
      northing = row.Northing,
      latitude = row.Latitude,
      longitude = row.Longitude,
      nts_area = row.NTS_Area,
      mining_district = row.Mining_District,
      mine_type = row.Mine_Type,
      mine_status = row.Mine_Status,
      mining_method = row.Mining_Method,
      development_stage = row.Dev_Stage,
      site_access = row.Site_Access,
      construction_year = row.Construction_Year
    )
    
    # Commodities
    comm_columns = [f"Commodity{i}" for i in range(1, comm_col_count+1)]
    for col in comm_columns:
      if pd.notna(row[col]):
        commodity_record = tools.get_commodity(row, col, self.cm_list, self.name_convert_dict, self.metals_dict, mine)
        self.row_records.append(commodity_record)
  
    # Aliases
    # There are often multiple comma-separated aliases. Split them up
    aliases = row.Site_Aliases
    if pd.notna(aliases):
      # Check if more than one
      aliases_list = [alias.strip() for alias in aliases.split(",")]
      for aliasName in aliases_list:
        alias = Alias(alias=aliasName)
        alias.mine=mine
        self.row_records.append(alias)

    # Owners
    owner = Owner(name=row.Owner_Operator)
    mine.owners.append(owner)
    self.row_records.append(owner)
    
    past_owners = row.Past_Owners
    if pd.notna(past_owners):
      past_owners_list = [_past_owner.strip() for _past_owner in past_owners.split(",")]
      for past_owner in past_owners_list:
        owner = Owner(name=past_owner)
        owner.mines.append(mine)
        self.row_records.append(owner)

    # References
    source_columns = [f"Source_{j}" for j in range(1, source_col_count+1)]
    for col in source_columns:
      source = row[col]
      if pd.notna(source) and source != "Unknown":
        source_id = row[f"{col}_ID"]
        link = row[f"{col}_Link"]
        reference = Reference(mine=mine, source=source, source_id=source_id, link=link)
        self.row_records.append(reference)

    # Default tailings facility. Every mine gets one
    default_TSF = TailingsFacility(
      name = f"defaultTSF_{mine.name}".strip(),
      cmdb_id = mine.cmdb_id,
      status = row.Mine_Status,
      hazard_class = row.Hazard_Class,
      latitude = mine.latitude,
      longitude = mine.longitude,
      is_default = True,
    )
    default_TSF.mines.append(mine)
    self.row_records.append(default_TSF)

    # Default impoundment. Every default tailings facility gets one
    impountment_name = f"{mine.name.strip()}_defaultImpoundment"
    default_impoundment = Impoundment(
      name=impountment_name,
      parentTsf = default_TSF,
      is_default = True,
      area = row.Tailings_Area,
      volume = row.Tailings_Volume,
      capacity = row.Tailings_Capacity,
      storage_method = row.Tailings_Storage_Method,
      max_height = row.Current_Max_Height,
      acid_generating = row.Acid_Generating,
      treatment = row.Treatment,
      rating_index = row.Rating_Index,
      stability_concerns = row.History_Stability_Concerns
    )
    self.row_records.append(default_impoundment)

    self.row_records.append(mine)

  def process_tsf(self, row:pd.Series, parent_mine:Mine):
    tsf = TailingsFacility(
      name = row.Site_Name,
      cmdb_id = row.CMIM_ID,
      status = row.Mine_Status,
      hazard_class = row.Hazard_Class,
      latitude = row.Latitude,
      longitude = row.Longitude,
      is_default = False
    )
    tsf.mines.append(parent_mine)
    return tsf
  
  def process_impoundment(self, row:pd.Series, parent_TSF:TailingsFacility):
    impoundment = Impoundment(
      name = row.Site_Name,
      cmdb_id = row.CMIM_ID,
      is_default = False,
      area = row.Tailings_Area,
      volume = row.Tailings_Volume,
      capacity = row.Tailings_Capacity,
      max_height = row.Current_Max_Height,
      acid_generating = row.Acid_Generating,
      treatment = row.Treatment,
      rating_index = row.Rating_Index,
      stability_concerns = row.History_Stability_Concerns
    )
    impoundment.parentTsf = parent_TSF
    return impoundment

class OMIImporter(DataImporter):
  def __init__(self, cm_list:list='config', metals_dict:dict='config', name_convert_dict:dict='config'):
    """
    Initializes the OMIImporter class with configuration data.
    
    :param cm_list: List of critical minerals.
    :type cm_list: list

    :param metals_dict: Dictionary mapping metal names to properties.
    :type metals_dict: dict

    :param name_convert_dict: Dictionary for converting commodity names.
    :type name_convert_dict: dict
    """
    super().__init__(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_convert_dict)
    self.prov_id = ProvID("ON")
  
  def clean_input_table(self, input_table):
    omi_dict = {
      'MDI_IDENT': 'U',
      'NAME': 'U',
      'STATUS': 'U',
      'TWP_AREA': 'U',
      'RGP_DIST': 'U',
      'P_COMMOD': 'U',
      'S_COMMOD': 'U',
      'ALL_NAMES': 'U',
      'DEP_CLASS': 'U',
      'LONGITUDE': 'f4',
      'LATITUDE': 'f4',
      'LL_DATUM': 'U',
      'DETAIL': 'U'
    }

    omi_columns = list(omi_dict.keys())
    omi_dtypes = list(omi_dict.values())
    omi_defaults = ["Unknown" if t == "U" else pd.NA for t in omi_dtypes]
    # omi_defaults = {}
    # for key, val in omi_dtypes.items():
    #   if val[0] in ['i','I','u','f']:
    #     omi_defaults[key] = pd.NA
    #   elif val == 'NAD':
    #     omi_defaults[key] = 83
    #   elif val == 'Site_Aliases':
    #     omi_defaults[key] = None
    #   elif val == 'U':
    #     omi_defaults[key] = 'Unknown'
    #   elif val == 'datetime64[ns]':
    #     omi_defaults[key] = pd.NaT   

    omi_types_table = pd.DataFrame({
        'Column': omi_columns,
        'Type': omi_dtypes,
        'Default': omi_defaults
    })

    converters = converter_factory(omi_types_table).create_converter_dict()

    omi_df = pd.read_excel(input_table, header=0, converters=converters)
    return omi_df

  def create_row_records(self, row: pd.Series, name_convert_dict: dict=None) -> list[object]:
    """
    Processes a row of data and creates associated database records.
    
    :param row: The data row to be processed.
    :type row: pd.Series

    :param name_convert_dict: Optional dictionary for commodity name conversion.
    :type name_convert_dict: dict, optional

    :return list[object]: A list of created data records.
    """
    # name_convert_dict will default to the OMIImporter attribute but can be overridden
    if name_convert_dict is None:
      name_convert_dict = self.name_convert_dict
    row_records = []
    try:
      row_id = self.prov_id.formatted_id
      self.prov_id.update_id()
      mine = Mine(
        cmdb_id = row_id,
        name = row['NAME'],
        latitude = row['LATITUDE'],
        longitude = row['LONGITUDE'],
        prov_terr="ON",
        mining_district = row['RGP_DIST']
      )
      row_records.append(mine)

      # Aliases
      aliases = [name.strip() for name in row['ALL_NAMES'].split(",")]
      for alias_val in aliases:
        alias = Alias(mine=mine, alias=alias_val)
        row_records.append(alias)
      
      # Commodities
      for comm_col in ['P_COMMOD', 'S_COMMOD']:
        comm_record = tools.get_commodity(row, comm_col, self.cm_list, self.name_convert_dict, self.metals_dict, mine)
        row_records.append(comm_record)

      # Default TSF
      tsf = TailingsFacility(is_default = True, name = f"default_TSF_{mine.name}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)

      # Default Impoundment
      impoundment = Impoundment(parentTsf = tsf, is_default = True, name = f"{tsf.name}_impoundment")
      row_records.append(impoundment)

      omi_reference = Reference(mine=mine, source = "OMI", source_id = row["MDI_IDENT"], link=row['DETAIL'])
      row_records.append(omi_reference)

      return row_records
    except Exception as e:
      raise e

class OAMImporter(DataImporter):
  def __init__(self, oam_comm_names:dict, cm_list='config', metals_dict='config', name_convert_dict='config'):
    """
    Initializes the OAMImporter class with configuration data and commodity names.

    :param oam_comm_names: Dictionary of OAM commodity names.
    :type oam_comm_names: dict

    :param cm_list: List of critical minerals.
    :type cm_list: list

    :param metals_dict: Dictionary mapping metal names to properties.
    :type metals_dict: dict

    :param name_convert_dict: Dictionary for converting commodity names.
    :type name_convert_dict: dict

    """
    super().__init__(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_convert_dict)
    self.oam_comm_names = oam_comm_names

  def check_year(self, val):
    """
    Checks and extracts the year from a value.

    :param val: Value to be checked for a year.
    :type val: str or float
    :return int or None: Extracted year or None if not available.
    """
    if isinstance(val, str):
      return tools.get_digits(val)
    elif pd.isna(val):
      return None
    else:
      return val

  def clean_input_table(self, input_table):
    oam_dict = {
      'OID': 'U',
      'Lat_DD': 'f4',
      'Long_DD': 'f4',
      'Jurisdiction': 'U',
      'Juris_ID': 'U',
      'Name': 'U',
      'Status': 'U',
      'Commodity_Code': 'U',
      'Commodity_Full_Name': 'U',
      'Mined_Quantity': 'f4',
      'Mine_Type': 'U',
      'Last_Year': 'U',
      'Permit': 'U',
      'URL': 'U',
      'Forcing_Features': 'U',
      'Feature_References': 'U',
      'Feature_Class': 'U',
      'Location': 'U',
      'County': 'U',
      'Landowner': 'U',
      'Last_Operator': 'U',
      'Start_Date': 'f4',
      'Peak_Production': 'U',
      'Last_Updated': 'f4'
  }

    # Take keys and values as columns and types for dataframe
    oam_columns = list(oam_dict.keys())
    oam_dtypes = list(oam_dict.values())
    #Set default values based on datatype
    oam_defaults = ["Unknown" if t == 'U' else pd.NA for t in oam_dtypes]

    # Create df for read_csv/read_excel function
    oam_df = pd.DataFrame({
        'Column': oam_columns,
        'Type': oam_dtypes,
        'Default': oam_defaults
    })

    oam_columns = list(oam_dict.keys())
    oam_dtypes = list(oam_dict.values())
    oam_defaults = ["Unknown" if t == "U" else pd.NA for t in oam_dtypes]
    oam_defaults = ["Unknown" if t == "U" else pd.NA for t in oam_dtypes]

    omi_types_table = pd.DataFrame({
        'Column': oam_columns,
        'Type': oam_dtypes,
        'Default': oam_defaults
    })

    converters = converter_factory(omi_types_table).create_converter_dict()
    try:
      oam_df = pd.read_csv(input_table, header=0, converters=converters)
      print("load csv")
    except:
      oam_df = pd.read_excel(input_table, header=0, converters=converters)
      print("load excel")
    return oam_df

  def create_row_records(self, row: pd.Series, oam_comm_names:dict, cm_list:list=None, metals_dict:dict=None, name_convert_dict:dict=None):
    """
    Processes a row of OAM data and creates associated database records.

    :param row: The data row to be processed.
    :type row: pd.Series

    :param oam_comm_names: Dictionary of OAM commodity names.
    :type oam_comm_names: dict

    :param cm_list: List of critical minerals.
    :type cm_list: list

    :param metals_dict: Dictionary mapping metal names to properties.
    :type metals_dict: dict

    :param name_convert_dict: Dictionary for converting commodity names.
    :type name_convert_dict: dict

    :return list[object]: A list of created data records.
    """
    # Data tables will default to OAMImporter attributes but can be overridden
    if oam_comm_names is None:
      oam_comm_names = self.oam_comm_names
    if cm_list is None:
      cm_list = self.cm_list
    if metals_dict is None:
      metals_dict = self.metals_dict
    if name_convert_dict is None:
      name_convert_dict = self.name_convert_dict

    row_records = []
    try:
      provID = getattr(self.id_manager, row["Jurisdiction"])
      provID.update_id()
      cmdb_id = provID.formatted_id

      mine = Mine(
        cmdb_id = cmdb_id,
        name = row["Name"].title(),
        latitude = row["Lat_DD"],
        longitude = row["Long_DD"],
        prov_terr = row["Jurisdiction"],
        mine_status = row["Status"],
        mine_type = row["Mine_Type"],
        construction_year = row["Start_Date"]
      )
      row_records.append(mine)

      comm_code = row['Commodity_Code']
      comm_full = row['Commodity_Full_Name'] # Records have either code or full name. Check both.
      comm_name = comm_code if pd.notna(comm_code) else comm_full # This assumes that no row_ have both.
      if pd.notna(comm_name):
        try:
          # Sometimes multiple listed in code, split it up and add one entry for each
          commodities = [comm.strip() for comm in comm_name.split(",")]
          for comm in commodities:
            # Convert to full name using OAM name values, then to element names
            comm_full_oam = tools.convert_commodity_name(comm, oam_comm_names, output_type='full', show_warning=False)
            comm_name = tools.convert_commodity_name(comm_full_oam, name_convert_dict, output_type='symbol', show_warning=False)
            start_year = self.check_year(row['Start_Date'])
            end_year = self.check_year(row['Last_Year'])
            produced = row["Mined_Quantity"] if pd.notna(row["Mined_Quantity"]) else None
            commodity_record = CommodityRecord(
              mine=mine,
              commodity=comm_name,
              produced=produced,
              source_year_start=start_year,
              source_year_end=end_year
            )
            commodity_record.is_critical = True if comm_name in cm_list else False
            commodity_record.is_metal = metals_dict.get(comm_name)

            row_records.append(commodity_record)
        except Exception as e:
          print(e)

      tsf = TailingsFacility(is_default = True, name = f"default_TSF_{mine.name}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)

      impoundment = Impoundment(parentTsf = tsf, is_default = True, name = f"{tsf.name}_impoundment")
      row_records.append(impoundment)

      owner = Owner(name = row["Last_Operator"])
      owner.mines.append(mine)
      row_records.append(owner)

      oam_reference = Reference(mine = mine, source = "OAM", source_id = row["OID"], link = row["URL"])
      row_records.append(oam_reference)

      return row_records
    except Exception as e:
      print(e)

class BCAHMImporter(DataImporter):
  def __init__(self, cm_list:list='config', metals_dict:dict='config', name_convert_dict:dict='config'):
    """
    Initializes the BCAHMImporter class with configuration data.
    
    :param cm_list: List of critical minerals.
    :type cm_list: list

    :param metals_dict: Dictionary mapping metal names to properties.
    :type metals_dict: dict

    :param name_convert_dict: Dictionary for converting commodity names.
    :type name_convert_dict: dict
    """
    super().__init__(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_convert_dict)
    self.provID = ProvID('BC')

  def clean_input_table(self, input_table):
    bcahm_dict = {
      "OBJECTID": "u4",
      "MINFILNO": "U",
      "NAME1": "U",
      "NAME2": "U",
      "STATUS": "U",
      "LATITUDE": "f4",
      "LONGITUDE": "f4",
      "UTM_ZONE": "f4",
      "UTM_NORT": "f4",
      "UTM_EAST": "f4",
      "ELEV": "f4",
      "COMMOD_C1": "U",
      "COMMOD_C2": "U",
      "COMMOD_C3": "U",
      "DEPOSITTYPE_D1": "U",
      "DEPOSITTYPE_D2": "U",
      "DEPOSITCLASS_D1": "U",
      "DEPOSITCLASS_D2": "U",
      "NTSMAP_C1": "U",
      "NTSMAP_C2": "U",
      "Permit1": "U",
      "Permit2": "U",
      "Mine_Name": "U",
      "Mine_Statu": "U",
      "Region": "U",
      "Tailings": "U",
      "Disposal_Method": "U",
      "Mined": "f4",
      "Milled": "f4",
      "Mine_type": "U",
      "Permitee1": "U",
      "Permittee2": "U",
      "URL": "U",
      "Current_st": "U",
      "Permit1_Status": "U",
      "Permit2_Status": "U",
      "First_Year": "f4",
      "Last_Year": "f4"
    }

    bcahm_columns = list(bcahm_dict.keys())
    bcahm_types = list(bcahm_dict.values())
    bcahm_defaults = ["Unknown" if t == "U" else pd.NA for t in bcahm_types]

    bcahm_types_table = pd.DataFrame({
        'Column': bcahm_columns,
        'Type': bcahm_types,
        'Default': bcahm_defaults
    })

    converters = converter_factory(bcahm_types_table).create_converter_dict()

    bcahm_df = pd.read_excel(input_table, header=0, converters=converters)
    return bcahm_df

  def create_row_records(self, row: pd.Series, cm_list:list=None, metals_dict:dict=None, name_convert_dict:dict=None):
    """
    Processes a row of data from the BCAHM dataset and creates associated database records.
    
    :param row: The data row to be processed.
    :type row: pd.Series

    :param cm_list: List of critical minerals, defaults to the class attribute.
    :type cm_list: list

    :param metals_dict: Dictionary of metals and their properties, defaults to the class attribute.
    :type metals_dict: dict

    :param name_convert_dict: Dictionary for commodity name conversion, defaults to the class attribute.
    :type name_convert_dict: dict
    
    :return list[object]: A list of created data records.
    """
    # Data tables will default to BCAHMImporter attributes but can be overridden
    if cm_list is None:
      cm_list = self.cm_list
    if metals_dict is None:
      metals_dict = self.metals_dict
    if name_convert_dict is None:
      name_convert_dict = self.name_convert_dict

    row_records = []
    try:
      bcahm_id = self.provID
      mine_vals = {
        "name": row["NAME1"],
        "latitude": row["LATITUDE"],
        "longitude": row["LONGITUDE"],
        "utm_zone": row["UTM_ZONE"],
        "northing": row["UTM_NORT"],
        "easting": row["UTM_EAST"],
        "year_opened": row["First_Year"],
        "year_closed": row["Last_Year"],
        "nts_area": row["NTSMAP_C1"],
        "prov_terr": "BC",
        "mine_status": "Inactive"
      }

      # If either lat or lon are missing, don't add that record
      if (pd.isna(mine_vals["latitude"]) or mine_vals["latitude"] == 'Null') or \
        (pd.isna(mine_vals["longitude"]) or mine_vals["longitude"] == 'Null'):
          return
      
      # Check coordinates for null strings as well
      if mine_vals['northing'] == 'Null' or pd.isna(mine_vals['northing']):
        del(mine_vals['northing'])
      if mine_vals['easting'] == 'Null' or pd.isna(mine_vals['easting']):
        del(mine_vals['easting'])
      if pd.isna(mine_vals['utm_zone']) or mine_vals['utm_zone'] == 'Null':
        mine_vals['utm_zone'] = tools.lon_to_utm_zone(mine_vals['longitude'])
      
      mine = Mine(cmdb_id = bcahm_id.formatted_id, **mine_vals)
      row_records.append(mine)
      bcahm_id.update_id()

      # Create alias if there's another name
      if pd.notna(row["NAME2"]):
        alias = Alias(mine=mine, alias=row["NAME2"])
        row_records.append(alias)
      
      # Commodities
      for comm_col in ['COMMOD_C1', 'COMMOD_C2', 'COMMOD_C3']:
        commodity_record = tools.get_commodity(row, comm_col, cm_list, name_convert_dict, metals_dict, mine)
        row_records.append(commodity_record)

      # TSF
      tsf = TailingsFacility(is_default = True, name = f"default_TSF_{mine_vals['name']}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)
      bcahm_id.update_id()

      # Impoundment
      impoundment = Impoundment(parentTsf=tsf, is_default=True, name=f"{tsf.name}_impoundment")
      row_records.append(impoundment)
      bcahm_id.update_id()

      #Reference
      reference = Reference(mine = mine, source = "BCAHM", source_id = str(row.OBJECTID))
      row_records.append(reference)
      if row.MINFILNO != "Null":
        minefileref = Reference(mine = mine, source = "BC Minfile", source_id = row.MINFILNO)
        row_records.append(minefileref)

      # Orebody
      orebody = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D1"], ore_class = row["DEPOSITCLASS_D1"])
      row_records.append(orebody)
      if row["DEPOSITTYPE_D2"] != "Null":
        orebody2 = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D2"], ore_class = row["DEPOSITCLASS_D2"])
        row_records.append(orebody2)

      return row_records
    except Exception as e:
      raise(e)