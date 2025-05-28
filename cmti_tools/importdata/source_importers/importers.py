from pathlib import Path
import pandas as pd
from sqlalchemy.orm import DeclarativeBase # Imported for typehints
from datetime import datetime

import cmti_tools.tools as tools
# from cmti_tools.tables import *
from cmti_tools.tables import Mine, TailingsFacility, Impoundment, Owner, OwnerAssociation, Alias, Reference, CommodityRecord, Orebody
from cmti_tools.importdata import DataImporter, converter_factory
from cmti_tools.datamappers import mappings

BASE_DIR = Path(__file__).resolve().parent.parent.parent

class WorksheetImporter(DataImporter):
  """
  Imports worksheet data into the database.
  """
  def __init__(self, name_convert_dict = 'config', cm_list = 'config', metals_dict = 'config', auto_generate_cmti_ids:bool=False):
    super().__init__(name_convert_dict, cm_list, metals_dict)

    # ID Manager currently relies on a session query to initialize IDs. Leave this out for now.
    # if auto_generate_cmti_ids:
    #   self.id_manager = ID_Manager()
  
  def clean_input_table(
      self,
      input_table, 
      drop_NA_columns=['Site_Name', 'Site_Type', 'CMTI_ID', 'Latitude', 'Longitude'], 
      calculate_UTM=True, 
      force_dtypes=True, 
      convert_units:bool=True,
      **kwargs
    ):

    '''
    :param input_table: The input table to be cleaned.
    :type input_table: pd.DataFrame or str

    :param drop_NA_columns: Columns where row should be dropped if value is missing. Provides a way of removing rows that lack required values before committing to database. 
      Default: ['Site_Name', 'Site_Type', 'CMTI_ID', 'Latitude', 'Longitude']
    :type drop_NA_columns: list

    :param calculate_UTM: If True, calculate UTM Zone based on Longitude. Default: True
    :type calculate_UTM: bool

    :param force_dtypes: If True, enforce data types for all columns. Default: True
    :type force_dtypes: bool

    :param convert_units_dict: Dictionary where key == column and value == desired unit. Leave empty to ignore. Default: {}
    :type convert_units_dict: dict

    :param kwargs: Additional keyword arguments for unit definitions:
      :param unit_definitions: Dictionary of unit definitions to be added to Pint UnitRegistry. Values should follow pattern '{unit} = {str of definition}'. E.g.: 'm2 = meter ** 2'.
        Default: None
      :type unit_definitions: dict

      :param dimensionless_value_units: Dictionary of dimensionless value units. Key = column, value = unit. If None, a default list will be used. Set as {} to disable. Default: None
      :type dimensionless_value_units: dict
    '''
      
    cmti_dtypes = {'Site_Name':'U', 'Site_Type':'U', 'CMTI_ID':'U', 'Site_Aliases': 'U', 'Last_Revised': 'datetime64[ns]',
      'Datum': 'U', 'UTM_Zone':'Int64', 'Easting':'Int64', 'Northing':'Int64', 'Latitude': 'f',
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
      'Tailings_Area_Notes': 'U', 'Orebody_Type':'U', 'Orebody_Class':'U', 'Orebody_Minerals':'U', 'Ore_Processed':'f'}
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
      if val == 'Site_Aliases':
        cmti_defaults[key] = None
      elif val[0] in ['i','I','u','f']:
        cmti_defaults[key] = None
      elif val == 'U':
        cmti_defaults[key] = 'Unknown'
      elif val == 'datetime64[ns]':
        cmti_defaults[key] = pd.NaT   
        
    cmti_types_table = pd.DataFrame(data={'Column': list(cmti_dtypes.keys()), 'Type': list(cmti_dtypes.values()), 'Default': list(cmti_defaults.values())})
    
    if convert_units:

      if 'dimensionless_value_units' not in kwargs:
        dimensionless_value_units = {}

      def create_default_unit_conversion_dict():
        """
        Creates a default unit conversion dictionary for the WorksheetImporter.
        """
        unit_conversion_dict={
        'Tailings_Area': 'km2',
        'Tailings_Volume': 'm3',
        'Tailings_Capacity': 'm3',
        'Current_Max_Height': 'm',
        'Ore_Processed': 't'}
        unit_conversion_dict.update(dict.fromkeys([col for col in input_table.columns if 'Produced' in col], 'kg'))
        unit_conversion_dict.update(dict.fromkeys([col for col in input_table.columns if 'Contained' in col], 'kg')) # Maybe redundant/inefficient to have each in their own loop, but makes it easier to change later.
        # Also inefficient, but overwrite gold and silver values
        unit_conversion_dict['Au_Produced'] = 'oz'
        unit_conversion_dict['Au_Contained'] = 'oz'
        unit_conversion_dict['Ag_Produced'] = 'oz'
        unit_conversion_dict['Ag_Contained'] = 'oz'
        
        return unit_conversion_dict

      # Get the unit conversion dictionary from the kwargs, if it exists or create a default one using the function above.
      unit_conversion_dict = kwargs.get('unit_conversion_dict', create_default_unit_conversion_dict())
      
    else:
      unit_conversion_dict = None
      dimensionless_value_units = None

    # Currently not dealing with grades. It's a bit of a mess in the CMTI data.

    converters = converter_factory(cmti_types_table, unit_conversion_dict, dimensionless_value_units=dimensionless_value_units).create_converter_dict()

    # If passing a directory for input_table, read the file. Otherwise, assume it's a DataFrame.
    if isinstance(input_table, str):
      try:
        cmti_df = pd.read_excel(input_table, header=0)
      except:
        cmti_df = pd.read_csv(input_table, header=0)
    else:
      cmti_df = input_table

    # Drop rows that are missing critical values in the drop_NA_columns list before converting types
    cmti_df = cmti_df.dropna(subset=drop_NA_columns)

    # Apply converters for initial cleanup
    for col, func in converters.items():
      if cmti_df.get(col) is not None:
        try:
          cmti_df[col] = cmti_df[col].apply(func)
        except ValueError as ve:
          raise ve
        except KeyError:
          print(f"Column {col} not found in input table.")
          pass

    # Final type coercion and special cases
    # Assume Datum is 83
    cmti_df['Datum'] = cmti_df['Datum'].fillna("NAD83")

    # Calculate UTM Zone
    if calculate_UTM:
      for row in cmti_df.itertuples():
        if pd.isna(row.Longitude):
          cmti_df.at[row.Index, 'UTM_Zone'] = None
        elif pd.isna(row.UTM_Zone):
          try:
            cmti_df.at[row.Index, 'UTM_Zone'] = tools.lon_to_utm_zone(row.Longitude)
          except:
            print(f"Error calculating UTM Zone for row {row.Index}.")
            raise

    # Fill blank "last revised" with today's date. 
      #   # Note: This should have been done in the converters but I couldn't get it to work. Probably a better option would be to allow Nulls for times.
    cmti_df.Last_Revised = cmti_df.Last_Revised.fillna(datetime.now().date())
    
    # Coerce all dtypes
    if force_dtypes:
      cmti_df = self.coerce_dtypes(cmti_types_table, cmti_df)

    return cmti_df

  def create_row_records(self, row, cm_list:list=None, metals_dict:dict=None, name_convert_dict:dict=None, comm_col_count:int=8, source_col_count:int=4) -> list[DeclarativeBase]:
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
      
    # The worksheet is based on 3 types of records. The imported data will change based on record type:
    site_type = row['Site_Type']
    if site_type == "Mine":
      return self.process_mine(row, comm_col_count, source_col_count)

  def process_mine(self, row:pd.Series, comm_col_count, source_col_count) -> list[DeclarativeBase]:
    """
    Processes mine-specific data and creates Mine, Owner, Alias, 
    Commodity, Reference, and default TSF and Impoundment records.
    """

    records = []
    mine = self.series_to_table(Mine, row, mappings.worksheet_table_mapping)
    
    # Commodities
    comm_columns = [f"Commodity{i}" for i in range(1, comm_col_count+1)]
    for col in comm_columns:
      if pd.notna(row[col]) and row[col] != "Unknown":
        commodity_record = tools.get_commodity(row, col, self.cm_list, self.name_convert_dict, self.metals_dict, mine)
        records.append(commodity_record)
  
    # Aliases
    # There are often multiple comma-separated aliases. Split them up
    aliases = row.Site_Aliases
    if pd.notna(aliases):
      # Check if more than one
      aliases_list = [alias.strip() for alias in aliases.split(",")]
      for alias_name in aliases_list:
        alias = Alias(alias=alias_name)
        alias.mine=mine
        records.append(alias)

    # Owners
    # This pattern is from the Basic Relationship Patterns guide in the SQLAlchemy documentation
    if pd.notna(row.Owner_Operator):
      owner = Owner(name=row.Owner_Operator)
      owner_association = OwnerAssociation(owner=owner, mine= mine, is_current_owner=True)
      mine.owners.append(owner_association)
      records.append(owner_association)
    
    # Add past owners. Usually a comma-separated list of names
    past_owners = row.Past_Owners
    if pd.notna(past_owners):
      past_owners_list = [past_owner.strip() for past_owner in past_owners.split(",")]
      for past_owner in past_owners_list:
        owner = Owner(name=past_owner)
        past_owner_association = OwnerAssociation(owner=owner, mine= mine, is_current_owner=False)
        past_owner_association.owner = owner
        mine.owners.append(past_owner_association)
        records.append(past_owner_association)

    # References
    source_columns = [f"Source_{i}" for i in range(1, source_col_count+1)]
    for col in source_columns:
      source = row[col]
      if pd.notna(source) and source != "Unknown":
        source_id = str(row[f"{col}_ID"])
        link = str(row[f"{col}_Link"])
        reference = Reference(mine=mine, source=source, source_id=source_id, link=link)
        records.append(reference)

    # Default tailings facility. Every mine gets one
    default_TSF = TailingsFacility(
      name = f"defaultTSF_{mine.name}".strip(),
      cmti_id = mine.cmti_id,
      status = row.Mine_Status,
      hazard_class = row.Hazard_Class,
      latitude = mine.latitude,
      longitude = mine.longitude,
      is_default = True,
    )
    default_TSF.mines.append(mine)
    records.append(default_TSF)

    # Default impoundment. Every default tailings facility gets one
    impountment_name = f"{mine.name.strip()}_defaultImpoundment"
    default_impoundment = Impoundment(
      name=impountment_name,
      parentTsf = default_TSF,
      parent_tsf_id=default_TSF.cmti_id,
      is_default = True,
      area = row.Tailings_Area,
      area_from_images = row.Tailings_Area_From_Images,
      area_notes = row.Tailings_Area_Notes,
      raise_type = row.Raise_Type,
      volume = row.Tailings_Volume,
      capacity = row.Tailings_Capacity,
      storage_method = row.Tailings_Storage_Method,
      max_height = row.Current_Max_Height,
      acid_generating = row.Acid_Generating,
      treatment = row.Treatment,
      rating_index = row.Rating_Index,
      stability_concerns = row.History_Stability_Concerns
    )
    records.append(default_impoundment)
    records.append(mine)

    return records

  def process_tsf(self, row:pd.Series, parent_mines:Mine | list[Mine]):
    tsf = TailingsFacility(
      name = row.Site_Name,
      cmti_id = row.CMTI_ID,
      status = row.Mine_Status,
      hazard_class = row.Hazard_Class,
      latitude = row.Latitude,
      longitude = row.Longitude,
      is_default = False
    )
    if isinstance(parent_mines, list):
      for mine in parent_mines:
        tsf.mines.append(mine)
    else:
      tsf.mines.append(parent_mines)
    return tsf
  
  def process_impoundment(self, row:pd.Series, parent_TSF:TailingsFacility):
    impoundment = Impoundment(
      name = row.Site_Name,
      cmti_id = row.CMTI_ID,
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
  def __init__(self, cm_list:list='config', metals_dict:dict='config', name_convert_dict:dict='config', force_dtypes:bool=True):
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
  
  def clean_input_table(self, input_table, drop_NA_columns=['MDI_IDENT', 'NAME', 'LONGITUDE', 'LATITUDE'], force_dtypes=True, clean_input_table=False):
    omi_dtypes = {
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

    omi_defaults = ["Unknown" if t == "U" else pd.NA for t in omi_dtypes]
    omi_types_table = pd.DataFrame(data={'Column': omi_dtypes.keys(), 'Type': omi_dtypes.values(), 'Default': omi_defaults})

    converters = converter_factory(omi_types_table).create_converter_dict()

    if isinstance(input_table, str):
      try:
        omi_df = pd.read_csv(input_table, header=0, converters=converters)
      except:
        omi_df = pd.read_excel(input_table, header=0, converters=converters)
    elif isinstance(input_table, pd.DataFrame):
      omi_df = input_table
    
    # Drop rows that are missing critical values in the drop_NA_columns list before converting types
    omi_df = omi_df.dropna(subset=drop_NA_columns)
    
    # Apply converters for initial cleanup
    for col, func in converters.items():
      try:
        omi_df[col] = omi_df[col].apply(func)
      except ValueError as ve:
        raise ve
      except KeyError:
        print(f"Column {col} not found in input table.")
        raise

    # Enforce types
    if force_dtypes:
      omi_df = self.coerce_dtypes(omi_types_table, omi_df)

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
      mine = Mine(
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
        if pd.notna(row[comm_col]):
          comm_record = tools.get_commodity(row, comm_col, self.cm_list, self.name_convert_dict, self.metals_dict, mine)
          row_records.append(comm_record)

      # Default TSF
      tsf = TailingsFacility(is_default = True, name = f"default_TSF_{mine.name}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)

      # Default Impoundment
      impoundment = Impoundment(
        parentTsf=tsf,
        parent_tsf_id=tsf.cmti_id,
        is_default = True, name = f"{tsf.name}_impoundment"
      )
      row_records.append(impoundment)

      omi_reference = Reference(mine=mine, source = "OMI", source_id = row["MDI_IDENT"], link=row['DETAIL'])
      row_records.append(omi_reference)

      return row_records
    except Exception as e:
      raise e

class OAMImporter(DataImporter):
  def __init__(self, oam_comm_names:dict='config', cm_list='config', metals_dict='config', name_convert_dict='config'):
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

    super(OAMImporter, self).__init__(cm_list=cm_list, metals_dict=metals_dict, name_convert_dict=name_convert_dict)
    
    if oam_comm_names == 'config':
      # Load OAM commodity names from a CSV file
      oam_comm_path = BASE_DIR / self.config['supplemental']['oam_comm_names']
      oam_comm_data = pd.read_csv(oam_comm_path)
      oam_comm_names = dict(zip(oam_comm_data['Symbol'], oam_comm_data['Full_Name']))
      
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

  def clean_input_table(self, input_table, drop_NA_columns=['OID', 'Lat_DD', 'Long_DD', 'Name'],  force_dtypes=True, convert_units=False):
    oam_dtypes = {
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
      'Last_Year': 'Int64',
      'Permit': 'U',
      'URL': 'U',
      'Forcing_Features': 'U',
      'Feature_References': 'U',
      'Feature_Class': 'U',
      'Location': 'U',
      'County': 'U',
      'Landowner': 'U',
      'Last_Operator': 'U',
      'Start_Date': 'Int64',
      'Peak_Production': 'U',
      'Last_Updated': 'f4'
  }

    # Take keys and values as columns and types for dataframe
    # Set default values based on datatype
    oam_defaults = ["Unknown" if t == "U" else pd.NA for t in oam_dtypes]
    oam_types_table = pd.DataFrame(data={'Column': oam_dtypes.keys(), 'Type': oam_dtypes.values(), 'Default': oam_defaults})
    conversion_dict = None # Placeholder for unit conversion dictionary if needed.
    
    converters = converter_factory(oam_types_table, conversion_dict).create_converter_dict()

    if isinstance(input_table, str):
      try:
        oam_df = pd.read_csv(input_table, header=0, converters=converters)
      except:
        oam_df = pd.read_excel(input_table, header=0, converters=converters)
    else:
      oam_df = input_table

    # Drop rows that are missing critical values in the drop_NA_columns list before converting types
    oam_df = oam_df.dropna(subset=drop_NA_columns)

    # Apply converters for initial cleanup
    for col, func in converters.items():
      try:
        oam_df[col] = oam_df[col].apply(func)
      except ValueError as ve:
        raise ve
      except KeyError as ke:
        print(f"Column {col} not found in input table.")
        pass

    # Coerce types
    if force_dtypes:
      oam_df = self.coerce_dtypes(oam_types_table, oam_df)

    return oam_df

  def create_row_records(self, row: pd.Series, oam_comm_names:dict=None, cm_list:list=None, metals_dict:dict=None, name_convert_dict:dict=None):
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
      mine = Mine(
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

      impoundment = Impoundment(
        parentTsf = tsf,
        parent_tsf_id=tsf.cmti_id, 
        is_default = True, 
        name = f"{tsf.name}_impoundment")
      row_records.append(impoundment)

      if pd.notna(row['Last_Operator']):
        owner = Owner(name = row["Last_Operator"])
        owner_association = OwnerAssociation(owner=owner, mine= mine, is_current_owner=False)
        mine.owners.append(owner_association)
        row_records.append(owner_association)

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

  def clean_input_table(self, input_table, drop_NA_columns=['OBJECTID', 'MINFILNO', 'NAME1', 'LATITUDE', 'LONGITUDE'], calculate_UTM=True, force_dtypes=True, convert_units=False):
    bcahm_dtypes = {
      "OBJECTID": "u4",
      "MINFILNO": "U",
      "NAME1": "U",
      "NAME2": "U",
      "STATUS": "U",
      "LATITUDE": "f4",
      "LONGITUDE": "f4",
      "UTM_ZONE": "Int64",
      "UTM_NORT": "Int64",
      "UTM_EAST": "Int64",
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
      "First_Year": "Int64",
      "Last_Year": "Int64"
    }

    bcahm_defaults = ["Unknown" if t == "U" else pd.NA for t in bcahm_dtypes]
    bcahm_types_table = pd.DataFrame(data={'Column': bcahm_dtypes.keys(), 'Type': bcahm_dtypes.values(), 'Default': bcahm_defaults})
    conversion_dict = None # Placeholder for unit conversion dictionary

    converters = converter_factory(bcahm_types_table, conversion_dict).create_converter_dict()

    if isinstance(input_table, str):
      try:
        bcahm_df = pd.read_excel(input_table, header=0, converters=converters)
      except:
        bcahm_df = pd.read_csv(input_table, header=0, converters=converters)
    else:
      bcahm_df = input_table

    # Drop rows that are missing critical values in the drop_NA_columns list before converting types
    bcahm_df = bcahm_df.dropna(subset=drop_NA_columns)

    # Apply converters for initial cleanup
    for col, func in converters.items():
      try:
        bcahm_df[col] = bcahm_df[col].apply(func)
      except ValueError as ve:
        raise ve
      except KeyError as ke:
        print(f"Column {col} not found in input table.")
        pass

    # Calculate UTM Zone
    if calculate_UTM:
      for row in bcahm_df.itertuples():
        if pd.isna(row.LONGITUDE):
          bcahm_df.at[row.Index, 'UTM_Zone'] = None
        elif pd.isna(row.UTM_ZONE):
          try:
            bcahm_df.at[row.Index, 'UTM_ZONE'] = tools.lon_to_utm_zone(row.LONGITUDE)
          except:
            print(f"Error calculating UTM Zone for row {row.Index}.")
            raise

    # Coerce types
    if force_dtypes:
      bcahm_df = self.coerce_dtypes(bcahm_types_table, bcahm_df)

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
      if (pd.isna(mine_vals["latitude"]) or mine_vals["latitude"] == 'Null') or (pd.isna(mine_vals["longitude"]) or mine_vals["longitude"] == 'Null'):
          raise ValueError("Latitude or Longitude missing from record.")
      
      # Check coordinates for null strings as well
      if pd.isna(mine_vals['northing']) or mine_vals['northing'] == 'Null':
        del(mine_vals['northing'])
      if pd.isna(mine_vals['easting']) or mine_vals['easting'] == 'Null':
        del(mine_vals['easting'])
      if pd.isna(mine_vals['utm_zone']) or mine_vals['utm_zone'] == 'Null':
        mine_vals['utm_zone'] = tools.lon_to_utm_zone(mine_vals['longitude'])
      
      mine = Mine(**mine_vals)
      row_records.append(mine)

      # Create alias if there's another name
      if pd.notna(row["NAME2"]):
        alias = Alias(mine=mine, alias=row["NAME2"])
        row_records.append(alias)
      
      # Commodities
      for comm_col in ['COMMOD_C1', 'COMMOD_C2', 'COMMOD_C3']:
        if pd.notna(row[comm_col]):
          commodity_record = tools.get_commodity(row, comm_col, cm_list, name_convert_dict, metals_dict, mine)
          row_records.append(commodity_record)

      # TSF
      tsf = TailingsFacility(is_default = True, name = f"default_TSF_{mine_vals['name']}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)

      # Impoundment
      impoundment = Impoundment(
        parentTsf=tsf,
        parent_tsf_id=tsf.cmti_id,
        is_default=True,
        name=f"{tsf.name}_impoundment"
      )
      row_records.append(impoundment)

      #Reference
      reference = Reference(mine = mine, source = "BCAHM", source_id = str(row.OBJECTID))
      row_records.append(reference)
      if row.MINFILNO != "Null":
        minefileref = Reference(mine = mine, source = "BC Minfile", source_id = row.MINFILNO)
        row_records.append(minefileref)

      # Orebody
      if row["DEPOSITTYPE_D1"] != "Null" and pd.notna(row["DEPOSITTYPE_D1"]):
        orebody = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D1"], ore_class = row["DEPOSITCLASS_D1"])
        row_records.append(orebody)
      if row["DEPOSITTYPE_D2"] != "Null" and pd.notna(row["DEPOSITTYPE_D2"]):
        orebody2 = Orebody(mine = mine, ore_type = row["DEPOSITTYPE_D2"], ore_class = row["DEPOSITCLASS_D2"])
        row_records.append(orebody2)

      return row_records
    except Exception as e:
      raise(e)
    
class NSMTDImporter(DataImporter):
  
  def __init__(self, name_convert_dict = 'config', cm_list = 'config', metals_dict = 'config'):
    super().__init__(name_convert_dict, cm_list, metals_dict)

  def clean_input_table(self, input_table, force_dtypes = True, convert_units=True):
    nsmtd_defaults = {
      'OBJECTID': 'Int64',
      'Name': 'U',
      'Latitude': 'f4',
      'Longitude': 'f4',
      'Tonnes': 'int',
      'Commodity': 'U',
      'Crusher1': 'Int64',
      'Crusher2': 'Int64',
      'Dates': 'U',
      'InfoSource': 'U',
      'AreaHa': 'f4',
      'Shape_Area': 'f4'}

    nsmtd_types_table = pd.DataFrame(data={'Column': nsmtd_defaults.keys(), 'Type': nsmtd_defaults.values(), 'Default': nsmtd_defaults.values()})
    if convert_units:
      unit_converters = {'AreaHa': 'km2'}
      dimless_units = {'dimensionless_value_unit': 'Ha'}
    else:
      unit_converters = None
      dimless_unit = None

    converters = converter_factory(nsmtd_types_table, unit_conversion_dict=unit_converters, kwargs=dimless_units).create_converter_dict()

    if isinstance(input_table, str):
      try:
        nsmtd_df = pd.read_excel(input_table, header=0, converters=converters)
      except:
        nsmtd_df = pd.read_csv(input_table, header=0, converters=converters)
    else:
      nsmtd_df = input_table

    if force_dtypes:
      nsmtd_df = self.coerce_dtypes(nsmtd_types_table, nsmtd_df)
    
    return nsmtd_df
  
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
    if cm_list is None:
      cm_list = self.cm_list
    if metals_dict is None:
      metals_dict = self.metals_dict
    if name_convert_dict is None:
      name_convert_dict = self.name_convert_dict

    row_records = []
    try:

      mine_vals = {
        "name": row['Name'],
        "latitude": row["Latitude"],
        "longitude": row["Longitude"],
        "prov_terr": "NS",
        "mine_status": "Inactive"
      }
      # Parse date range
      if pd.notna(row["Dates"]):
        dates = []
        for date in row["Dates"]:
          try:
              eras = date.split(",")
              for era in eras:
                era_dates = era.split("-")
                if len(era_dates) == 4: # Sometimes written as, e.g., 1850-65
                  date_ints = [int(d) for d in era_dates]
                  dates.append(date_ints)
          except:
            raise
        if len(dates) > 0:
          mine.start_year = min(dates)
          mine.end_year = max(dates)

      mine = Mine(**mine_vals)
      row_records.append(mine)

      # Aliases
      alias_name = row['Name'].split('(')[0].strip()
      alias = Alias(mine=mine, alias=alias_name)
      row_records.append(alias)

      # Commodities
      comms = row["Commodity"].split(",")
      for comm_name in comms:
        if pd.notna(comm_name):
          comm_name = tools.convert_commodity_name(comm_name, name_convert_dict, output_type='symbol', show_warning=False)
          commodity_record = CommodityRecord(
            mine=mine,
            commodity=comm_name
          )
          commodity_record.is_critical = True if comm_name in cm_list else False
          commodity_record.is_metal = metals_dict.get(comm_name)
          row_records.append(commodity_record)

      # TSF
      tsf = TailingsFacility(is_default = True, name = f"default_TSF_{mine_vals['name']}".strip())
      mine.tailings_facilities.append(tsf)
      row_records.append(tsf)

      # Impoundment
      impoundment_vals = {
        "name": f"{tsf.name}_impoundment",
        "parentTsf": tsf,
        "parent_tsf_id": tsf.cmti_id,
        "is_default": True,
        "area": row["AreaHa"], # If running clean_input_table, this will be in km2
        "volume": row["Tonnes"]
      }
      impoundment = Impoundment(**impoundment_vals)
      row_records.append(impoundment)

      #Reference
      reference = Reference(mine = mine, source = "NSMTD", source_id = row['OBJECTID'])
      row_records.append(reference)

      return row_records
    except Exception as e:
      raise(e)