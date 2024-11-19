import csv
import pathlib
from configparser import ConfigParser
from configparser import Error as ConfigError
import pandas as pd
from warnings import warn
from math import ceil
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from cmti_tools.tables import *
from cmti_tools.idmanager import *

def create_name_dict(elements_csv) -> dict:
  elements = pd.read_csv(elements_csv)
  name_convert_dict = dict(zip(elements['symbol'], elements['name']))
  return name_convert_dict

# Load data files from config parser
def create_module_variables() -> dict:
  """
  Generate variables used in various places across the package. Configure file locations in config.toml.

  :return: dict

  """

  config = ConfigParser()
  config_path = pathlib.Path(__file__).parent.absolute() / "config.toml"
  config.read(config_path)

  with open(config.get('sources', 'critical_minerals'), mode='r') as critical_minerals_file:
    critical_minerals = pd.read_csv(critical_minerals_file, header=0)
  with open(config.get('sources', 'metals'), mode='r') as metals_file:
    metals = pd.read_csv(metals_file, header=0, encoding='utf-8')
    metals_dict = dict(zip(metals['Commodity'], metals['Name']))
  with open(config.get('sources', 'elements'), mode='r') as elements_file:
    name_convert_dict = create_name_dict(elements_file)
  return {"cm_list":critical_minerals, "metals_dict":metals_dict, "name_convert_dict":name_convert_dict}

try:
  data_tables = create_module_variables()
except ConfigError as config_error:
  print(config_error)
except Exception as e:
  print(e)

# try:
#   engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/cmti')
# except:
#   print("Create Engine Failed")
# Session = sessionmaker(bind=engine)
# session = Session()


def get_digits(value: str, output: str = 'float'):
  """
  Used for columns that contain quantities and may have erroneously included units.

  :param value: A quantity that includes numerical values.
  :type value: str.jkmn

  :param output: The output type. Either 'float' or 'int'. Default: 'float'.
  :type output: str.

  :return: A numerical value with units removed.
  :rtype: Either float or int, according to param 'output'.
  """
  stripped = "".join(list(filter(lambda x: x.isdigit(), value.replace(' ', ''))))
  try:
    if output == 'float':
      return float(stripped)
    elif output == 'int':
      return int(stripped)
    else:
      raise ValueError("'output' must be 'float' or 'int'")
  except ValueError as e:
    pass

def convert_commodity_name(name:str, name_convert_dict:dict, output_type:str="full", show_warning=True):
  """
  Takes element names and converts them to either symbol or full name. Ignores names not found in name_convert_dict.

  :param name: The commodity name.
  :type value: str.

  :param name_convert_dict: A dictionary where keys are symbols and values are full names.
  :type name_convert_dict: dict.

  :param output_type: The type of output desired. Default: "full".
  :type output_type: str.

  :param show_warning:
    Determines whether or not a warning is printed when "name" isn't present in "name_convert_dict".
    Absences are expected for non-element commodities. Default: True
  :type name_convert_dict: bool.
  """
  # _name = name # Save original name in case no match is found. Capitalize name to account for input differences
  name = name.strip().capitalize()

  cap_dict = {}
  for symbol, comm in name_convert_dict.items():
    cap_dict[symbol.capitalize()] = comm

  if output_type == "full":
    # Convert symbol to full name
    commName = cap_dict.get(name, None)
    if commName is None:
      # If name isn't an element, assume it's a mineral and return the original. Misspellings will get through.
      if show_warning:
          warn(f"Could not convert {name}")
      return name
    else:
      return commName
  elif output_type == "symbol":
    # Convert full name to symbol
    el_dict_reversed = {value: key for key, value in cap_dict.items()}
    commName = el_dict_reversed.get(name, None)
    if commName is None:
      if show_warning:
          warn(f"Could not convert {name}")
      return name # If name isn't in cap_dict, return original name.
    else:
      return commName
  else:
    raise ValueError("output_type must be either 'full' or 'symbol'")
  
def get_commodities(row:pd.Series, commodity_columns:list, critical_mineral_list:list, name_convert_dict:dict, metals_dict:dict, mine:Mine, session:Session, name_type:str="full"):
  """
  Takes multiple commodity columns from the spreadsheet and creates a Commodity object.
  Adds that object to the database. #TODO: Make this return a commodity rather than add it to session.

  :param row: A dataframe row.
  :type row: pandas.Series.

  :param commodity_columns: A list of column names containing commodity values.
  :type commodity_columns: list.

  :param critical_mineral_list: A list of critical minerals.
  :type critical_mineral_list: list.

  :param name_convert_dict: A dictionary that can convert commodity element symbols to full names or vice versa.
  :type name_convert_dict: dict.

  :param metals_dict: A dictionary that determines whether a commodity is a non-metal, metal, or REE.
  :type metals_dict: dict.

  :param mine: An sqlalchemy ORM class of type Mine.
  :type mine: sqlalchemy.orm.DeclarativeBase.

  :param session: The database session.
  :type session: sqlalchemy.orm.Session

  :param name_type: The output style for the commodity name, as entered in convert_commodity_name.
    Either "full" or "symbol". Default: "full".
  :type name_type: str.

  :return: None
  """
  # commodityCols = list(filter(lambda x: x.startswith("Commodity"), dataframe.columns))
  # Check each "commodity" column in table to see if it has a value
  for col in commodity_columns:
    # If it has a value, create an ORM object. This commodity does not necessarily need to have quantities
    comm = row[col]
    if pd.notna(row[col]):
      commName = convert_commodity_name(comm.capitalize(), name_convert_dict, name_type)
      commodity = CommodityRecord(mine=mine, commodity=commName)
      # Check if metal and critical
      try:
        commodity.metal_type = metals_dict.get(commName)
      except KeyError:
        pass
      commodity.is_critical = True if commName in critical_mineral_list else False
      # Now try and attach quantities, if present
      try:
        # grade = get_comm_value(row, valCol)
        grade = row[f"{comm}_Grade"]
        if pd.notna(grade):
          commodity.grade = grade if isinstance(grade, (float, int)) else get_digits(grade)
      except KeyError:
        pass
      try:
        # produced = row[f"{comm}_Produced"]
        produced = row[f"{comm}_Produced"]
        if pd.notna(produced):
          commodity.produced = produced if isinstance(produced, (float, int)) else get_digits(produced)
      except KeyError:
        pass
      try:
        contained = row[f"{comm}_Contained"]
        if pd.notna(contained):
          commodity.contained = contained if isinstance(contained, (float, int)) else get_digits(contained)
      except KeyError:
        pass
      # Get record dates
      # dateStart = row["Record_Period_Start"]
      # dateEnd = row["Record_Period_End"]
      # if pd.notna(dateStart):
      #   commodity.source_year_start = dateStart
      # if pd.notna(dateEnd):
      #   commodity.source_year_end = dateEnd
      session.merge(commodity)
      session.flush()

def get_table_values(row:pd.Series, columnDict:dict, default_null:object=None):
  """
  Takes column values, set out in columnDict, and produces a new dictionary where key = database column and
  value = original (dataframe/excel) value. This dictionary can be used to create an ORM object via dict unpacking.

  :param row: A dataframe row.
  :type row: pandas.Series.

  :param columnDict: A dictionary where key = dataframe column name and value = database column name
  :type name_type: str.

  :param default_null: The value that will be added to the output dictionary if column value is missing.
  :type default_null: Any

  :return: A dictionary of table values.
  :rtype: dict.
  """
  # Use a dictionary to match DF column names to DB table columns
  valueDict = {}
  for df_column, db_attribute in columnDict.items():
    # df_value = row[df_column]
    df_value = row.get(df_column, pd.NA)
    if pd.notna(df_value):
      # If value exists in dataframe row, assign it to dict indicating database column
      if isinstance(df_value, str):
        df_value = df_value.strip()
      valueDict[db_attribute] = df_value
    else:
      valueDict[db_attribute] = default_null
  # Returns a dict where key = database column name and value = dataframe (pandas/excel) column value
  return valueDict

def value_to_range(value:int|float, unit_singular:str, unit_plural:str=None, intervals:list=[1, 10, 100, 1000, 10_000, 100_000, 1_000_000]):
  """
  Converts a single value to a string representing range.

  :param value: A numerical value.
  :type value: int or float.

  :param unit_singular: Name of unit if quantity is 1.
  :type unit_singular: str.

  :param unit_plural: Name of unit is quantity is more than 1.
  :type unit_plural: str. Default: None.

  :param intervals: A list of cutoff values to create range categories.
  :type intervals: list.
  """

  if unit_plural == None:
    unit_plural = unit_singular
  ranges = [range(intervals[i], intervals[i+1]) for i in range(0, len(intervals) - 1)]
  if value < intervals[0]:
      unit = unit_singular if intervals[0] == 1 else unit_plural
      return f"Under {intervals[0]} {unit}"
  elif value > intervals[-1]:
      return f"Over {intervals[-1]} {unit_plural}"
  else:
      for r in ranges:
          if value in r:
              return f"{r[0]:,} to {r[-1]:,} {unit_plural}"

def lon_to_utm_zone(lon_deg:float):
  """
  Takes the longitude in decimal degrees and returns the UTM zone as an int.
  Assumes coordinates are in the northern hemisphere.

  :param lon_deg: The longitudinal coordinate in decimal degrees. Include a negative sign for western hemisphere.
  :type lon_deg: float.
  """
  zone = ceil(((float(lon_deg) + 180)/6) % 60)
  return zone

def assign_totals(mine_site:Mine, column_name:str, session):
  """
  Queries the DB for all child TSFs and Impoundments and sums a numeric property.

  :param mine_site: A Mine object.
  :type mine_site: sqlalchemy.orm.DeclarativeBase of type Mine

  :param column_name: The name of the numeric column being summed. This column exists in the impoundment object.
  :type column_name: str.

  :param session: The database session.
  :type session: sqlalchemy.sessionmaker.Session

  :return: None
  """

  tsfs = mine_site.tailings_facilities
  impoundments = []
  for tsf in tsfs:
    tsf_impoundments = tsf.impoundments
    impoundments.append(tsf_impoundments)
  print(impoundments)
  column_sum = sum([impoundment.column_name for impoundment in impoundments])
  print(column_sum)
  categorized = value_to_range(column_sum)
  print(categorized)


def merge_to_session(session, row:pd.Series, orm_class:object, column_dict:dict):
  """
  Generate a table entry (ORM object) and add to an existing session. For adding data to the CMTI that doesn't come
  from the data-entry spreadsheet.

  :param session: An existing sqlalchemy session.
  :type session: sqlalchemy.orm.Session.

  :param row: A row of a pandas dataframe to be converted to an ORM object
  :type row: pandas.Series.

  :param orm_class: The ORM class being created and merged
  :type orm_class: sqlalchemy.orm.DeclarativeBase

  :param column_dict: Dictionary fed into get_table_values to convert pandas DataFrame to table values.
  Keys are DF columns and values are table values.
  :type column_dict: dict

  :return: None.
  """
  newValues = get_table_values(row, column_dict)
  newEntry = orm_class(**newValues)
  session.merge(newEntry)

def orm_to_csv(orm_class:object, out_name:str, session):
  """
  Exports an ORM class object as a csv.

  :param orm_class: An ORM object.
  :type orm_class: sqlalchemy.orm.DeclarativeBase

  :param out_name:
  The name of the output csv. Include .csv extension. Include full filepath if location other than working
  directory is desired.
  :type out_name: str.

  :param session: The sqlalchemy session.
  :type session: sqlalchemy.Session.

  :return: None.
  """
  query = session.query(orm_class)
  columns = orm_class.__table__.columns
  csvColumns = [col.key for col in columns]
  with open(out_name, 'w') as file:
    writer = csv.writer(file)
    # Write header (columns names)
    writer.writerow(csvColumns)
    # Get rows and write
    [writer.writerow([getattr(row, column.name) for column in orm_class.__mapper__.columns]) for row in query]
  session.close()

def db_to_dataframe(worksheet:pd.DataFrame, session, name_convert_dict, ignore_default_records:bool=True):

  """
  Converts database (in form of sqlalchemy Session) to a Pandas dataframe.

  :param worksheet: The original worksheet table used to generate the database, or a table with the desired columns.
  :type worksheet: pandas.Dataframe.

  :param session: An existing sqlalchemy session.
  :type session: sqlalchemy.orm.Session.

  :param ignore_default_records: Whether to ignore or use the "default" TSF and Impoundment values generated in the database. Default: true.
  :type ignore_default_records: bool.
  """

  # new_records = pd.DataFrame(columns=worksheet.columns)
  new_rows = []
  existing_ids = worksheet['CMIM_ID'].tolist()
  new_sites_stmt = select(Mine).filter(Mine.cmdb_id not in existing_ids)
  with session.execute(new_sites_stmt).scalars() as new_sites:
    for r in new_sites:
      new_row = {} # Each value is assigned to a dictionary

      # Direct values of mine table
      new_row['Site_Name'] = r.name
      new_row['Site_Type'] = 'Mine'
      new_row['CMIM_ID'] = r.cmdb_id
      new_row['NAD'] = 83
      new_row['UTM_Zone'] = r.utm_zone
      new_row['Easting'] = r.easting
      new_row['Northing'] = r.northing
      new_row['Latitude'] = r.latitude
      new_row['Longitude'] = r.longitude
      new_row['Country'] = "Canada"
      new_row['Province_Territory'] = r.prov_terr
      new_row['Mine_Type'] = r.mine_type
      new_row['Mining_Method'] = r.mining_method
      new_row['Mine_Status'] = r.mine_status
      new_row['Dev_Stage'] = r.development_stage
      new_row['Site_Access'] = r.site_access
      new_row['Construction_Year'] = r.construction_year

      # Values of children of mine object
      # Commodities
      comm_number = 1
      for comm in r.commodities:
        # Maintain list of existing commodities to avoid duplicates
        row_commodities = [new_row[f'Commodity{n}'] for n in range(1, comm_number)]
        comm_col = f'Commodity{comm_number}'
        code = convert_commodity_name(comm.commodity, name_convert_dict, 'symbol', show_warning=False)
        if code not in row_commodities:
          new_row[comm_col] = code
          new_row[f'{code}_Grade'] = comm.grade
          new_row[f'{code}_Produced'] = comm.produced
          new_row[f'{code}_Contained'] = comm.contained
          comm_number += 1
        else:
          # print(f"{comm} already in row")
          pass

      # Owner
      if len(r.owners) == 1:
        new_row['Owner'] = r.owners[0].name

      # Alias
      new_alias = ""
      for alias in r.aliases:
        new_alias = f"{new_alias}, {alias.alias}"
      new_row['Site_Aliases'] = new_alias

      # TSF and impoundment should get their own queries
      # check ignore_defaults

      # Impoundment

      # References
      source_number = 1
      for source in r.references:
        if source_number <= 4 : # Currently only storing 3 sources
          new_row[f'Source_{source_number}'] = source.source
          new_row[f'Source_{source_number}_ID'] = source.source_id
          new_row[f'Source_{source_number}_Link'] = source.link
          source_number += 1
        else:
          print(f"More than 4 sources detected for site {r.id}")

      new_rows.append(new_row)
  new_records = pd.DataFrame(new_rows)
  out_df = pd.concat([worksheet, new_records], axis=0, ignore_index=True, join='outer')
  return out_df