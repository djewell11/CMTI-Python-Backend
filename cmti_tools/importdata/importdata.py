import os
import pandas as pd
from configparser import ConfigParser
from datetime import datetime
from sqlalchemy.orm import DeclarativeBase # Imported for typehints
from abc import ABC, abstractmethod

import cmti_tools.tools as tools
from cmti_tools.idmanager import ID_Manager
from cmti_tools.qualitycontrol import convert_unit

pd.options.mode.chained_assignment = None

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

  def __init__(self, types_table, unit_conversion_dict:dict=None, **kwargs):
    """
    :param types_table: A DataFrame with columns "Column", "Type", and "Default". Column values must be unique.
    :type types_table: pandas.DataFrame.

    :param unit_conversion_dict: A dictionary of unit conversions for specific columns. Key = column, value = desired unit. Default: None.
    """
    self.types_table = types_table
    # Replace "None" with an empty dict if no unit_conversion_dict is provided to avoid errors in create_converter.
    self.unit_conversion_dict = unit_conversion_dict if unit_conversion_dict is not None else {}
    self.kwargs = kwargs

  def create_converter(self, column:str):
    """
    Creates a function for the input column that either returns the default or performs some cleanup action.

    :param column: The Column value from types_table.
    :type column: str.

    :return: Function
    """
    col_dtype = self.types_table.loc[self.types_table.Column == column, 'Type'].values[0]
    default = self.types_table.loc[self.types_table.Column == column, 'Default'].values[0]

    dimensionless_value_units = self.kwargs.get('dimensionless_value_unit', {})
    
    # Create a converter function based on dtype
    match col_dtype:

      # Create a function for each dtype. If the value is NA, return the default value. If column has a unit_conversion, convert the value.
      case 'Int64' | 'UInt64' | 'u' | 'u4' | 'u8' | 'int':
        def get_int(val):
          if pd.isna(val):
            return default
          if column in self.unit_conversion_dict.keys():
            val = convert_unit(val, desired_unit=self.unit_conversion_dict.get(column), dimensionless_value_unit=dimensionless_value_units.get(column))
          if isinstance(val, str):
            val = tools.get_digits(val, 'int')
          elif isinstance(val, float):
            val = round(val)
          return val
        return get_int
      
      case 'f' | 'float' | 'float64' | 'f4':
        def get_float(val):
          if pd.isna(val):
            return default
          if column in self.unit_conversion_dict.keys():
            val = convert_unit(val, desired_unit=self.unit_conversion_dict.get(column), dimensionless_value_unit=dimensionless_value_units.get(column))
          if isinstance(val, str):
            val = tools.get_digits(val, 'float')
          return val
        return get_float
      
      case 'U':
        def get_str(val):
          if pd.isna(val):
            return default
          if isinstance(val, str):
            return val.strip()
          return val
        return get_str
      
      case 'datetime64[ns]':
        def get_datetime(val):
          if pd.isnull(val):
            return datetime.now()
          return val
        return get_datetime
      
      case _:
        raise ValueError(f"Invalid dtype for column/value: {column} / {col_dtype}")

  def create_converter_dict(self):
    """
    Runs create_converter for all columns in types_table.Column.

    :return: dict.
    """
    converters = {}
    # Fill converters dict with a function for each column
    for i, row in self.types_table.iterrows():
      converters[row.Column] = self.create_converter(row.Column)
    return converters

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
  def create_row_records(self, row: pd.Series) -> None:
    """
    Process a single row and generates a DeclarativeBase objects based on inputs. 
    Implemented by child classes.
    """
    pass

  @abstractmethod
  def clean_input_table(self, input_table:pd.DataFrame, force_dtypes:bool=True, convert_units:bool=True) -> pd.DataFrame:
    """
    Clean values in the input table and make column data types consistent.
    """
    pass

  def series_to_table(self, table:DeclarativeBase, series:pd.Series, datamapping:dict) -> DeclarativeBase:
    """
    Creates a table object from a pandas Series. Fetches table attributes from the series automatically based on mapping
    """
    table_values = {k: series[v] for k, v in datamapping.items() if v in series}
    return table(**table_values)

  def coerce_dtypes(self, input_types_table, input_table:pd.DataFrame) -> pd.DataFrame:
    """
    Coerces the data types of the input table based on the types_table.
    """

    # Final type coercion
    for column in input_table.columns:
      try:
        dtype_row = input_types_table[input_types_table['Column'] == column]
        if dtype_row.empty:
            raise KeyError(f"Column '{column}' not found in input_types_table.")
        dtype = dtype_row['Type'].iloc[0]
        if dtype.startswith('u') or dtype.startswith('i') or dtype.startswith('I'):
          input_table[column] = pd.to_numeric(input_table[column], errors='coerce').astype('Int64')
        elif dtype.startswith('f'):
          input_table[column] = pd.to_numeric(input_table[column], errors='coerce').astype('float')
      except Exception as e:
        print(f"Error coercing column {column}: {e}")
        raise
    return input_table
  
  def map_to_worksheet(self, worksheet:pd.DataFrame, source:pd.DataFrame, mapping:dict) -> pd.DataFrame:
    """
    Maps source DataFrame to worksheet DataFrame using the provided mapping dictionary.
    Creates a new DataFrame with the same columns as the worksheet and populates it with values from the source DataFrame.

    :param worksheet: The DataFrame to be populated.
    :type worksheet: pandas.DataFrame.

    :param source: The DataFrame to be mapped.
    :type source: pandas.DataFrame.

    :param mapping: A dictionary mapping source columns to worksheet columns.
    :type mapping: dict.

    :return: The populated worksheet DataFrame.
    """
    source_df = pd.DataFrame(columns=worksheet.columns)

    for source_col, worksheet_col in mapping.items():
      if source_col in source.columns:
        source_df[worksheet_col] = source[source_col]
      # else:
      #   raise KeyError(f"Column '{source_col}' not found in source DataFrame.")
    return source_df