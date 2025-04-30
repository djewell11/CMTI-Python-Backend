import pandas as pd
from pint import UnitRegistry

def check_categorical_values(row:pd.Series, qa_dict:dict, ignore_unknown=True, ignore_na=True, ignore_blank=True):
  """
  Verifies that value given matches list of approved values in template. #TODO determine behaviour for bad values (currently prints to console).

  :param row: A row from an input DataFrame.
  :type row: pandas.Series

  :param qa_dict: A dictionary where keys are columns and values are lists of approved strings for those columns.
  :type qa_dict: dict

  :param ignore_unknown: Whether to flag entries where value == 'Unknown' or 'N/A/Unknown'. Default: True.
  :type ignore_unknown: bool

  :param ignore_na: Whether to flag entries where value is an NA type. Default: True.
  :type ignore_na: bool

  :param ignore_blank: Whether to flag entries where value == ''. Default: True.
  :type ignore_blank: bool
  """
  # qa_dict keys are columns, values are allowed values
  # This function flags errant values but doesn't change them
  def print_bad_value(key, val):
    print(f"{row.Site_Name} -- {key}: {val}")

  for col_key, value_list in qa_dict.items():
    if pd.isna(row[col_key]) and ignore_na:
      pass
    else:
      col_values = [val.strip() for val in str(row[col_key]).split(',')]
      for col_value in col_values:
        if col_value not in value_list:
          if ignore_unknown and str(col_value) in ['Unknown', 'N/A/Unknown']:
            pass
          elif ignore_blank and len(col_value) == 0:
            pass
          else:
            print_bad_value(col_key, col_value)

def convert_unit(value, desired_unit:str, dimensionless_value_unit:str = None, ureg:UnitRegistry = None):
  """
  Converts a value to a desired unit using a pint.UnitRegistry object.

  :param value: The input value.
  :type value: int, float, or str

  :param desired_unit: The desired output unit. If None, the usual defaults are used.
  :type desired_unit: str

  :param ureg: A pint.UnitRegistry object.
  :type ureg: pint.UnitRegistry

  :param dimensionless_value_unit: The unit of the input value if dimensionless. If None, dimensionless values are ignored. Will not overwrite strings with dimensions. Default: None.
  :type dimensionless_value_unit: str 

  :return: The converted value.
  :rtype: int, float
  """

  # TODO: This function is extremely slow and needs optimizing. It currently tries to convert every value entered.

  from pint import DimensionalityError
  from pint import UndefinedUnitError

  if ureg is None:
    ureg = UnitRegistry()
    ureg.define('km2 = kilometer ** 2')
    ureg.define('m2 = meter ** 2')
    ureg.define('Ha = hectare = 10000 m2')
    ureg.define('m3 = meter ** 3')

  Q = ureg.Quantity

  # Pint doesn't like None values. Exit early if value is None.
  if pd.isna(value) or desired_unit is None:
    return value
  
  value = value.replace(' ', '') if isinstance(value, str) else value
  try:
    # Try a simple conversion, if value contains unit
    return Q(value).to(desired_unit).magnitude
  except DimensionalityError:
    try:
      # If value has no dimension, but a dimensionless unit is provided, try to add it
      if dimensionless_value_unit is not None:
        # This should handle strings or stringified numbers
        value_dim = f'{value} {dimensionless_value_unit}'
        return Q(value_dim).to(desired_unit).magnitude
      else:
        # If no dimensionless unit is provided, return the value as is
        return value
    except:
      # This shouldn't be reachable, but if it is, raise an error
      raise(f"Could not convert value: {value} to desired unit.")
  except UndefinedUnitError:
    # If the unit is not defined, return the value as is
      return value
  except:
    raise
    
# Data Grading

class DataGrader:

  def __init__(self, main:dict, comms:dict, years:dict, source:dict, comm_col_count=8, source_col_count=4):
    self.main = main
    self.comms = comms
    self.commodity_cols = [f"Commodity{n}" for n in range(1, comm_col_count+1)]
    self.years = years
    self.source = source
    self.source_col_count = source_col_count
    self.source_cols = self.__create_source_columns()
    self.perfect_score=0
    self.perfect_score = self.__perfect_score()


  # Init methods
  def __create_source_columns(self):
    source_cols = []
    for source_n  in range(1, self.source_col_count+1):
      source_cols += [f"Source_{source_n}", f"Source_{source_n}_ID", f"Source_{source_n}_Link"]
      return source_cols

  def perfect_row(self):
    all_cols = {}
    for col_dict in [self.main, self.comms, self.years, self.source]:
      cols = col_dict.keys()
      all_cols.update(dict(zip(cols, [True for n in range(len(cols))])))

    full_row = pd.Series(all_cols)
    return full_row

  def __perfect_score(self):
    perfect_score = 0
    for point_dict in [self.main, self.comms, self.years, self.source]:
      for points in point_dict.values():
        perfect_score += points
    return perfect_score

  # Points calculation methods. These could be used individually, but it's easier to just call self.assign_score()

  def calculate_main_values(self, row):
    initial_points = 0
    for key in self.main.keys():
      entry = row[key]
      if pd.notna(entry) and entry != 'Unknown':
        initial_points += self.main[key]
    return initial_points

  def calculate_commodity_values(self, row):
    comm_points_list = []
    cols_with_values = set()
    for commodity_col in self.commodity_cols:
      comm = row[commodity_col]
      comm_points = 0
      if pd.notna(comm):
        comm = comm.strip()
        comm_points += self.comms['Commodity']
        if f"{comm}_Produced" in row.index.to_list() and pd.notna(row[f"{comm}_Produced"]):
          comm_points += self.comms['Commodity_Produced']
        if f"{comm}_Contained" in row.index.to_list() and pd.notna(row[f"{comm}_Contained"]):
          comm_points += self.comms['Commodity_Contained']
        if f"{comm}_Grade" in row.index.to_list() and pd.notna(row[f"{comm}_Grade"]):
          comm_points += self.comms['Commodity_Grade']
      comm_points_list.append(comm_points)

    return max(comm_points_list)

  def calculate_year_values(self, row):
    for col in self.years.keys():
      col_value = row[col]
      if pd.notnull(col_value) and col_value != 'Unknown':
        # The first time a value is encountered, return value and exit method
        year_points = self.years[col]
        return year_points
    return 0 # If no value encountered, no points returned

  def calculate_source_values(self, row):
    points_list = []
    for i in range(1, self.source_col_count+1):
      source_points = 0
      if pd.notna(row[f'Source_{i}']):
        source_points += self.source['Source']
        if pd.notna(row[f'Source_{i}_ID']):
          source_points += self.source['Source_ID']
        elif pd.notna(row[f'Source_{i}_Link']) and pd.isna(row[f'Source_{i}_ID']):
          source_points += self.source['Source_Link']

      points_list.append(source_points)
    return max(points_list)

  def assign_score(self, row):
    total_points = (
        self.calculate_main_values(row) +
        self.calculate_commodity_values(row) +
        self.calculate_year_values(row) +
        self.calculate_source_values(row)
        )
    score = round((total_points/self.perfect_score)*100, 2)
    return score