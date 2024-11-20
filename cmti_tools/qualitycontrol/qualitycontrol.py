import pandas as pd
from pint import UnitRegistry

ureg = UnitRegistry()
ureg.define('m3 = meter ** 3')

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

def check_units(value: int|float, expected_unit: str):
  """
  Conforms units for quantified values, removes unit from string, and returns a numerical value.

  :param value: The input value.
  :type value: int or float

  :param expected_unit: The desired output unit.
  :type expected_unit: str

  """
  try:
    val = value.replace(' ', '')
    quantity = ureg(val)
    converted = quantity.to(expected_unit)
    return converted.magnitude
  except Exception as e:
    pass

# Data Grading

points_assignment = {
    'Mine_Type': 3,
    'Mine_Status': 4,
    'Owner_Operator': 5,
    'Dev_Stage': 3,
    'Site_Access': 2,
    'Orebody_Type': 1,
    'Orebody_Class': 1,
    'Ore_Minerals': 2,
    'Ore_Processed': 4,
    'Forcing_Features': 1,
    'Raise_Type': 2,
    'History_Stability_Concerns': 2,
    'Acid_Generating': 2,
    'Current_Max_Height': 4,
    'Tailings_Storage_Method': 4,
    'Tailings_Volume': 5,
    'Tailings_Capacity': 5,
    'Tailings_Area': 5
  }

comm_dict = {
    'Commodity': 4,
    'Commodity_Grade': 5,
    'Commodity_Contained': 3,
    'Commodity_Produced': 2
  }

year_dict = {
    # Values for all keys should be the same
    'Construction_Year': 3,
    'Year_Opened': 3
  }

source_dict = {
    'Source': 2,
    'Source_Link': 3,
    'Source_ID': 5
  }

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
        comm_points += comm_dict['Commodity']
        if f"{comm}_Produced" in row.index.to_list() and pd.notna(row[f"{comm}_Produced"]):
          comm_points += comm_dict['Commodity_Produced']
        if f"{comm}_Contained" in row.index.to_list() and pd.notna(row[f"{comm}_Contained"]):
          comm_points += comm_dict['Commodity_Contained']
        if f"{comm}_Grade" in row.index.to_list() and pd.notna(row[f"{comm}_Grade"]):
          comm_points += comm_dict['Commodity_Grade']
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
        source_points += source_dict['Source']
        if pd.notna(row[f'Source_{i}_ID']):
          source_points += source_dict['Source_ID']
        elif pd.notna(row[f'Source_{i}_Link']) and pd.isna(row[f'Source_{i}_ID']):
          source_points += source_dict['Source_Link']

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