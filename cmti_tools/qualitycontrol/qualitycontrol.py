import pandas as pd
<<<<<<< Updated upstream
from pint import UnitRegistry

ureg = UnitRegistry()
ureg.define('m3 = meter ** 3')
=======
from unit_converter.converter import convert
from pint import UnitRegistry
>>>>>>> Stashed changes

def check_categorical_values(row, qa_dict, ignore_unknown=True, ignore_na=True, ignore_blank=True):
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

# def _check_units(row, column_units: dict, ignore_unknown=True, ignore_na=True, ignore_blank=True):
#   # Conforms units for quantified values, removes unit from string, and returns a numerical value
#   for col, unit in column_units.items():
#     # Check for units
#     try:
#       val = str(row[col]).replace(' ', '')
#       converted = convert(val, unit)
#       row.col = converted
#     except Exception as e:
#       pass

def check_units(value:int|float|str, desired_unit:str, out_type:str="str", ignore_unkown:bool=True, ignore_na:bool=True, ignore_blank:bool=True):
  # Conforms units for quantified values, removes unit from string, and returns a numerical value
<<<<<<< Updated upstream
  for col, unit in column_units.items():
    # Check for units
    try:
      val = str(row[col]).replace(' ', '')
      quantity = ureg(row[col])
      converted = quantity.to(unit)
      row[col] = converted.magnitude
    except Exception as e:
      pass
=======
  unit_registry = UnitRegistry()
  unit_registry.define('[cm] = [volume] ** 3 = m ** 3')
  try:
    # Remove spaces
    value = unit_registry(value)
    converted = value.to(desired_unit).value
    if out_type == 'str':
      return str(converted)
    elif out_type == 'int':
      return int(converted)
    elif out_type == 'float':
      return float(converted)
  except Exception as e:
    print(e)
>>>>>>> Stashed changes
