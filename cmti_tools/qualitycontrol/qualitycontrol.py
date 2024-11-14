import pandas as pd
from pint import UnitRegistry

ureg = UnitRegistry()
ureg.define('m3 = meter ** 3')

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

def check_units(value, expected_unit, ignore_unknown=True, ignore_na=True, ignore_blank=True):
  # Conforms units for quantified values, removes unit from string, and returns a numerical value
  try:
    val = value.replace(' ', '')
    quantity = ureg(val)
    converted = quantity.to(expected_unit)
    return converted.magnitude
  except Exception as e:
    pass