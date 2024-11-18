import pytest
import pandas as pd
from cmti_tools.qualitycontrol import check_categorical_values, check_units

def test_categorical_vals(capfd):
    # Create a row with incorrect category vals
    qa_dict = {"Status": ["Active", "Inactive"]}
    row = pd.Series({"Site_Name": "Test Site", "Status": "Unknown"})
    check_categorical_values(row, qa_dict, ignore_unknown = False)

    # Read console output
    output = capfd.readouterr()
    # Check that test_categorical_vals catches bad values
    assert "Test Site -- Status: Unknown" in output.out

def test_check_units():
    value = '100 m3'
    unit = 'liter'
    # Convert units using check_units
    converted = check_units(value, unit)
    # Check if units were properly converted, allowing for rounding error
    assert converted == pytest.approx(100000, 0.1)