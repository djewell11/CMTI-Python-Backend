import pytest
import pandas as pd
from cmti_tools.qualitycontrol.qualitycontrol import check_categorical_values, check_units

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
    row = pd.Series({"Capacity": "100 m3"})
    column_units = {"Capacity": "liter"}
    # Convert units using check_units
    check_units(row, column_units)
    # Check if units were properly converted, allowing for rounding error
    assert row.Capacity == pytest.approx(100000, 0.1)