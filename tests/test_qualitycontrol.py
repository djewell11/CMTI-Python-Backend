import pytest
import pandas as pd
from cmti_tools.qualitycontrol import check_categorical_values, check_units, DataGrader
from cmti_tools.qualitycontrol import points_assignment, comm_dict, year_dict, source_dict

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

grader = DataGrader(points_assignment, comm_dict, year_dict, source_dict)

def test_perfect_row():
    row = grader.perfect_row

    assert row['Mine_Type'] is True
    assert row['Commodity'] is True
    assert row['Construction_Year'] is True
    assert row['Source'] is True

def test_perfect_score():
    assert grader.perfect_score == 85

def test_assign_score():
    row = pd.Series({
        'Mine_Type': 'Underground',
        'Mine_Status': 'Closed',
        'Owner_Operator': None,
        'Commodity1': 'Gold',
        'Commodity1_Produced': 1000,
        'Construction_Year': 'Unknown',
        'Source_1': 'Source',
        'Source_1_ID': None,
        'Source_1_Link': 'https://...'
    })

    score = grader.assign_score(row)
    expected_points = 3+4+4+2+2+3

    expected_score = round((expected_points/grader.perfect_score)*100, 2)
    assert score == expected_score

def test_empty_row():
    row = pd.Series({})

    score = grader.assign_score(row)

    assert score == 0