import pytest
import pandas as pd
from cmti_tools.qualitycontrol import check_categorical_values, check_units, DataGrader

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

grader = DataGrader(
    main = {
        'Mine_Type': 3,
        'Mine_Status': 4,
        'Owner_Operator': 5
    },
    comms = {
        'Commodity': 4,
        'Commodity_Produced': 2,
        'Commodity_Grade': 5
    },
    years = {
        'Construction_Year': 3
    },
    source = {
        'Source': 2,
        'Source_Link':3,
        'Source_ID': 5
    },
    comm_col_count = 2, source_col_count = 1
)

def test_perfect_row():
    row = grader.perfect_row()

    assert row['Mine_Type'] == True
    assert row['Commodity'] == True
    assert row['Construction_Year'] == True
    assert row['Source'] == True

def test_perfect_score():
    assert grader.perfect_score == (
        sum(grader.main.values()) +
        sum(grader.comms.values()) +
        sum(grader.years.values()) +
        sum(grader.source.values())
    )

def test_assign_score():
    row = pd.Series({
        'Mine_Type': 'Underground',
        'Mine_Status': 'Closed',
        'Owner_Operator': None,
        'Commodity1': 'Au',
        'Au_Produced': None,
        'Au_Grade': None,
        'Commodity2': 'Ag',
        'Ag_Produced': 500,
        'Ag_Grade': None,
        'Construction_Year': 'Unknown',
        'Source_1': 'Source',
        'Source_1_ID': None,
        'Source_1_Link': 'https://...'
    })

    score = grader.assign_score(row)
    expected_points = 18

    expected_score = round((expected_points/grader.perfect_score)*100, 2)
    assert score == expected_score