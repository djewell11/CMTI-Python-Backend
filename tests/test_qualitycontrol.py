import pytest
import pandas as pd
from cmti_tools.qualitycontrol import check_categorical_values, check_units, DataGrader

def test_categorical_vals(capfd):
    """
    Tests the check_categorical_vals function.
    Ensures incorrect values are caught.
    """
    # Create a row with incorrect values
    qa_dict = {"Status": ["Active", "Inactive"]}
    row = pd.Series({"Site_Name": "Test Site", "Status": "Unknown"})
    check_categorical_values(row, qa_dict, ignore_unknown = False)

    # Read console output
    output = capfd.readouterr()
    # Check that test_categorical_vals catches bad values
    assert "Test Site -- Status: Unknown" in output.out

def test_check_units():
    """
    Tests the check_units function.
    Confirms unit conversion from cubic meters to liters works correctly.
    """
    value = '1km2'
    unit = 'm2'
    # Convert units using check_units
    converted = check_units(value, unit)
    # Check if units were properly converted, allowing for rounding error
    assert converted == pytest.approx(1_000_000, 0.1)

# Initialize DataGrader with a custom scoring criteria
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
    """
    Tests DataGrader's perfect_row method.
    Verifies that all required columns are correctly flagged as True.
    """
    row = grader.perfect_row()

    assert row['Mine_Type'] == True
    assert row['Commodity'] == True
    assert row['Construction_Year'] == True
    assert row['Source'] == True

def test_perfect_score():
    """
    Tests calculation of the perfect_score attribute.
    Confirms it sums all defined scoring weights correctly.
    """
    assert grader.perfect_score == (
        sum(grader.main.values()) +
        sum(grader.comms.values()) +
        sum(grader.years.values()) +
        sum(grader.source.values())
    )

def test_assign_score():
    """
    Tests DataGrader's assign_score method.
    Verifies that an example row receives the correct calculated score.
    """
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
    # Expected points based on input
    expected_points = 18

    # Expected score based on the formula used in qualitycontrol.py
    expected_score = round((expected_points/grader.perfect_score)*100, 2)
    assert score == expected_score