from cmti_tools.tools import get_digits, convert_commodity_name, lon_to_utm_zone

def test_get_digits():
    """
    Tests the get_digits function.
    Extracts numeric values from strings and verifies correct types and results.
    """
    assert get_digits("100m", output="int") == 100
    assert get_digits("50g/L") == 50.0

def test_convert_commodity_name():
    """
    Tests the convert_commodity_name function.
    Ensures proper conversion between commodity symbols and names and vice versa.
    """
    convert_dict = {"Au": "Gold", "Cu": "Copper"}
    assert convert_commodity_name("Au", convert_dict) == "Gold"
    assert convert_commodity_name("Copper", convert_dict, output_type="symbol") == "Cu"

def test_lon_to_utm_zone():
    """
    Tests the lon_to_utm_zone function.
    Verifies correct UTM zone calculation based on longitude.
    """
    assert lon_to_utm_zone(-128.12) == 9
