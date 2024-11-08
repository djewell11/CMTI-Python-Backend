from cmti_tools.tools import get_digits, convert_commodity_name, lon_to_utm_zone

def test_get_digits():
    assert get_digits("100m", output="int") == 100
    assert get_digits("50g/L") == 50.0

def test_convert_commodity_name():
    convert_dict = {"Au": "Gold", "Cu": "Copper"}
    assert convert_commodity_name("Au", convert_dict) == "Gold"
    assert convert_commodity_name("Copper", convert_dict, output_type="symbol") == "Cu"

def test_lon_to_utm_zone():
    assert lon_to_utm_zone(-128.12) == 9
