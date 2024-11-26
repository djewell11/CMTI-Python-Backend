from cmti_tools import convert_commodity_name

def test_convert_commodity_name():
    name = 'au'
    name_convert_dict = {'Au': 'Gold'}
    converted_name = convert_commodity_name(name, name_convert_dict)

    assert(converted_name == 'Gold')