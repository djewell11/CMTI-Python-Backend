from cmti_tools import get_commodity
from cmti_tools import convert_commodity_name
from cmti_tools.tables import Mine
from pandas import Series

def test_get_commodity():
    parent_mine = Mine(name='Big Mine', cmdb_id='ON00001', prov_terr='ON', latitude=65., longitude=-65.37,)
    row = Series({'Commodity': 'Copper', 'Cu_Grade': 2.5, 'Cu_Produced':100, 'Cu_Contained': 10_000})
    critical_mineral_list=['Copper']
    name_convert_dict = {'Cu': 'Copper'}
    metals_dict = {'Cu': 'metal'}

    comm = get_commodity(row, 'Commodity', critical_mineral_list, name_convert_dict, metals_dict, parent_mine)
    assert(comm.commodity == 'Copper')
    assert(comm.mine.name == 'Big Mine')
    assert(comm.grade == 2.5)
    assert(comm.produced == 100)
    assert(comm.contained == 10_000)
    assert(comm.is_critical == True)
    assert(comm.metal_type == 'metal')