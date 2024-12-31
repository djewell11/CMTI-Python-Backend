import pytest
from cmti_tools.idmanager import ProvID

def test_prov_id_init():
    """
    Tests prov_id initialization and ensures code is correctly set
    """
    # Create ProvID instance and assert attributes are correct
    prov = ProvID('ON')
    assert prov.code == "ON"
    assert prov.max_id >= 0
    assert isinstance(prov._formatted_id, str)

def test_update_id():
    """
    Tests the update_id method in ProvID
    Confirms max_id and formatted_id update as expected
    """
    prov = ProvID('ON')
    original_id = prov.max_id
    prov.update_id()
    assert prov.max_id == original_id + 1
    assert prov.formatted_id == f"{prov.code}{prov.max_id:06d}"