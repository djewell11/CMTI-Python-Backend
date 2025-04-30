import cmti_tools.idmanager as idm

def test_prov_id_init():
    """
    Tests prov_id initialization and ensures code is correctly set
    """
    # Create ProvID instance and assert attributes are correct
    prov = idm.ProvID('ON')
    assert prov.code == "ON"
    assert prov.max_id == 0
    assert isinstance(prov.formatted_id, str)

def test_update_id():
    """
    Tests the update_id method in ProvID
    Confirms max_id and formatted_id update as expected
    """
    prov = idm.ProvID('ON')
    new_id = prov.max_id + 1
    prov.update_id(new_id)
    assert prov.max_id == 1
    assert prov.formatted_id == f"{prov.code}{prov.max_id:06d}"

def test_query_id():
    """
    Tests the query_ids method in ProvID
    Confirms it returns the highest ID for a given provID
    """
    prov = idm.ProvID('ON')
    ids = ["ON000001", "ON000002", "ON000003"]
    max_id = prov.get_max_id(ids)
    assert max_id == 3

def test_id_manager_init():
    """
    Tests ID_Manager initialization and ensures all ProvID objects are created correctly
    """
    id_manager = idm.ID_Manager()
    assert isinstance(id_manager.ON, idm.ProvID)
    assert id_manager.ON.code == 'ON'

def test_update_ids():
    id_manager = idm.ID_Manager()

    ids = ["ON000001", "ON000002", "ON000003", "QC000001", "QC000002", "NB000001"]
    id_manager.update_prov_ids(ids)

    assert id_manager.ON.max_id == 3
    assert id_manager.QC.max_id == 2
    assert id_manager.NB.max_id == 1