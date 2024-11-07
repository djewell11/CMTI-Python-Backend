import pytest
from cmti_tools.tables import Mine, CommodityRecord, Owner, Alias

def test_mine_creation(session):
    mine = Mine(name="Test Mine", cmdb_id = "AB001")
    alias = Alias(alias="Test Alias", mine = mine)
    owner = Owner(name="Test Owner", mines = [mine])
    commodity = CommodityRecord(commodity="Gold", mine = mine)

    session.add(mine)
    session.commit()

    assert session.query(Mine).filter_by(name="Test Mine").first() is not None
    assert session.query(Alias).filter_by(alias="Test Alias").first() is not None
    assert session.query(Owner).filter_by(name="Test Owner").first() is not None
    assert session.query(CommodityRecord).filter_by(commodity="Gold").first() is not None