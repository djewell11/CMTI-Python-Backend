from sqlalchemy import select

from cmti_tools.tables import Mine
from cmti_tools.tables import Mine

class ProvID:
  """
  An object containing a string representing a provincial ID.

  Attributes:
    code (str): A two-letter code denoting the province or territory.
    max_id (int): The largest existing ID per that code. i.e., the integer to start with when creating  additionaly IDs.
    formatted_id (str): A concatenated string of the code and max_id.
    session (sqlalchemy Session): The SQL Alchemy Session object. Used to search for existing IDs.
  
  """
  # Holds the highest ID for a prov_terr and can generate a new one
  def __init__(self, code:str, session):
    self._code = code
    self._max_id = self.get_highest_id()
    self._formatted_id = self.format_id()
    self.session = session

  @property
  def code(self):
    return self._code
  @code.setter
  def code(self, code):
    self._code = code

  @property
  def max_id(self):
    return self._max_id
  @max_id.setter
  def max_id(self, value:int):
    self._max_id = value

  @property
  def formatted_id(self):
    return self._formatted_id
  @formatted_id.setter
  def formatted_id(self, value):
    self._formatted_id = self.format_id()

  def get_highest_id(self):
    """
    Query the session to get the highest ID for a given prov_terr.

    :return: int
    """
    stmt = select(Mine.cmdb_id).filter(Mine.prov_terr==self.code)
    ids = []
    with self.session.execute(stmt).scalars() as q:
      for r in q:
        id_num = r[2:]
        ids.append(int(id_num))
    return max(ids) if len(ids) > 0 else 0

  def format_id(self):
    """
    Concatenates province/territory code and max_id to create ID string.

    :return: str
    """
    return f"{self.code}{self.max_id:06d}"

  def update_id(self):
    self.max_id += 1
    self.formatted_id = self.format_id()

class ID_Manager:
  """
  A container providing easy access to ProvID objects.
  """

  def __init__(self):
    # Initialize highest ID for each prov_terr
    self.AB = ProvID('AB')
    self.BC = ProvID('BC')
    self.MN = ProvID('MN')
    self.NB = ProvID('NB')
    self.NL = ProvID('NL')
    self.NS = ProvID('NS')
    self.ON = ProvID('ON')
    self.PE = ProvID('PE')
    self.QC = ProvID('QC')
    self.SK = ProvID('SK')
    self.NT = ProvID('NT')
    self.NU = ProvID('NU')
    self.YT = ProvID('YT')