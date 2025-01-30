from sqlalchemy import select
from cmti_tools.tables import Mine

class ProvID:
  """
  An object containing a string representing a provincial ID.

  Attributes:
    code (str): A two-letter code denoting the province or territory.
    max_id (int): The largest existing ID per that code. i.e., the integer to start with when creating  additionaly IDs.
    formatted_id (str): A concatenated string of the code and max_id.
  """
  # Holds the highest ID for a prov_terr and can generate a new one
  def __init__(self, code:str):
    self._code = code
    self._max_id = 0
    self._formatted_id = self.format_id(self.max_id)

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
  def formatted_id(self, val:str):
    try:
      self._formatted_id = val
    except ValueError:
      raise ValueError("formatted_id must be a string. Please use the format_id method to generate a new formatted_id.")

  def query_session_id(self, session):
    """
    Query the session to get the highest ID for a given provID.

    :return: int
    """
    ids = []
    for mine in session.query(Mine).filter(Mine.prov_terr == self.code):
      mine_id = mine.cmdb_id
      id_num = int(mine_id[2:]) # The integer portion of the ID
      ids.append(id_num)
    return max(ids) if len(ids) > 0 else 0

  def format_id(self, id_val:int) -> str:
    """
    Concatenates province/territory code and max_id to create ID string.

    :return: str
    """
    return f"{self.code}{id_val:06d}"

  def update_id(self, id_val:int):
    """
    Updates the max_id and formatted_id attributes.
    """
    try:        
      if id_val > self.max_id:
        self.max_id = id_val
        self.formatted_id = self.format_id(id_val)
      else:
        print(f"New ID must be greater than the current max_id ({self.max_id}).")
    except ValueError as e:
      raise(f"{e}. Please provide an integer for id_val.")
  
  def generate_id(self):
    """
    Generates a new ID by incrementing the max_id by 1.
    """
    self.update_id(self.max_id + 1)

class ID_Manager:
  """
  A container providing easy access to a ProvID object for all Canadian jurisdictions.
  """

  def __init__(self):
    # Create a list to hold all ProvID objects
    self.all = []

    # Initialize highest ID for each prov_terr
    self.AB = ProvID('AB')
    self.all.append(self.AB)
    self.BC = ProvID('BC')
    self.all.append(self.BC)
    self.MB = ProvID('MB')
    self.all.append(self.MB)
    self.NB = ProvID('NB')
    self.all.append(self.NB)
    self.NL = ProvID('NL')
    self.all.append(self.NL)
    self.NS = ProvID('NS')
    self.all.append(self.NS)
    self.ON = ProvID('ON')
    self.all.append(self.ON)
    self.PE = ProvID('PE')
    self.all.append(self.PE)
    self.QC = ProvID('QC')
    self.all.append(self.QC)
    self.SK = ProvID('SK')
    self.all.append(self.SK)
    self.NT = ProvID('NT')
    self.all.append(self.NT)
    self.NU = ProvID('NU')
    self.all.append(self.NU)
    self.YT = ProvID('YT')
    self.all.append(self.YT)

  def update_all(self, session):
    for prov in self.all:
      session_max = prov.query_session_id(session)
      if session_max > prov.max_id:
        prov.update_id(session_max)