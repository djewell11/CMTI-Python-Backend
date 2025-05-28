from cmti_tools.tables import Mine
from pandas import Series # For type hinting

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

  def get_max_id(self, mine_ids: list | Series) -> int:
    """
    Query the list of IDs to get the highest ID for a given provID.

    :param mine_ids: list or Series of mine IDs. Entries must be in the format of 'XX000001' where XX is the province/territory code.
    :type mine_ids: list | Pandas.Series

    :return: int
    """
    if isinstance(mine_ids, Series):
      mine_ids = mine_ids.tolist()
    
    ids = []
    
    for mine_id in mine_ids:
      if mine_id.startswith(self.code):
        id_num = int(mine_id[2:])
        ids.append(id_num)
    return max(ids) if len(ids) > 0 else 0
  
  def query_session_id(self, session):
    # Probably deperacted now, leaving it to not break anything
    """
    Query the session to get the highest ID for a given provID.

    :return: int
    """
    ids = []
    for mine in session.query(Mine).filter(Mine.prov_terr == self.code):
      mine_id = mine.cmti_id
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
    self.prov_ids = {}
    for code in ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'ON', 'PE', 'QC', 'SK', 'NT', 'NU', 'YT']:
      self._add_code(code)

  def __getattr__(self, code):
    # Allows dot access to prov_ids via code (e.g., id_manager.ON)
    if code in self.prov_ids:
      return self.prov_ids[code]
    else:
      raise AttributeError(f"'{self.__class__.__name__}' object has no ProvID '{code}'")

  def _add_code(self, code:str):
    """
    Adds a ProvID via code to the ID_Manager.

    :param code: str
    """
    if code not in self.prov_ids:
      prov_id = ProvID(code)
      self.prov_ids[code] = prov_id
    else:
      raise ValueError(f"Code '{code}' already exists in ID_Manager.")
    
  def update_prov_ids(self, mine_ids: list | Series):
    """
    Updates the max_id for each provID object based on the provided list of mine IDs.

    :param mine_ids: list or Series of mine IDs. Entries must be in the format of 'XX000001' where XX is the province/territory code.
    :type mine_ids: list | Pandas.Series

    :param mine_ids: list or Series of mine IDs
    """
    for prov in self.prov_ids.values():
      max_id = prov.get_max_id(mine_ids)
      if max_id > prov.max_id:
        prov.update_id(max_id)

  def update_all_session(self, session):
    # Probably deprecated now, leaving it to not break anything
    for prov in self.all:
      session_max = prov.query_session_id(session)
      if session_max > prov.max_id:
        prov.update_id(session_max)