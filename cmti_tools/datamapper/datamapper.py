import pandas as pd

# The Mapper and MapperManager store information about the data types and default values for each column in the input table.
class Mapper:
  def __init__(self, destination_column_name, source_column_name, dtype=None, default=None):
    self.destination_column_name = destination_column_name
    self.source_column_name = source_column_name
    self.dtype = dtype
    self.default = default
      
class MapperManager:
  
  def __init__(self):
    self._mappers = {}

  # Allow access to mappers by column name using the dot operator
  def __getattr__(self, destination_column_name):
    if destination_column_name in self._mappers.keys():
      return self._mappers[destination_column_name]
    else:
      raise AttributeError(f"{self.__class__.__name__} has no attribute {destination_column_name}")

  def create_mapper(self, destination_column_name, source_column_name, dtype, default=None):
    self._mappers[destination_column_name] = Mapper(destination_column_name, source_column_name,  dtype, default)

  def __repr__(self):
    return f"MapperManager with {len(self._mappers)} mappers."
  
  def get_mapper_df(self):
    """
    Returns a DataFrame with the column name, dtype, and default value for each mapper.
    
    :return: pd.DataFrame
    """
    
    df = pd.DataFrame(columns=['DB_Column', 'Input_Column', 'Type', 'Default'])
    for mapper in self._mappers.values():
      data = pd.DataFrame(data={'DB_Column': mapper.destination_column_name, 'Input_Column': mapper.source_column_name, 'Type': mapper.dtype, 'Default': mapper.default}, index=[0])
      df = pd.concat([df, data], ignore_index=True)
    return df