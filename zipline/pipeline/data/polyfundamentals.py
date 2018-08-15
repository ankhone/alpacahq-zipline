from zipline.utils.numpy_utils import object_dtype
from .dataset import Column, DataSet

class PolygonCompany(DataSet):

    symbol = Column(object_dtype)