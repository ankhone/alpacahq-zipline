import numpy as np

from zipline.pipeline.data import polygon
from zipline.assets.assets_alpaca import AssetFinderAlpaca

from .base import PipelineLoader


class PolygonCompanyLoader(PipelineLoader):

    def __init__(self, asset_finder):
        self._asset_finder = asset_finder

    def load_adjusted_array(self, columns, dates, assets, mask):

        asset_finder = self._asset_finder
        asset_symbols = [
            a.symbol for a in asset_finder.retrieve_all(assets)
        ]

        companies = polygon.companies()
        out = {}
        for c in columns:
            data = [
                companies.get(a.symbol, {}).get(c.name, c.missing_value)
                for a in asset_finder.retrieve_all(assets)
            ]
            out[c] = np.tile(np.array(data, dtype=c.dtype), (len(dates), 1))
        return out
