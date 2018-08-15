from zipline.assets.assets_alpaca import AssetFinderAlpaca
import numpy as np

from .base import PipelineLoader


class PolyCompanyLoader(PipelineLoader):

    def __init__(self, asset_finder):
        self._asset_finder = asset_finder

    def load_adjusted_array(self, columns, dates, assets, mask):

        asset_finder = self._asset_finder
        asset_symbols = [
            a.symbol for a in asset_finder.retrieve_all(assets)
        ]

        out = {}
        for c in columns:
            out[c] = np.array(asset_symbols * len(dates)).reshape(len(dates), -1)
        return out