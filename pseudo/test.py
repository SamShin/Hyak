from splink.datasets import splink_datasets
from splink.duckdb.linker import DuckDBLinker
import altair as alt

import pandas as pd
pd.options.display.max_rows = 1000
df = splink_datasets.historical_50k