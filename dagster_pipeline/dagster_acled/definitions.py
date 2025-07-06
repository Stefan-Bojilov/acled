from dagster import Definitions, load_assets_from_modules

from dagster_acled.assets import acled

all_assets = load_assets_from_modules([acled])

defs = Definitions(
    assets=all_assets
)
