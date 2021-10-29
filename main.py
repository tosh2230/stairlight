import os
from pprint import pprint

from lddr import Ladder

if __name__ == "__main__":
    config_file = 'config/lddr.yaml'
    target_dir = str(os.path.dirname(__file__))
    condition = '**/*.sql'

    ladder = Ladder(
        config_file=config_file,
        target_dir=target_dir,
        condition=condition
    )
    maps = ladder.maps
    pprint(maps)
