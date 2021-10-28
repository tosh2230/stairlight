import os
from pprint import pprint

from msb import Msb

if __name__ == "__main__":
    config_file = 'config/msb.yaml'
    target_dir = str(os.path.dirname(__file__))
    condition = '**/*.sql'

    msb = Msb(
        config_file=config_file,
        target_dir=target_dir,
        condition=condition
    )
    results = msb.search_nodes()
    pprint(results)
