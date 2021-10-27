import os
from pprint import pprint

from msb.entrypoint import Msb

if __name__ == "__main__":
    target_dir = str(os.path.dirname(__file__))
    condition = '**/*.sql'

    msb = Msb(target_dir=target_dir, condition=condition)
    results = msb.search_nodes()
    pprint(results)
