import os
from pprint import pprint

from lddr import Ladder

if __name__ == "__main__":
    config_file = "config/lddr.yaml"
    home_dir = str(os.path.dirname(os.path.abspath(__file__)))
    template_dir = f"{home_dir}/tests/sql"
    condition = "**/*.sql"

    ladder = Ladder(
        config_file=config_file, template_dir=template_dir, condition=condition
    )
    pprint(ladder.maps)
    print("Undefined files are detected!: " + str(ladder.undefined_files))
