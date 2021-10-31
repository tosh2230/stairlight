import os
from pprint import pprint

from stairlight import StairLight

if __name__ == "__main__":
    config_file = "config/lddr.yaml"
    home_dir = str(os.path.dirname(os.path.abspath(__file__)))
    template_dir = f"{home_dir}/tests/sql"
    condition = "**/*.sql"

    stair_light = StairLight(template_dir=template_dir, condition=condition)
    pprint(stair_light.maps)
    print("Undefined files are detected!: " + str(stair_light.undefined_files))
