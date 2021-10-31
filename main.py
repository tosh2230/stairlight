from pprint import pprint

from stairlight import StairLight

if __name__ == "__main__":
    stair_light = StairLight()
    pprint(stair_light.maps)
    print("Undefined files are detected!: " + str(stair_light.undefined_files))
