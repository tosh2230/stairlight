from stairlight import StairLight

if __name__ == "__main__":
    stair_light = StairLight()
    print("stair_light.all()")
    print(stair_light.all())
    print()
    print('stair_light.up(table_name="PROJECT_D.DATASET_E.TABLE_F")')
    print(stair_light.up(table_name="PROJECT_D.DATASET_E.TABLE_F"))
    print()
    print('stair_light.down(table_name="PROJECT_C.DATASET_C.TABLE_C")')
    print(stair_light.down(table_name="PROJECT_C.DATASET_C.TABLE_C"))
    print("Undefined files are detected!: " + str(stair_light.undefined_files))

    stair_light.make_config()
