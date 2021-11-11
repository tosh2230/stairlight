from src.stairlight import StairLight

if __name__ == "__main__":
    stair_light = StairLight()
    stair_light.set()
    if not stair_light.maps:
        exit()

    result = stair_light.up(
        table_name="PROJECT_D.DATASET_E.TABLE_F", recursive=True, verbose=False
    )
    print(result)

    result = stair_light.up(
        table_name="PROJECT_D.DATASET_E.TABLE_F",
        recursive=True,
        verbose=False,
        response_type="file",
    )
    print(result)

    result = stair_light.up(
        table_name="PROJECT_D.DATASET_E.TABLE_F", recursive=True, verbose=True
    )
    print(result)

    result = stair_light.down(
        table_name="PROJECT_C.DATASET_C.TABLE_C", recursive=True, verbose=False
    )
    print(result)

    result = stair_light.down(
        table_name="PROJECT_C.DATASET_C.TABLE_C",
        recursive=True,
        verbose=False,
        response_type="file",
    )
    print(result)

    result = stair_light.down(
        table_name="PROJECT_C.DATASET_C.TABLE_C", recursive=True, verbose=True
    )
    print(result)
