from src.stairlight.source.s3.map import get_s3_object


def test_get_s3_object(mocker):
    mocker.patch("src.stairlight.source.s3.map.boto3.resource")
    assert get_s3_object(uri="gs://stairlight/results/test.json")
