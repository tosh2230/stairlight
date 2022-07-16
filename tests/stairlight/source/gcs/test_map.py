from src.stairlight.source.gcs.map import get_gcs_blob


def test_get_gcs_blob(mocker):
    mocker.patch("src.stairlight.source.gcs.map.Client.get_bucket")
    assert get_gcs_blob(uri="gs://stairlight/results/test.json")
