from __future__ import annotations

from typing import Any, Iterator

import pytest

import src.stairlight.cli as cli_main
from src.stairlight import StairLight
from tests.conftest import teardown_rm_config, teardown_rm_file


@pytest.fixture(scope="session")
def stairlight_init() -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="tests/config/test_init")
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(file="tests/config/test_init/stairlight.yaml")


@pytest.fixture(scope="session")
def stairlight_check() -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="tests/config/test_check")
    stairlight.create_map()
    yield stairlight
    teardown_rm_config(pathname="tests/config/test_check/mapping_*.yaml")


@pytest.fixture(scope="session")
def stairlight_check_no_file_found() -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="tests/config/test_check_no_file_found")
    stairlight.create_map()
    yield stairlight


class TestSuccess:
    parser = cli_main.create_parser()

    def test_command_init(self, stairlight_init: StairLight):
        message = cli_main.command_init(
            stairlight=stairlight_init, args=self.parser.parse_args([])
        )
        assert len(message) > 0 and "already exists" not in message

    @pytest.mark.integration
    def test_command_check(self, stairlight_save: StairLight):
        message = cli_main.command_check(
            stairlight=stairlight_save, args=self.parser.parse_args([])
        )
        assert len(message) > 0

    def test_command_check_at_first(self, stairlight_check: StairLight):
        message = cli_main.command_check(
            stairlight=stairlight_check, args=self.parser.parse_args([])
        )
        assert len(message) > 0

    def test_command_check_no_file_found(
        self, stairlight_check_no_file_found: StairLight
    ):
        message = cli_main.command_check(
            stairlight=stairlight_check_no_file_found, args=self.parser.parse_args([])
        )
        assert not message

    @pytest.mark.integration
    def test_command_up_table(self, stairlight_save: StairLight):
        args = self.parser.parse_args(
            [
                "up",
                "--table",
                "PROJECT_D.DATASET_E.TABLE_F",
                "-t",
                "PROJECT_d.DATASET_d.TABLE_d",
                "-r",
                "-v",
            ]
        )
        results = cli_main.command_up(stairlight=stairlight_save, args=args)
        assert len(results) > 0

    @pytest.mark.integration
    def test_command_up_label(self, stairlight_save: StairLight):
        args = self.parser.parse_args(
            [
                "up",
                "--label",
                "Test:b",
                "-l",
                "Source:gcs",
                "-r",
                "-v",
            ]
        )
        results = cli_main.command_up(stairlight=stairlight_save, args=args)
        assert len(results) > 0

    @pytest.mark.integration
    def test_command_down_table(self, stairlight_save: StairLight):
        args = self.parser.parse_args(
            [
                "down",
                "--table",
                "PROJECT_C.DATASET_C.TABLE_C",
            ]
        )
        results = cli_main.command_down(stairlight=stairlight_save, args=args)
        assert len(results) > 0

    @pytest.mark.integration
    def test_command_down_label(self, stairlight_save: StairLight):
        args = self.parser.parse_args(
            [
                "down",
                "--label",
                "Test:c",
                "-r",
                "-v",
            ]
        )
        results: dict[str, Any] | list[dict[str, Any]] = cli_main.command_down(
            stairlight=stairlight_save, args=args
        )
        actual: dict[str, Any]
        if isinstance(results, dict):
            actual = results
        elif isinstance(results, list):
            actual = results[0]
        assert actual.get("PROJECT_J.DATASET_K.TABLE_L")

    @pytest.mark.integration
    def test_main(self, monkeypatch, capfd):
        monkeypatch.setattr("sys.argv", ["", "-c", "tests/config"])
        cli_main.main()
        out, err = capfd.readouterr()
        assert len(out) > 0 and len(err) == 0

    @pytest.mark.integration
    def test_main_quiet(self, monkeypatch, capfd):
        monkeypatch.setattr("sys.argv", ["", "-c", "tests/config", "-q"])
        cli_main.main()
        out, err = capfd.readouterr()
        assert len(out) == 0 and len(err) == 0

    @pytest.mark.integration
    def test_main_up(self, monkeypatch, capfd):
        monkeypatch.setattr(
            "sys.argv",
            [
                "",
                "up",
                "-c",
                "tests/config",
                "--table",
                "PROJECT_D.DATASET_E.TABLE_F",
                "-t",
                "PROJECT_d.DATASET_d.TABLE_d",
                "-r",
                "-v",
            ],
        )
        cli_main.main()
        out, err = capfd.readouterr()
        assert len(out) > 0 and len(err) == 0

    @pytest.mark.integration
    def test_main_down(self, monkeypatch, capfd):
        monkeypatch.setattr(
            "sys.argv",
            [
                "",
                "down",
                "-c",
                "tests/config",
                "--label",
                "Test:b",
                "-l",
                "Source:gcs",
                "-r",
                "-v",
            ],
        )
        cli_main.main()
        out, err = capfd.readouterr()
        assert len(out) > 0 and len(err) == 0
