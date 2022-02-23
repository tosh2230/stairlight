# from pytest import monkeypatch

import src.stairlight.cli as cli_main
from stairlight import StairLight


class TestSuccess:
    parser = cli_main.create_parser()

    def test_command_init(self, stairlight_init: StairLight):
        message = cli_main.command_init(stairlight=stairlight_init, args=None)
        assert len(message) > 0

    def test_command_check(self, stairlight_save: StairLight):
        message = cli_main.command_check(stairlight=stairlight_save, args=None)
        assert len(message) > 0

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
        results = cli_main.command_down(stairlight=stairlight_save, args=args)
        assert results.get("PROJECT_J.DATASET_K.TABLE_L") != {}

    def test_main(self, monkeypatch, capfd):
        monkeypatch.setattr("sys.argv", ["", "-c", "tests/config"])
        cli_main.main()
        out, _ = capfd.readouterr()
        assert len(out) > 0
