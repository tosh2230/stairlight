# from pytest import monkeypatch

import main as cli_main


class TestSuccess:
    parser = cli_main.create_parser()

    def test_command_init(self, stairlight_init):
        message = cli_main.command_init(stairlight=stairlight_init, args=None)
        assert len(message) > 0

    def test_command_check(self, stairlight):
        message = cli_main.command_check(stairlight=stairlight, args=None)
        assert len(message) > 0

    def test_command_up(self, stairlight):
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
        results = cli_main.command_up(stairlight=stairlight, args=args)
        assert len(results) > 0

    def test_command_down(self, stairlight):
        args = self.parser.parse_args(
            [
                "down",
                "--table",
                "PROJECT_C.DATASET_C.TABLE_C",
            ]
        )
        results = cli_main.command_down(stairlight=stairlight, args=args)
        assert len(results) > 0

    def test_main(self, monkeypatch, capfd):
        monkeypatch.setattr("sys.argv", ["", "-c", "config"])
        cli_main.main()
        out, _ = capfd.readouterr()
        assert len(out) > 0
