from stairlight.template import Template


class TestSuccess:
    template = Template()

    def test_search_fs(self):
        source = {
            "type": "local",
            "path": "./tests/sql",
            "pattern": "*.sql",
        }
        result = []
        for file in self.template.search_fs(source=source):
            result.append(file)
        assert result == [
            "tests/sql/test_b.sql",
            "tests/sql/test_a.sql",
        ]

    def test_is_excluded(self):
        assert self.template.is_excluded("tests/sql/c/test_exclude.sql")
