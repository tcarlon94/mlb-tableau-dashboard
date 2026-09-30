import pandas as pd

from src.export_public_sheet import build_export_views, open_workbook, write_sheet


def test_build_export_views_returns_expected_tabs():
    expected = {
        "games": "PUBLIC_EXPORT.GAMES",
        "team_game": "PUBLIC_EXPORT.TEAM_GAME",
        "batting": "PUBLIC_EXPORT.BATTING",
        "pitching": "PUBLIC_EXPORT.PITCHING",
    }

    assert build_export_views() == expected


def test_open_workbook_uses_sheet_id_when_configured(monkeypatch):
    class FakeClient:
        def open_by_key(self, sheet_id):
            return ("id", sheet_id)

        def open(self, workbook_name):
            raise AssertionError("Title lookup should not be used when ID is set")

    monkeypatch.setenv("GOOGLE_SHEET_ID", "sheet-id")

    assert open_workbook(FakeClient()) == ("id", "sheet-id")


def test_write_sheet_handles_empty_dataframe():
    class FakeWorksheet:
        def __init__(self):
            self.cleared = False
            self.values = None

        def clear(self):
            self.cleared = True

        def update(self, values):
            self.values = values

    class FakeWorkbook:
        def __init__(self):
            self.worksheets_called = False
            self.worksheet_obj = FakeWorksheet()

        def worksheets(self):
            self.worksheets_called = True
            return [type("Ws", (), {"title": "games"})()]

        def worksheet(self, sheet_name):
            assert sheet_name == "games"
            return self.worksheet_obj

    workbook = FakeWorkbook()
    write_sheet(workbook, "games", pd.DataFrame())

    assert workbook.worksheet_obj.cleared is True
    assert workbook.worksheet_obj.values == [[]]