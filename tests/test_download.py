import pathlib

from _pytest.capture import CaptureFixture

from nemdata.downloader import download


def test_system_mmsdm(base_dir: pathlib.Path, capsys: CaptureFixture) -> None:
    tables = [
        {"table": "trading-price", "months": ["2020-01", "2020-02", "2025-01"], "repeat": "2025-01"},
        {"table": "predispatch", "months": ["2024-01", "2025-01"], "repeat": "2025-01"},
    ]
    for config in tables:
        table = config["table"]
        for month in config["months"]:
            assert not (base_dir / table / month / "clean.parquet").exists()
            download(start=month, end=month, table=table, base_directory=base_dir)
            assert (base_dir / table / month / "clean.parquet").exists()

        capsys.readouterr()
        repeat_month = config["repeat"]
        download(start=repeat_month, end=repeat_month, table=table, base_directory=base_dir)
        assert (base_dir / table / repeat_month / "clean.parquet").exists()
        captured = capsys.readouterr()
        assert "CACHED" in captured.out
        assert not "NOT CACHED" in captured.out


def test_system_nemde(base_dir: pathlib.Path, capsys: CaptureFixture) -> None:
    days = ["2020-01-01", "2020-01-02"]
    for day in days:
        assert not (base_dir / "nemde" / day / "clean.parquet").exists()
        download(start=day, end=day, table="nemde", base_directory=base_dir)
        assert (base_dir / "nemde" / day / "clean.parquet").exists()

    capsys.readouterr()
    repeat_days = ["2020-01-02"]
    for day in repeat_days:
        download(start=day, end=day, table="nemde", base_directory=base_dir)
        assert (base_dir / "nemde" / day / "clean.parquet").exists()
        captured = capsys.readouterr()
        assert "CACHED" in captured.out
        assert not "NOT CACHED" in captured.out
