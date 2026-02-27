import datetime
import pathlib
import typing
import warnings

import numpy as np
import pandas as pd
import pydantic
import requests
from rich import print

from nemdata import utils
from nemdata.config import DEFAULT_BASE_DIRECTORY
from nemdata.constants import constants


class VariableFrequency(pydantic.BaseModel):
    frequency_minutes_before: int
    frequency_minutes_after: int
    transition_datetime_interval_end: datetime.datetime


class MMSDMTable(pydantic.BaseModel):
    name: str
    table: str
    directory: str
    datetime_columns: typing.Union[list[str], None] = None
    interval_column: typing.Union[str, None] = None
    frequency: typing.Union[int, VariableFrequency, None] = None
    legacy_table: typing.Union[str, None] = None


mmsdm_tables = [
    MMSDMTable(
        name="dispatch-price",
        table="DISPATCHPRICE",
        directory="DATA",
        datetime_columns=["SETTLEMENTDATE"],
        interval_column="SETTLEMENTDATE",
        frequency=5,
    ),
    MMSDMTable(
        name="predispatch",
        table="PREDISPATCHPRICE#ALL#FILE01",
        directory="PREDISP_ALL_DATA",
        datetime_columns=["LASTCHANGED", "DATETIME"],
        interval_column="DATETIME",
        frequency=30,
        legacy_table="PREDISPATCHPRICE",
    ),
    MMSDMTable(
        name="unit-scada",
        table="DISPATCH_UNIT_SCADA",
        directory="DATA",
        datetime_columns=["LASTCHANGED", "SETTLEMENTDATE"],
        interval_column="SETTLEMENTDATE",
        frequency=5,
    ),
    MMSDMTable(
        name="trading-price",
        table="TRADINGPRICE",
        directory="DATA",
        datetime_columns=["SETTLEMENTDATE"],
        interval_column="SETTLEMENTDATE",
        frequency=VariableFrequency(
            frequency_minutes_before=30,
            transition_datetime_interval_end=constants.transition_datetime_interval_end,
            frequency_minutes_after=5,
        ),
    ),
    MMSDMTable(
        name="demand",
        table="DISPATCHREGIONSUM",
        directory="DATA",
        datetime_columns=["LASTCHANGED", "SETTLEMENTDATE"],
        interval_column="SETTLEMENTDATE",
        frequency=5,
    ),
    MMSDMTable(
        name="interconnectors",
        table="DISPATCHINTERCONNECTORRES",
        directory="DATA",
        datetime_columns=["LASTCHANGED", "SETTLEMENTDATE"],
        interval_column="SETTLEMENTDATE",
        frequency=5,
    ),
    MMSDMTable(
        name="p5min",
        table="P5MIN_REGIONSOLUTION_ALL#ALL#FILE01",
        directory="P5MIN_ALL_DATA",
        datetime_columns=["RUN_DATETIME", "INTERVAL_DATETIME", "LASTCHANGED"],
        interval_column="INTERVAL_DATETIME",
        frequency=5,
        legacy_table="P5MIN_REGIONSOLUTION_ALL",
    ),
    MMSDMTable(
        name="predispatch-sensitivities",
        table="PREDISPATCHPRICESENSITIVITIE_D",
        directory="DATA",
        datetime_columns=["LASTCHANGED", "DATETIME"],
        interval_column="DATETIME",
        frequency=30,
    ),
    MMSDMTable(
        name="predispatch-demand",
        table="PREDISPATCHREGIONSUM#ALL#FILE01",
        directory="PREDISP_ALL_DATA",
        datetime_columns=["LASTCHANGED", "DATETIME"],
        interval_column="DATETIME",
        frequency=30,
        legacy_table="PREDISPATCHREGIONSUM",
    ),
]


class MMSDMFile(pydantic.BaseModel):
    year: int
    month: int
    table: MMSDMTable
    url: str
    csv_name: str
    data_directory: pathlib.Path
    zipfile_path: pathlib.Path


def find_mmsdm_table(name: str) -> MMSDMTable:
    """finds a MMSDMTable by it's `name`"""
    for table in mmsdm_tables:
        if table.name == name:
            return table
    raise ValueError(
        f"MMSDMTable {name} not found - tables available are {[table.name for table in mmsdm_tables]}"
    )


def make_many_mmsdm_files(
    start: str, end: str, table: MMSDMTable, base_directory: pathlib.Path
) -> list[MMSDMFile]:
    """creates many MMSDMFiles - one for each month"""
    table = find_mmsdm_table(table.name)
    months = pd.date_range(start=start, end=end, freq="MS")

    files = []
    for year, month in zip(months.year, months.month):
        files.append(
            make_one_mmsdm_file(
                year=year, month=month, table=table, base_directory=base_directory
            )
        )
    return files


def make_one_mmsdm_file(
    year: int, month: int, table: MMSDMTable, base_directory: pathlib.Path
) -> MMSDMFile:
    """creates a single MMSDMFile object that represents one file on the AEMO MMSDM website"""
    padded_month = str(month).zfill(2)

    url_prefix = f"https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{padded_month}/MMSDM_Historical_Data_SQLLoader"

    legacy_table_name = table.legacy_table if table.legacy_table else table.table

    if (year, month) >= (2024, 8):
        url = f"{url_prefix}/{table.directory}/PUBLIC_ARCHIVE#{table.table}#{year}{padded_month}010000.zip"
        url = url.replace("#", "%23")
        csv_name = f"PUBLIC_ARCHIVE#{table.table}#{year}{padded_month}010000.CSV"
    else:
        url = f"{url_prefix}/{table.directory}/PUBLIC_DVD_{legacy_table_name}_{year}{padded_month}010000.zip"
        csv_name = f"PUBLIC_DVD_{legacy_table_name}_{year}{padded_month}010000.CSV"

    data_directory = base_directory / table.name / f"{year}-{padded_month}"
    data_directory.mkdir(exist_ok=True, parents=True)

    return MMSDMFile(
        year=year,
        month=month,
        table=table,
        url=url,
        csv_name=csv_name,
        data_directory=data_directory,
        zipfile_path=data_directory / "raw.zip",
    )


def load_unzipped_mmsdm_file(
    mmsdm_file: MMSDMFile, skiprows: int = 1, tail: int = -1
) -> pd.DataFrame:
    """read the CSV from an unzipped MMSDMFile"""
    path = mmsdm_file.data_directory / mmsdm_file.csv_name
    #  remove first row via skiprows
    data = pd.read_csv(path, skiprows=skiprows)
    #  remove last row via iloc
    return data.iloc[:tail, :]


def make_datetime_columns(data: pd.DataFrame, table: MMSDMTable) -> pd.DataFrame:
    """cast the `mmsdm_table.datetime_columns` to datetime objects"""
    datetime_columns = table.datetime_columns
    assert datetime_columns
    datetime_columns += ["interval-start", "interval-end"]

    for col in datetime_columns:
        try:
            data[col] = pd.to_datetime(data[col])
            data[col] = data[col].dt.tz_localize(constants.nem_tz)
        except KeyError:
            pass
    return data


def download_mmsdm(
    start: str,
    end: str,
    table_name: str,
    base_directory: pathlib.Path = DEFAULT_BASE_DIRECTORY,
    dry_run: bool = False,
) -> pd.DataFrame:
    """main for downloading MMSDMFiles"""
    table = find_mmsdm_table(table_name)
    files = make_many_mmsdm_files(start, end, table, base_directory)

    dataset = []
    for mmsdm_file in files:
        data = download_one_mmsdm(table, mmsdm_file, dry_run)
        if data is not None:
            dataset.append(data)
    try:
        return pd.concat(dataset, axis=0)
    except ValueError:
        return pd.DataFrame()


def check_for_additional_files(mmsdm_file: MMSDMFile) -> None:
    """Warn if there are additional files (FILE02, etc.) when FILE01 is in the URL."""
    if "FILE01" not in mmsdm_file.url:
        return

    file02_url = mmsdm_file.url.replace("FILE01", "FILE02")
    try:
        response = requests.head(file02_url, timeout=10)
        if response.status_code == 200:
            warnings.warn(
                f"Multiple files exist for {mmsdm_file.table.name} ({mmsdm_file.year}-{mmsdm_file.month:02d}). "
                f"Only FILE01 is being downloaded. Additional files (FILE02, etc.) are not supported.",
                UserWarning,
            )
    except requests.RequestException:
        pass


def download_one_mmsdm(
    table: MMSDMTable, mmsdm_file: MMSDMFile, dry_run: bool
) -> typing.Union[pd.DataFrame, None]:
    clean_fi = mmsdm_file.data_directory / "clean.parquet"
    if clean_fi.exists():
        print(f" [blue]CACHED[/] {' '.join(clean_fi.parts[-5:])}")
        return pd.read_parquet(clean_fi)
    else:
        print(f" [blue]NOT CACHED[/] {' '.join(clean_fi.parts[-5:])}")

    check_for_additional_files(mmsdm_file)

    data_available = utils.download_zipfile(mmsdm_file)

    if not data_available:
        print(f" [red]NOT AVAILABLE[/] {' '.join(mmsdm_file.zipfile_path.parts[-5:])}")
        return None

    else:
        print(f" [green]DOWNLOADING[/] {' '.join(mmsdm_file.zipfile_path.parts[-5:])}")
        utils.unzip(mmsdm_file.zipfile_path)
        data = load_unzipped_mmsdm_file(mmsdm_file)
        assert table.datetime_columns
        data = make_datetime_columns(data, table)
        data = utils.add_interval_column(data, table)

        if not dry_run:
            print(f" [green]SAVING [/] {clean_fi}")
            data.to_csv(clean_fi.with_suffix(".csv"))
            data.to_parquet(clean_fi.with_suffix(".parquet"))
        return data
