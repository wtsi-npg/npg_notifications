#
# Copyright © 2024, 2025 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib import parse

import pytest
import requests
from npg.conf import IniData
from requests import HTTPError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database

from npg_notify.db import mlwh
from npg_notify.db.mlwh import OseqFlowcell, Sample, StockResource, Study, StudyUser
from npg_notify.ont.event import (
    MYSQL_MLWH_CONFIG_FILE_SECTION,
    PORCH_CONFIG_FILE_SECTION,
)
from npg_notify.ont.porch import Pipeline

ont_test_config = Path("./tests/data/ont_event_app_config.ini")


@dataclass
class TestDBConfig:
    dbuser: str
    dbpassword: str
    dbhost: str
    dbport: str
    dbschema: str

    @property
    def url(self):
        return (
            f"mysql+pymysql://{self.dbuser}:{parse.quote_plus(self.dbpassword)}"
            f"@{self.dbhost}:{self.dbport}/{self.dbschema}?charset=utf8mb4"
        )


def porch_server_available() -> bool:
    config = IniData(Pipeline.ServerConfig).from_file(
        ont_test_config, PORCH_CONFIG_FILE_SECTION
    )
    try:
        response = requests.request("GET", config.url, timeout=5)
        return response.status_code == 200
    except (requests.ConnectionError, HTTPError):
        return False
    except Exception:
        raise


@pytest.fixture(scope="session")
def porch_server_config() -> Pipeline.ServerConfig:
    return IniData(Pipeline.ServerConfig).from_file(
        ont_test_config, PORCH_CONFIG_FILE_SECTION
    )


def ont_tag_identifier(tag_index: int) -> str:
    """Return an ONT tag identifier in tag set EXP-NBD104, given a tag index."""
    return f"NB{tag_index:02d}"


def make_id_source(start: int = 0) -> callable:
    """Return a deterministic generator for unique ID to use in test fixtures."""

    def fn():
        n = start
        while True:
            n += 1
            yield n

    gen = fn()

    return lambda: next(gen)


CREATED = datetime(year=2019, month=12, day=30, hour=0, minute=0, second=0)
UPDATED = datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0)


def initialize_mlwh_ont(session: Session):
    """Populate the ML warehouse database with synthetic ONT-related records.

    Three studies, X, Y and Z. Samples from studies X and Y, but none from Z,
    are sequenced.

    A plate with 6 samples, 3 from one study X and 3 from Y. Two of the samples,
    1 from each study are sequenced as simple (non-multiplexed) runs. The other 4
    samples are not sequenced. Another plate has 3 samples from study Z, none of
    which are sequenced.

    Six plates of 6 samples each, all the samples in 3 of the plates being from the
    same study X and the other 3 plates from Y.

    The first plate of each study is sequenced as a multiplexed run of 12 samples.
    The second plate of each study is sequenced twice as multiplexed runs of 12 samples.
    The third plate of each study is not sequenced.

    """
    instrument_name = "instrument_01"
    default_timestamps = {
        "created": CREATED,
        "last_updated": UPDATED,
        "recorded_at": UPDATED,
    }

    ont_lims = "ONT_LIMS"
    tracking_lims = "OTHER_LIMS"
    ids = make_id_source()

    study_2 = Study(id_lims=tracking_lims, id_study_lims="2", name="Study two name")
    study_3 = Study(id_lims=tracking_lims, id_study_lims="3", name="Study three name")
    study_4 = Study(id_lims=tracking_lims, id_study_lims="4", name="Study four name")
    study_5 = Study(id_lims=tracking_lims, id_study_lims="5", name="Study five name")
    session.add_all([study_2, study_3, study_4, study_5])
    session.commit()

    session.add_all(
        [
            StudyUser(
                email="user1@sanger.ac.uk",
                id_study_tmp=study_2.id_study_tmp,
                role="owner",
            ),
            StudyUser(email="user1@sanger.ac.uk", id_study_tmp=study_4.id_study_tmp),
            StudyUser(
                email="user2@sanger.ac.uk",
                id_study_tmp=study_2.id_study_tmp,
                role="manager",
            ),
            StudyUser(
                email="user3@sanger.ac.uk",
                id_study_tmp=study_2.id_study_tmp,
                role="follower",
            ),
            StudyUser(
                email="user3@sanger.ac.uk",
                id_study_tmp=study_2.id_study_tmp,
                role="owner",
            ),
            StudyUser(
                email="owner@sanger.ac.uk",
                id_study_tmp=study_2.id_study_tmp,
                role="owner",
            ),
            StudyUser(id_study_tmp=study_2.id_study_tmp),
            StudyUser(
                email="loader@sanger.ac.uk",
                id_study_tmp=study_3.id_study_tmp,
                role="loader",
            ),
            StudyUser(id_study_tmp=study_4.id_study_tmp, role="owner"),
        ]
    )

    # Three plates of samples for simple (non-multiplexed) runs.
    splates = defaultdict(list[StockResource])  # Plate number -> wells in the plate
    for plate_num, study in enumerate([study_2, study_3, study_4]):
        well_address = "A1"
        smid = ids()
        sm = Sample(
            id_lims=ont_lims,
            id_sample_lims=f"id_{smid}",
            name=f"sample{smid}",
            **default_timestamps,
        )
        session.add(sm)

        srid = ids()
        well = StockResource(
            id_lims=ont_lims,
            id_stock_resource_lims=f"plate{plate_num}_{srid}",
            stock_resource_uuid=f"stock_resource_uuid{srid:0>3}",
            labware_type="well",
            labware_machine_barcode=f"plate{plate_num}_barcode",
            labware_human_barcode=f"plate{plate_num}_barcode",
            labware_coordinate=well_address,
            sample=sm,
            study=study,
        )
        session.add(well)
        splates[plate_num].append(well)

    # Two simple (non-multiplexed) experiment runs, using the first sample from each
    # of the first two plates respectively. No samples from plate the third are
    # sequenced.
    for expt in [0, 1]:
        plate_num = expt
        flowcell_id = f"flowcell_s{expt:0>3}"
        first_well, *other_wells = splates[plate_num]

        fc = OseqFlowcell(
            sample=first_well.sample,
            study=first_well.study,
            instrument_name=instrument_name,
            instrument_slot=1,
            experiment_name=f"simple_experiment_{expt:0>3}",
            flowcell_id=flowcell_id,
            id_lims=ont_lims,
            id_flowcell_lims=flowcell_id,
            last_updated=UPDATED,
            recorded_at=UPDATED,
        )
        session.add(fc)

    # Six plates of samples for multiplexed runs.
    mplates = defaultdict(list[StockResource])  # Plate number -> wells in the plate
    for plate_num, study in enumerate(
        [study_2, study_2, study_3, study_3, study_4, study_4]
    ):
        for well_address in ["A1", "A2", "A3", "A4", "A5", "A6"]:
            smid = ids()
            sm = Sample(
                id_lims=ont_lims,
                id_sample_lims=f"id_{smid}",
                name=f"sample{smid}",
                **default_timestamps,
            )
            session.add(sm)

            srid = ids()
            well = StockResource(
                id_lims=ont_lims,
                id_stock_resource_lims=f"plate{plate_num}_{srid}",
                stock_resource_uuid=f"stock_resource_uuid{srid:0>3}",
                labware_type="well",
                labware_machine_barcode=f"plate{plate_num}_barcode",
                labware_human_barcode=f"plate{plate_num}_barcode",
                labware_coordinate=well_address,
                sample=sm,
                study=study,
            )
            session.add(well)
            mplates[plate_num].append(well)

    barcodes = [
        "CACAAAGACACCGACAACTTTCTT",
        "ACAGACGACTACAAACGGAATCGA",
        "CCTGGTAACTGGGACACAAGACTC",
        "TAGGGAAACACGATAGAATCCGAA",
        "AAGGTTACACAAACCCTGGACAAG",
        "GACTACTTTCTGCCTTTGCGAGAA",
        "AAGGATTCATTCCCACGGTAACAC",
        "ACGTAACTTGGTTTGTTCCCTGAA",
        "AACCAAGACTCGCTGTGCCTAGTT",
        "GAGAGGACAAAGGTTTCAACGCTT",
        "TCCATTCCCTCCGATAGATGAAAC",
        "TCCGATTCTGCTTCTTTCTACCTG",
    ]

    # Two multiplexed experiment runs of 12 samples, each using the 6 samples from 2
    # different plates of the same study
    for expt, plate_num in enumerate([(0, 1), (2, 3)]):
        plate_num_a, plate_num_b = plate_num
        flowcell_id = f"flowcell_m{expt:0>3}"
        expt_name = f"multiplexed_experiment_{expt:0>3}"

        # Six samples from plate_a, 6 from plate_b
        wells = [*mplates[plate_num_a], *mplates[plate_num_b]]

        for i, (well, barcode) in enumerate(zip(wells, barcodes, strict=True)):
            fc = OseqFlowcell(
                sample=well.sample,
                study=well.study,
                instrument_name=instrument_name,
                instrument_slot=1,
                experiment_name=expt_name,
                flowcell_id=flowcell_id,
                id_lims=ont_lims,
                id_flowcell_lims=flowcell_id,
                tag_set_id_lims="tag_set_id_lims",
                tag_set_name="EXP-NBD104",
                tag_sequence=barcode,
                tag_identifier=ont_tag_identifier(i),
                last_updated=UPDATED,
                recorded_at=UPDATED,
            )
            session.add(fc)

    session.commit()


@pytest.fixture(scope="function")
def mlwh_session():
    """Create an empty ML warehouse database fixture."""
    dbconfig = IniData(TestDBConfig).from_file(
        ont_test_config, MYSQL_MLWH_CONFIG_FILE_SECTION
    )
    engine = create_engine(dbconfig.url, echo=False)

    if database_exists(engine.url):
        drop_database(engine.url)

    create_database(engine.url)

    with engine.connect() as conn:
        # Workaround for invalid default values for dates.
        conn.execute(text("SET sql_mode = '';"))
        conn.commit()

    mlwh.Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine)
    sess: Session() = session_maker()

    try:
        yield sess
    finally:
        sess.close()

    # This is for the benefit of MySQL where we have a schema reused for
    # a number of tests. Without using sqlalchemy-utils, one would call:
    #
    #   for t in reversed(meta.sorted_tables):
    #       t.drop(engine)
    #
    # Dropping the database for SQLite deletes the SQLite file.
    drop_database(engine.url)


@pytest.fixture(scope="function")
def ont_synthetic_mlwh(mlwh_session):
    """An ML warehouse database fixture populated with ONT-related records."""
    initialize_mlwh_ont(mlwh_session)
    yield mlwh_session
