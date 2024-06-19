import pytest
import os

from npg_notify.db.utils import (
    create_schema,
    get_connection,
    batch_load_from_yaml,
)
from npg_notify.db.mlwh import Base

TEST_CONFIG_FILE= "qc_state_app_config.ini"
test_config = os.path.join(os.path.dirname(__file__), "data", TEST_CONFIG_FILE)


@pytest.fixture(scope="module", name="mlwh_test_session")
def get_test_db_session():
    """
    Establishes a connection to the database, creates a schema, loads
    data and returns a new database session.
    """
    fixtures_dir = os.path.join(
        os.path.dirname(__file__), "data/mlwh_fixtures"
    )
    create_schema(
        base=Base,
        drop=True,
        conf_file_path=test_config,
        conf_file_section="MySQL MLWH",
    )

    with get_connection(
        conf_file_path=test_config, conf_file_section="MySQL MLWH"
    ) as session:
        batch_load_from_yaml(
            session=session,
            fixtures_dir_path=fixtures_dir,
            module="npg_notify.db.mlwh",
        )
        return session
