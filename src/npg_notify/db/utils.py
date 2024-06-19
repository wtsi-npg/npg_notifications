import os
import importlib
import pathlib
import re
import yaml

from contextlib import contextmanager

from sqlalchemy import create_engine, text, insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, DeclarativeBase
from sqlalchemy_utils import create_database, database_exists, drop_database

from npg_notify.config import get_config_data


def db_credentials_from_config_file(
    conf_file_path: str, conf_file_section: str = None
):
    """Parses a configuration file, generates a database connection string.

    Args:
      conf_file_path:
        A configuration file with database connection details.
      conf_file_section:
        The section of the configuration file. Optional.

    Returns:
      MySQL connection string suitable for SQLAchemy
    """

    config = get_config_data(
        conf_file_path=conf_file_path, conf_file_section=conf_file_section
    )
    if "dbschema" not in config:
        raise Exception("Database schema name should be defined as dbschema")

    user_creds = config["dbuser"]
    # Not having a password is not that unusual for read-only access.
    if "dbpassword" in config:
        user_creds += ":" + config["dbpassword"]

    return (
        f"mysql+pymysql://{user_creds}@"
        f"{config['dbhost']}:{config['dbport']}/{config['dbschema']}?charset=utf8mb4"
    )


def get_db_connection_string(
    conf_file_path: str, conf_file_section: str = None
):
    """Generates a database connection string from supplied database credentials.

    Args:
      conf_file_path:
        A configuration file with database connection details. If the
        configuration file does not exist, assumes that the value is the name
        of the environment variable that holds the database connection string.
      conf_file_section:
        The section of the configuration file. Optional.

    Returns:
      MySQL connection string suitable for SQLAchemy
    """
    try:
        if os.path.exists(conf_file_path):
            url = db_credentials_from_config_file(
                conf_file_path, conf_file_section
            )
        else:
            url = os.environ.get("conf_file_path")
            if url is None or url == "":
                raise Exception(
                    f"{conf_file_path} is not a file path, neither it is a defined env. variable"
                )
    except Exception as err:
        raise Exception(
            "Failed to get db credentials: " + str(err.with_traceback(None))
        )

    return url


@contextmanager
def get_connection(
    conf_file_path: str, conf_file_section: str = None
) -> Session:
    """Connects to MySQL database and returns a database session.

    Using database credentials specified in the configuration file, establishes
    a connection to MySQL database and returns sqlalchemy.orm.Session object.

    Args:
      conf_file_path:
        A configuration file with database connection details.
      conf_file_section:
        The section of the configuration file. Optional.

    Returns:
      sqlalchemy.orm.Session object

    Example:
        with get_connection(
            conf_file_path=test_config, conf_file_section="MySQL MLWH"
        ) as session:
            pass
    """

    url = get_db_connection_string(
        conf_file_path=conf_file_path, conf_file_section=conf_file_section
    )
    engine: Engine = create_engine(url, echo=False)
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


def create_schema(
    base: DeclarativeBase,
    conf_file_path: str,
    conf_file_section: str = None,
    drop: bool = False,
):
    """Connects to MySQL database, creates a new schema.

    This method is good to use in unit tests. While it can be used for
    creating production instances from scratch, the correctness of the created
    schema cannot be guaranteed.

    Args:
      base:
        sqlalchemy.orm.DeclarativeBase object for the schema to be loaded.
      conf_file_path:
        A configuration file with database connection details.
      conf_file_section:
        The section of the configuration file. Optional.
      drop:
        A boolean option, defaults to False. If True, the existing tables of
        the database schema are dropped.
    """

    url = get_db_connection_string(
        conf_file_path=conf_file_path, conf_file_section=conf_file_section
    )
    engine: Engine = create_engine(url, echo=False)

    if database_exists(engine.url):
        if drop is True:
            drop_database(engine.url)
        else:
            raise Exception(
                "Cannot create a new database: "
                "Database exists, drop_database option is False"
            )

    create_database(engine.url)
    base.metadata.create_all(engine)
    with engine.connect() as conn:
        # Workaround for invalid default values for dates.
        # Needed only in CI.
        conn.execute(text("SET sql_mode = '';"))
        conn.commit()


def batch_load_from_yaml(
    session: Session, module: str, fixtures_dir_path: str
):
    """Loads data to the database.

    This method is good for use in unit tests, it is not intended
    for production use.

    Args:
      session:
        sqlalchemy.orm.Session object
      module:
        A string representing the name of the module where ORM classes
        for the database tables are defined.
      fixtures_dir_path:
        A path to the directory with YAML files containing data to load, one
        file per table. The names of the files should follow the pattern
        200-<ORM_CLASS_NAME>.yml The integer prefix can be any number,
        data from files with lower value of the prefix are loader first.

    Example:
        batch_load_from_yaml(
            session=session,
            fixtures_dir_path="tests/data/mlwh_fixtures",
            module="npg_notify.db.mlwh",
        )
    """
    # Load the schema module where the table ORM classes are defined.
    module = importlib.import_module(module)
    # Find all files in a given directory.
    dir_obj = pathlib.Path(fixtures_dir_path)
    file_paths = list(str(f) for f in dir_obj.iterdir())
    file_paths.sort()

    for file_path in file_paths:
        with open(file_path, "r") as f:
            (head, file_name) = os.path.split(file_path)
            # File name example: 200-PacBioRun.yml
            m = re.match(r"\A\d+-([a-zA-Z]+)\.yml\Z", file_name)
            if m is not None:
                class_name = m.group(1)
                table_class = getattr(module, class_name)
                data = yaml.safe_load(f)
                session.execute(insert(table_class), data)

    session.commit()
