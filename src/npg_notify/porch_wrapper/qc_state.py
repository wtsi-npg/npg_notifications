import argparse
import logging
import os
import re
import sys
import time
from pathlib import PurePath
from urllib.parse import urljoin

from npg_notify.config import get_config_data
from npg_notify.db.mlwh import get_study_contacts
from npg_notify.db.utils import get_connection, get_db_connection_string
from npg_notify.mail import generate_email_pac_bio, send_notification
from npg_porch_cli import send_request
from npg_porch_cli.api import Pipeline, PorchAction
from npg_porch_cli.api import send as send_porch_request

SSL_CONFIG_FILE_SECTION = "SSL"
LANGQC_CONFIG_FILE_SECTION = "LANGQC"
PORCH_CONFIG_FILE_SECTION = "PORCH"
IRODS_CONFIG_FILE_SECTION = "IRODS"
MAIL_CONFIG_FILE_SECTION = "MAIL"

logger = logging.getLogger("npg_notify")


def run():
    _configure_logger()
    logger.info("Started")

    parser = argparse.ArgumentParser(
        prog="qc_state_notification",
        description="Creates or processes notifications about QC states.",
    )
    parser.add_argument(
        "action",
        type=str,
        help="A task to perform.",
        choices=["register", "process"],
    )
    parser.add_argument(
        "--conf_file_path",
        type=str,
        required=True,
        help="Configuration file path.",
    )
    args = parser.parse_args()

    conf_file_path = args.conf_file_path
    ssl_cert_file = _get_ssl_cert_file_path(conf_file_path=conf_file_path)
    if ssl_cert_file:
        os.environ["SSL_CERT_FILE"] = ssl_cert_file
        # For a custom CA SSL certificate, the directory  assigned to the
        # REQUESTS_CA_BUNDLE env. variable should have been 'treated' with the
        # c_rehash tool, which is supplied with the Python interpreter, see
        # https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification
        os.environ["REQUESTS_CA_BUNDLE"] = str(PurePath(ssl_cert_file).parent)

    action = args.action
    if action == "register":
        success = create_tasks(conf_file_path=conf_file_path)
    elif action == "process":
        success = process_task(conf_file_path=conf_file_path)
    else:
        raise Exception(f"Action '{action}' is not implemented")

    logger.info("Finished")
    exit(0 if success else 1)


def create_tasks(conf_file_path: str) -> bool:
    """Retrieves and registers with Porch recently assigned QC states.

    Retrieves from LangQC API all recently (within the last four weeks) assigned
    final QC states for PacBio wells. Registers these states as pending with
    one of the pipelines registered with Porch API. URLs to use with these APIs,
    details of the Porch pipeline and Porch authorization token should be given
    in the configuration file.

    All errors in task registration are captured and logged.

    Args:
      conf_file_path:
        Configuration file path for this application.
    """

    porch_conf = get_config_data(
        conf_file_path=conf_file_path,
        conf_file_section=PORCH_CONFIG_FILE_SECTION,
    )
    pipeline = _pipeline_object(porch_conf)

    # query LangQC for recently QC-ed wells
    langqc_conf = get_config_data(
        conf_file_path=conf_file_path,
        conf_file_section=LANGQC_CONFIG_FILE_SECTION,
    )
    qc_states = send_request(
        validate_ca_cert=_validate_ca_cert(),
        url=urljoin(langqc_conf["api_url"], langqc_conf["recently_qced_path"]),
        method="GET",
        auth_type=None,
    )

    num_products = len(qc_states)
    logger.info(f"Retrieved QC states for {num_products} products.")

    os.environ["NPG_PORCH_TOKEN"] = porch_conf.pop("npg_porch_token")

    # qc_states is a dictionary where keys are product IDs and values are
    # lists of QC states. A list of one QC state is expected  because we are
    # limiting to sequencing products only.
    num_errors = 0
    for product_id, qc_state_data in qc_states.items():
        try:
            task = create_task(
                porch_config=porch_conf,
                pipeline=pipeline,
                qc_state=qc_state_data[0],
            )
            logger.debug(f"Created a new task {task}")
        except Exception as err:
            logger.error(
                f"Error registering task for pipeline {pipeline.name} with "
                f"QC state change of {product_id}: {str(err)}"
            )
            num_errors += 1

    del os.environ["NPG_PORCH_TOKEN"]

    logger.info(
        f"{num_errors} errors when registering products. "
        f"Registered QC states for {num_products-num_errors} products."
    )

    return True if not num_errors else False


def create_task(
    porch_config: dict[str], pipeline: Pipeline, qc_state: dict[str]
):
    """Creates and queues a single porch task for the notification pipeline.

    Args:
      porch_config:
        A dictionary of porch-related configuration parameters.
      pipeline:
        npg_porch_cli.api.Pipeline object
      qc_state:
        A Python object that can be encoded into a JSON string. This object
        uniquely defines a porch task for a given pipeline.
    """

    action = PorchAction(
        validate_ca_cert=_validate_ca_cert(),
        porch_url=porch_config["api_url"],
        action="add_task",
        task_input=qc_state,
    )
    send_porch_request(action=action, pipeline=pipeline)


def process_task(conf_file_path) -> bool:
    """Processes one task for the email notification pipeline.

    Performs the following steps:
        1. claims one porch task,
        2. gets necessary details about the product (PacBio well) from the
           LangQC API,
        3. for each study linked to the product gets contact details from
           ml warehouse,
        4  for each study sends an email notification,
        5. using porch API, updates the claimed task status to `DONE`, `FAIL`,
           or resets it back to `PENDING`.

    If an error occurs at steps 2 or 3, the task will be re-assigned the
    `PENDING` status.

    On step 4 an attempt to send an email will be made separately for each
    study. If this fails for one of the studies, the task will is assigned the
    `FAIL` status.

    Step 5 can fail, then the task will remain as claimed. Tasks like this
    should be mopped up manually. The log will contain information about the
    intended state of the task.

    The event log (table) of the porch database contains information about all
    task status updates. This information can be used to identify tasks, which
    has their status updated repeatedly.
    """

    porch_config = get_config_data(
        conf_file_path=conf_file_path,
        conf_file_section=PORCH_CONFIG_FILE_SECTION,
    )
    langqc_conf = get_config_data(
        conf_file_path=conf_file_path,
        conf_file_section=LANGQC_CONFIG_FILE_SECTION,
    )
    irods_config = get_config_data(
        conf_file_path=conf_file_path,
        conf_file_section=IRODS_CONFIG_FILE_SECTION,
    )
    mail_config = get_config_data(
        conf_file_path=conf_file_path,
        conf_file_section=MAIL_CONFIG_FILE_SECTION,
    )

    # Get all config data or error before claiming the task.
    porch_api_url = porch_config["api_url"]
    pac_bio_well_libraries_path = langqc_conf["pac_bio_well_libraries_path"]
    langqc_base_url = langqc_conf["api_url"]
    pac_bio_run_iu_path = langqc_conf["pac_bio_run_iu_path"]
    irods_docs_url = irods_config["user_manual_url"]
    domain = mail_config["domain"]
    # Check that database credentials are in place
    get_db_connection_string(
        conf_file_path=conf_file_path, conf_file_section="MySQL MLWH"
    )

    os.environ["NPG_PORCH_TOKEN"] = porch_config["npg_porch_token"]
    pipeline = _pipeline_object(porch_config)
    action = PorchAction(
        validate_ca_cert=_validate_ca_cert(),
        porch_url=porch_api_url,
        action="claim_task",
    )
    claimed_tasks = send_porch_request(action=action, pipeline=pipeline)
    if len(claimed_tasks) == 0:
        logger.info("No pending tasks returned from porch")
        return True

    claimed_task = claimed_tasks[0]
    logger.debug(f"Claimed task {claimed_task}")

    new_task_status = "DONE"

    # Get product lims data from LangQC - a platform-specific step.
    # For PacBio the product is a well.
    product_id = claimed_task["task_input"]["id_product"]
    try:
        url = re.sub(
            r"\[\w+\]",
            product_id,
            pac_bio_well_libraries_path,
        )
        url = urljoin(langqc_base_url, url)
        response = send_request(
            validate_ca_cert=_validate_ca_cert(),
            url=url,
            method="GET",
            auth_type=None,
        )
        libraries_per_study = {}
        for library in response["libraries"]:
            study_id = library["study_id"]
            if study_id in libraries_per_study:
                libraries_per_study[study_id].append(library)
            else:
                libraries_per_study[study_id] = [library]

        study_ids = libraries_per_study.keys()
        contacts_per_study = {}

        with get_connection(
            conf_file_path=conf_file_path, conf_file_section="MySQL MLWH"
        ) as session:
            for study_id in study_ids:
                contacts_per_study[study_id] = get_study_contacts(
                    session=session, id=study_id
                )

    except Exception as err:
        logger.error(str(err))
        new_task_status = "PENDING"

    task_input = claimed_task["task_input"]

    if new_task_status == "DONE":
        for study_id, contacts in contacts_per_study.items():
            if len(contacts) == 0:
                print(f"No contacts are registered for study {study_id}")
                continue

            try:
                (subject, text) = generate_email_pac_bio(
                    domain=domain,
                    langqc_run_url=urljoin(
                        langqc_base_url, pac_bio_run_iu_path
                    ),
                    irods_docs_url=irods_docs_url,
                    qc_outcome=task_input,
                    well_data=response,
                    libraries=libraries_per_study[study_id],
                )  # platform-specific step
                send_notification(
                    domain=domain,
                    contacts=contacts,
                    subject=subject,
                    content=text,
                )
            except Exception as err:
                logger.error(
                    "Error generating or sending a notification: " + str(err)
                )
                new_task_status = "FAILED"

    return _update_task_status(
        porch_api_url, pipeline, task_input, new_task_status
    )


def _pipeline_object(porch_conf: dict):
    return Pipeline(
        name=porch_conf["pipeline_name"],
        uri=porch_conf["pipeline_uri"],
        version=porch_conf["pipeline_version"],
    )


def _update_task_status(porch_api_url, pipeline, task_input, task_status):
    action = PorchAction(
        validate_ca_cert=_validate_ca_cert(),
        porch_url=porch_api_url,
        action="update_task",
        task_input=task_input,
        task_status=task_status,
    )

    num_attempts = 3
    i = 0
    success = False

    message = f"porch task {task_input} to status {task_status}"

    while i < num_attempts:
        try:
            send_porch_request(action=action, pipeline=pipeline)
            logger.info(f"Updated {message}")
            success = True
            i = num_attempts
        except Exception as err:
            logger.warning(
                "Error: " + str(err) + f"\nwhile trying to update {message}"
            )
            if i != (num_attempts - 1):
                time.sleep(60 + (i * 300))  # sleep 60 or 360 sec
            i = +1
    if not success:
        logger.error(f"Failed to update {message}.")

    return success


def _get_ssl_cert_file_path(conf_file_path: str):
    ssl_cert_file = None
    try:
        ssl_conf = get_config_data(
            conf_file_path=conf_file_path,
            conf_file_section=SSL_CONFIG_FILE_SECTION,
        )
        ssl_cert_file = ssl_conf["ssl_cert_file"]
        if not os.path.isfile(ssl_cert_file):
            ssl_cert_file = None
    except Exception as err:
        logger.warning(str(err))

    return ssl_cert_file


def _validate_ca_cert():
    return True if "SSL_CERT_FILE" in os.environ else False


def _configure_logger():
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        datefmt="%Y:%m:%d %H:%M:%S",
        format="%(asctime)s %(levelname)s %(message)s",
    )
