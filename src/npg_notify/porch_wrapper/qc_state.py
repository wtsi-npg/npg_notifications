import argparse
import os
from urllib.parse import urljoin

import requests
from npg_notify.config import get_config_data
from npg_porch_cli.api import Pipeline, PorchAction
from npg_porch_cli.api import send as send_porch_request

LANGQC_CONFIG_FILE_SECTION = "LANGQC"
PORCH_CONFIG_FILE_SECTION = "PORCH"

VERIFY_CERTIFICATE = False  # TODO: change to True
CLIENT_TIMEOUT = (10, 60)


def run():
    parser = argparse.ArgumentParser(
        prog="qc_state_notification",
        description="Creates or processes notifications about QC states.",
    )
    parser.add_argument(
        "action",
        type=str,
        help="Action to send to npg_porch server API",
        choices=["register"],
    )
    parser.add_argument(
        "--conf_file_path",
        type=str,
        required=True,
        help="Configuration file path.",
    )
    args = parser.parse_args()

    create_tasks(conf_file_path=args.conf_file_path)


def create_tasks(conf_file_path: str):
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
    # query LangQC for recently QC-ed wells
    pipeline = Pipeline(
        name=porch_conf["pipeline_name"],
        uri=porch_conf["pipeline_uri"],
        version=porch_conf["pipeline_version"],
    )

    langqc_conf = get_config_data(
        conf_file_path=conf_file_path,
        conf_file_section=LANGQC_CONFIG_FILE_SECTION,
    )
    qc_states = _send_request(
        url=urljoin(langqc_conf["api_url"], langqc_conf["recently_qced_url"])
    )

    num_products = len(qc_states)
    print(
        f"Retrieved QC states for {num_products} products."
    )  # TODO: log properly

    os.environ["NPG_PORCH_TOKEN"] = porch_conf["npg_porch_token"]
    del porch_conf["npg_porch_token"]

    # qc_states is a dictionary where keys are product IDs and values are
    # lists of QC states. A list of one QC state is expected.
    num_errors = 0
    for product_id, qc_state_data in qc_states.items():
        try:
            create_task(
                porch_config=porch_conf,
                pipeline=pipeline,
                qc_state=qc_state_data[0],
            )
            # TODO: in DEBUG mode log the new task returned by this call.
        except Exception as err:
            # TODO: log the error message as an error
            print(
                f"Error registering QC state for product {product_id}: {str(err)}"
            )
            num_errors += 1

    del os.environ["NPG_PORCH_TOKEN"]

    print(
        f"{num_errors} errors when registering products."
        f"Registered QC states for {num_products-num_errors} products."
    )  # TODO: log properly


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
        validate_ca_cert=VERIFY_CERTIFICATE,
        porch_url=porch_config["api_url"],
        action="add_task",
        task_input=qc_state,
    )
    send_porch_request(action=action, pipeline=pipeline)


def _send_request(url: str):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    request_args = {
        "headers": headers,
        "timeout": CLIENT_TIMEOUT,
        "verify": VERIFY_CERTIFICATE,
    }

    response = requests.request("GET", url, **request_args)
    if not response.ok:
        raise Exception(
            f"Sending GET request to {url} failed. "
            f'Status code {response.status_code} "{response.reason}" '
            f"received from {response.url}"
        )

    return response.json()
