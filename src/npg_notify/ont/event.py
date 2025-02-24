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

import argparse
import sys
from dataclasses import dataclass
from enum import Enum
from importlib import resources
from string import Template
from typing import Type

from npg.cli import add_io_arguments, add_logging_arguments
from npg.conf import IniData
from npg.log import configure_structlog
from partisan.irods import Collection
from sqlalchemy.orm import Session
from structlog import get_logger

import npg_notify
from npg_notify.db.mlwh import (
    OseqFlowcell,
    StockResource,
    find_flowcells_for_ont_run,
    find_plates_for_ont_run,
    find_studies_for_ont_run,
    get_study_contacts,
)
from npg_notify.db.utils import get_connection
from npg_notify.mail import send_notification
from npg_notify.ont.porch import Pipeline, Task

log = get_logger(__package__)

MYSQL_MLWH_CONFIG_FILE_SECTION = "MySQL MLWH"
PORCH_CONFIG_FILE_SECTION = "PORCH"
MAIL_CONFIG_FILE_SECTION = "MAIL"

description = """
This application sends email notifications to the contacts of the studies associated
with Oxford Nanopore Technology (ONT) runs. The emails are triggered by events such as
the run being uploaded to iRODS or basecalled.

The application can be run in two modes: 'add' or 'run'.

In 'add' mode, the application reads ONT run collections in baton JSON format from a
file or STDIN, one per line. These collections must have the standard ONT run metadata:

    - ont:experiment_name
    - ont:instrument_slot
    - ont:flowcell_id

For each collection, a new task is created and added to the Porch server. The collection
is then written to a file or STDOUT in baton JSON format, one per line.

In 'run' mode, the application retrieves any ONT event email tasks that have not been
done from the Porch server and runs them to send notifications. For each task, an email
is sent to all the contacts of the studies associated with the run.

The type of event to be reported in the email can be specified with the --event option.

The application requires a configuration file that specifies the Porch server URL and
authentication token, and the MySQL MLWH database connection details. The configuration
file should be in INI format and have the following sections:

    [MySQL MLWH]
    dbhost = <MySQL host>
    dbport = <MySQL port>
    dbuser = <MySQL user>
    dbpassword = <MySQL password>
    dbschema = <MySQL schema>

    [PORCH]
    url = <Porch server URL>
    admin_token = <Porch server admin authentication token>
    pipeline_token = <Porch server pipeline authentication token>

    [MAIL]
    domain = <email FQDN>
"""


class MetadataError(ValueError):
    """An exception raised when data selected for processing is missing metadata
    required for that operation."""

    pass


class EventType(str, Enum):
    """The events that can trigger an email."""

    UPLOADED = "uploaded"  # The run has been uploaded to iRODS
    BASECALLED = "basecalled"  # The run has been basecalled (for cases where we don't know the basecall type)
    BASECALLED_HAC = (
        "basecalled (HAC)"  # The run has been basecalled HAC (high accuracy)
    )
    BASECALLED_SUP = (
        "basecalled (SUP)"  # The run has been basecalled SUP (super-high accuracy)
    )
    BASECALLED_MOD = (
        "basecalled (MOD)"  # The run has been basecalled MOD (modified bases)
    )

    def __str__(self):
        return self.value


@dataclass(kw_only=True)
class ContactEmail(Task):
    """A task for sending ONT event email.

    An email is sent to the contacts of the studies associated with the ONT run.
    If the run is multiplexed, the email is sent to the contacts of all studies
    involved.

    The path to the run is part of the identity of the task, so creating tasks for
    multiple paths will send multiple emails. This is what we want because part of
    the purpose of the emails is to inform the contacts of the run's location.
    """

    @classmethod
    def from_serializable(cls, serializable: dict):
        return cls(**serializable)

    def __init__(
        self,
        experiment_name: str = None,
        instrument_slot: int = None,
        flowcell_id: str = None,
        path: str = None,
        event: EventType = None,
    ):
        """Create a new email task for an event on an ONT run.

        Args:
            experiment_name: The experiment name.
            instrument_slot: The instrument slot.
            flowcell_id: The flowcell ID
            path: The path to the collection containing the entire run.
            event: The type event that triggers the email.
        """
        super().__init__(Task.Status.PENDING)

        if experiment_name is None:
            raise ValueError("experiment_name is required")
        if instrument_slot is None:
            raise ValueError("instrument_slot is required")
        # GridION has 1-5, PromethION has 1-24
        if instrument_slot < 1 or instrument_slot > 24:
            raise ValueError("instrument_slot must be between 1 and 24")
        if flowcell_id is None:
            raise ValueError("flowcell_id is required")
        if path is None:
            raise ValueError("path is required")
        if event is None:
            raise ValueError("event is required")

        self.experiment_name = experiment_name
        self.instrument_slot = instrument_slot
        self.flowcell_id = flowcell_id
        self.path = path
        self.event = event

    def subject(self) -> str:
        """Return the subject of the email."""
        return (
            f"Update: ONT run {self.experiment_name} flowcell {self.flowcell_id} "
            f"has been {self.event}"
        )

    def body(self, flowcells: list[Type[OseqFlowcell]], domain: str = None) -> str:
        """Return the body of the email.

        Args:
            flowcells: OseqFlowcell records associated with this run (one per sample).
            domain: A network domain name to use when sending email. The email will
                be sent from mail.<domain> with <user>@<domain> in the From: header.
        """
        if domain is None:
            raise ValueError("domain is required")

        source = resources.files("npg_notify.data.resources").joinpath(
            "ont_event_email_template.txt"
        )

        breakdown, plates, studies = self._plate_summary(flowcells)
        with resources.as_file(source) as template:
            with open(template) as f:
                t = Template(f.read())
                body = t.substitute(
                    {
                        "experiment_name": self.experiment_name,
                        "flowcell_id": self.flowcell_id,
                        "path": self.path,
                        "event": self.event,
                        "studies": studies,
                        "plates": plates,
                        "breakdown": breakdown,
                        "domain": domain,
                    }
                )
                # Right trim any unused whitespace padding (helps in testing)
                return "\n".join([line.rstrip() for line in body.splitlines()])

    def to_serializable(self) -> dict:
        return {
            "experiment_name": self.experiment_name,
            "instrument_slot": self.instrument_slot,
            "flowcell_id": self.flowcell_id,
            "path": self.path,
            "event": self.event,
        }

    def __str__(self):
        return (
            f"<ONT experiment: {self.experiment_name} "
            f"instrument slot: {self.instrument_slot} "
            f"flowcell ID: {self.flowcell_id} "
            f"event: {self.event}>"
        )

    @staticmethod
    def _plate_summary(flowcells: list[Type[OseqFlowcell]]) -> tuple[str, str, str]:
        """Return a multi-line strings containing human-readable summaries of plates,
        studies and tags."""
        # Column format widths are reasonable guesses
        c1, c2, c3, c4, c5, c6 = 16, 4, 12, 32, 16, 6

        summary = [
            " ".join(
                [
                    "Plate".ljust(c1),
                    "Well".ljust(c2),
                    "Tag".rjust(c3),
                    "Tag Sequence".ljust(c4),
                    "Sample ID".ljust(c5),
                    "Study ID".ljust(c6),
                ]
            )
        ]

        plates = set()
        studies = set()

        for fc in flowcells:
            well: StockResource = fc.stock_resources
            plates.add(well.labware_human_barcode)
            studies.add(f"{fc.study.id_study_lims} ({fc.study.name})")

            summary.append(
                f"{well.labware_human_barcode:<{c1}} "
                f"{str(well.labware_coordinate):<{c2}} "  # Nullable
                f"{str(fc.tag_identifier):>{c3}} "  # Nullable
                f"{str(fc.tag_sequence):<{c4}} "  # Nullable
                f"{fc.sample.id_sample_lims:<{c5}} "
                f"{fc.study.id_study_lims:<{c6}}"
            )

        return (
            "\n".join(summary),
            "\n".join(sorted(plates)),
            "\n".join(sorted(studies)),
        )


def add_email_tasks(
    pipeline: Pipeline, event: EventType, reader, writer
) -> tuple[int, int, int]:
    """Add new ONT event email tasks to the pipeline.

    For each collection read from the reader, a new task is created and added to the
    pipeline. The collection is then written to the writer.

    Args:
        pipeline: The pipeline to which the tasks are added.
        event: The event that triggers the email.
        reader: A reader of ONT run collections in baton JSON format, one per line.
           These collections must have the standard ONT run metadata:
              - ont:experiment_name
              - ont:instrument_slot
              - ont:flowcell_id
        writer: A writer to which the collections are written after processing, also
           in baton JSON format, one per line.
    Returns:
        The number of collections processed, the number of tasks added, and the number
        of errors encountered.
    """
    np = ns = ne = 0

    for line in reader:
        np += 1
        try:
            coll = Collection.from_json(line)
            try:
                expt = coll.avu("ont:experiment_name").value
                slot = int(coll.avu("ont:instrument_slot").value)
                flowcell_id = coll.avu("ont:flowcell_id").value
            except ValueError as e:
                raise MetadataError(
                    "Collection does not have the metadata required for ONT event email"
                ) from e

            task = ContactEmail(
                experiment_name=expt,
                instrument_slot=slot,
                flowcell_id=flowcell_id,
                path=coll.path.as_posix(),
                event=event,
            )
            if pipeline.add(task):
                ns += 1
                log.info("Task added", pipeline=pipeline, task=task)
            else:
                log.info("Task already exists", pipeline=pipeline, task=task)

            print(coll.to_json(indent=None, sort_keys=True), file=writer)
        except Exception as e:
            ne += 1
            log.exception(e)

    return np, ns, ne


def run_email_tasks(
    pipeline: Pipeline, session: Session, domain: str
) -> tuple[int, int, int]:
    """Claim tasks from the pipeline and run them to send ONT event emails.

    For each task (representing an ONT run) an email will be sent to all the contacts
    of the studies associated with the run.

    Args:
        pipeline: The pipeline whose tasks are to be run.
        session: An open MLWH DB session.
        domain: A network domain name to use when sending email. The main will be sent
           from mail.<domain> with <user>@<domain> in the From: header.

    Returns:
        The number of tasks processed, the number of tasks that succeeded, and the
        number of errors encountered.
    """
    np = ns = ne = 0
    batch_size = 100

    for task in pipeline.claim(batch_size):
        try:
            np += 1
            studies = find_studies_for_ont_run(
                session, task.experiment_name, task.instrument_slot, task.flowcell_id
            )
            plates = find_plates_for_ont_run(
                session, task.experiment_name, task.instrument_slot, task.flowcell_id
            )
            flowcells = find_flowcells_for_ont_run(
                session, task.experiment_name, task.instrument_slot, task.flowcell_id
            )

            for plate in plates:
                log.info("Plate found", pipeline=pipeline, task=task, plate=plate)

            # We are sending a single email to all contacts of all studies in the run
            contacts = set()
            for study in studies:
                c = get_study_contacts(session=session, study_id=study.id_study_lims)
                contacts.update(c)

            log.info(
                "Preparing email",
                pipeline=pipeline,
                task=task,
                studies=[s.id_study_lims for s in studies],
                contacts=contacts,
            )

            if len(contacts) == 0:
                log.info(
                    "No contacts found", pipeline=pipeline, task=task, studies=studies
                )

                pipeline.done(task)
                ns += 1
                continue

            try:
                send_notification(
                    domain=domain,
                    contacts=sorted(contacts),
                    subject=task.subject(),
                    content=task.body(flowcells, domain=domain),
                )

                pipeline.done(task)
                ns += 1
            except Exception as e:
                ne += 1
                log.exception(e)

                # Retry on failure to send as it's likely to be transient.
                #
                # There is only one email per run, so it's safe to retry without the
                # risk of spamming the contacts; if it failed to send, nobody received
                # the email. This would not be the case if we were sending multiple
                # emails, one per study, for example.
                pipeline.retry(task)

        except Exception as e:
            ne += 1
            log.exception(e)

            # Retry on failure to query the MLWH as it's likely to be transient.
            pipeline.retry(task)

    return np, ns, ne


@dataclass
class EmailConfig:
    domain: str
    """The domain name to use when sending email. The main will be sent from mail.<domain>"""


def main():
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "action",
        type=str,
        help="The 'add' action acts as a producer by sending new notification "
        "tasks to the Porch server. The 'run' action acts as a consumer "
        "by retrieving any notification tasks that have not been done and "
        "running them to send notifications."
        ""
        "The remaining actions are for administrative purposes and require an admin "
        "token to be set in the configuration file."
        "The 'register' action registers the pipeline with the Porch server. It must "
        "be run once, before any tasks can be added or run. The 'token' action "
        "generates a new token for the pipeline. This token is used to authenticate "
        "the pipeline with the Porch server.",
        choices=["add", "run", "register", "token"],
    )
    parser.add_argument(
        "--event",
        type=str,
        help="The event that triggers the email.",
        choices=[e.name for e in EventType],
        default=EventType.UPLOADED,
    )

    parser.add_argument(
        "--conf-file-path",
        "--conf_file_path",
        type=str,
        required=True,
        help="Configuration file path.",
    )
    parser.add_argument(
        "--version",
        help="Print the version and exit.",
        action="version",
        version=npg_notify.version(),
    )
    add_io_arguments(parser)
    add_logging_arguments(parser)

    args = parser.parse_args()
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.json,
    )

    config_file = args.conf_file_path
    pipeline_config = IniData(Pipeline.ServerConfig).from_file(config_file, "PORCH")
    email_config = IniData(EmailConfig).from_file(config_file, "MAIL")

    pipeline = Pipeline(
        ContactEmail,
        name="ont-event-email",
        uri="https://github.com/wtsi/npg_notifications.git",
        version=npg_notify.version(),
        config=pipeline_config,
    )

    num_processed, num_succeeded, num_errors = 0, 0, 0

    action = args.action
    event = args.event
    if action == "add":
        num_processed, num_succeeded, num_errors = add_email_tasks(
            pipeline, event, args.input, args.output
        )
    elif action == "run":
        with get_connection(config_file, MYSQL_MLWH_CONFIG_FILE_SECTION) as session:
            num_processed, num_succeeded, num_errors = run_email_tasks(
                pipeline, session, email_config.domain
            )
    elif action == "register":
        pipeline.register()
    elif action == "token":
        print(pipeline.new_token("ont-event-email"))
    else:
        raise ValueError(f"Unknown action: {action}")

    if num_errors > 0:
        log.error(
            f"Failed to {action} some tasks",
            pipeline=pipeline,
            num_processed=num_processed,
            num_succeeded=num_succeeded,
            num_errors=num_errors,
        )
        sys.exit(1)

    log.info(
        f"Completed {action}",
        pipeline=pipeline,
        num_processed=num_processed,
        num_added=num_succeeded,
        num_errors=num_errors,
    )


if __name__ == "__main__":
    main()
