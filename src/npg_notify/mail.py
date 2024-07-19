import logging
import os
import smtplib
from email.message import EmailMessage
from email.utils import parseaddr

logger = logging.getLogger("npg_notify")


def send_notification(
    domain: str, contacts: list[str], subject: str, content: str
):
    """Sends an email.

    Sending an email succeeds if at least one recipient is likely to receive it.

    Args:
      domain:
        The domain of the mail server which is used for sending the email.
      contacts:
        A non-empty list of valid email addresses.
      subject:
        The subject line of the email, a non-empty string.
      content:
        The content of the email, a non-empty string.
    """
    user_name = os.environ.get("USER")
    if not user_name:
        raise ValueError("USER not set in the environment.")

    if subject == "":
        raise ValueError("Email subject cannot be empty.")
    if content == "":
        raise ValueError("Email content cannot be empty.")

    if len(contacts) == 0:
        raise ValueError("List of contacts cannot not be empty.")
    validated_contacts = [
        valid_address
        for valid_address in [parseaddr(addr)[1] for addr in contacts]
        if valid_address != ""
    ]
    if len(validated_contacts) != len(contacts):
        logger.warning(
            "Some contact emails are invalid in " + ", ".join(contacts)
        )
    if len(validated_contacts) == 0:
        raise ValueError(
            "All contact emails are invalid in " + ", ".join(contacts)
        )

    recipients = ", ".join(validated_contacts)
    logger.debug(f"Sending an email to {recipients}")

    msg = EmailMessage()
    msg.set_content(content)
    msg["Subject"] = subject
    msg["To"] = recipients
    msg["From"] = f"{user_name}@{domain}"
    s = smtplib.SMTP(f"mail.{domain}")
    # Sending the message might fail, then there will be an error we
    # do not have to catch here. Sending succeeds if at least one
    # recipient is likely to receive the message. The return value
    # contains information about failed attempts. If this happens it is
    # safer to consider the message as being sent.
    reply = s.send_message(msg)
    s.quit()
    if len(reply) != 0:
        logger.warning(reply)


def generate_email_pac_bio(
    domain: str,
    langqc_run_url: str,
    irods_docs_url: str,
    qc_outcome: dict,
    well_data: dict,
    libraries: list,
) -> tuple[str, str]:
    """Generates the subject line and the content for a notification.

    This code is specific for the PacBio platform.

    Args:
      domain:
        The domain for any email addresses in the content of the notification.
      langqc_run_url:
        LangQC run URL for the recipient to refer to.
      irods_docs_url:
        iRODS documentation page URL for the recipient to refer to.
      qc_outcome:
        A dictionary representing information about the QC outcome for
        for the PacBio well.
      well_data:
        A dictionary representing information about a well.
      libraries:
        A list of dictionaries, which represent individual libraries.
        All libraries in this list should belong to the same study.

    Returns:
      A tuple of two strings, the subject line and the content for the
      email notification.
    """
    study_ids = {lib["study_id"] for lib in libraries}
    if len(study_ids) != 1:
        raise ValueError(
            "Libraries from different studies in 'libraries' attribute"
        )

    run_name = well_data["run_name"]
    plate_number = (
        well_data["plate_number"] if well_data["plate_number"] else "n/a"
    )
    outcome = "Undefined"
    if qc_outcome["outcome"] is not None:
        outcome = "Pass" if qc_outcome["outcome"] is True else "Fail"

    study_id = study_ids.pop()
    study_name = libraries[0]["study_name"]

    subject = f"Study {study_id}: PacBio data is available"

    lines = [
        subject,
        "",
        f"Study name: {study_name}",
        f"Run: {run_name}",
        f"Well label: {well_data['label']}",
        f"Plate number: {plate_number}",
        f"QC outcome: {qc_outcome['qc_state']} ({outcome})",
        "Samples:",
    ]
    num_samples = len(libraries)
    num_samples_listed = num_samples if num_samples <= 5 else 5
    for name in [
        lib["sample_name"] for lib in libraries[0:num_samples_listed]
    ]:
        lines.append(f"\t{name},")

    if num_samples > num_samples_listed:
        lines.append("\t.....")

    lines = lines + [
        f"\t{num_samples} sample"
        + ("s" if num_samples > 1 else "")
        + " in total",
        "",
        "The QC review is complete and your data should now be available from"
        + f" iRODS (see {irods_docs_url}).",
        f"QC information for this run: {'/'.join([langqc_run_url, run_name])}.",
        "",
        "If you have any questions or need further assistance, please feel "
        + "free to reach out to a Scientific Service Representative at "
        + f"dnap-ssr@{domain}.",
        "",
        "NPG on behalf of DNA Pipelines\n",
    ]

    return (subject, "\n".join(lines))
