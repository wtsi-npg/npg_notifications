import random

from npg_notify.mail import generate_email

domain = "langqc.com"
langqc_run_url = "https://langqc.com/ui/run"
irods_docs_url = "https://confluence_irods.com/iRODS"
footer = (
    "The QC review is complete and your data should now be available "
    + "from iRODS (see https://confluence_irods.com/iRODS).\n"
    + "QC information for this run: https://langqc.com/ui/run/TRACTION-RUN.\n\n"
    + "If you have any questions or need further assistance, please feel free "
    + "to reach out to a Scientific Service Representative at dnap-ssr@langqc.com.\n\n"
    + "NPG on behalf of DNA Pipelines\n"
)

id_product = "f910e2fc6bd10913fb7df100e788192962d71d57c85e3d300c9fa3e24e6691db"


def get_well_data(libraries):
    return {
        "id_product": id_product,
        "label": "D1",
        "plate_number": 1,
        "run_name": "TRACTION-RUN-1333",
        "run_start_time": "2024-06-18T10:01:42",
        "run_complete_time": "2024-06-19T16:26:46",
        "well_start_time": "2024-06-18T11:44:43",
        "well_complete_time": "2024-06-20T14:36:27",
        "run_status": "Complete",
        "well_status": "Complete",
        "instrument_name": "84098",
        "instrument_type": "Revio",
        "qc_state": None,
        "libraries": libraries,
    }


def generate_tag():
    return "".join([random.choice(["C", "T", "G", "A"]) for i in range(1, 16)])


def test_generate_email_one_library():
    qc_outcome = {
        "qc_state": "Passed With Distinction",
        "is_preliminary": False,
        "qc_type": "sequencing",
        "outcome": True,
        "id_product": id_product,
        "date_created": "2024-06-28T14:22:18",
        "date_updated": "2024-06-28T14:24:47",
        "user": "user1@langqc.com",
        "created_by": "LangQC",
    }
    libraries = [
        {
            "study_id": "1234",
            "study_name": "Reference Genomes_ DNA",
            "sample_id": "778655549",
            "sample_name": "1234STDY13618009",
            "tag_sequence": ["CTGCGATCACGAGTAT"],
            "library_type": "Pacbio_HiFi",
            "pool_name": "TRAC-2-3818",
        }
    ]

    (subject, generated_content) = generate_email(
        domain=domain,
        langqc_run_url=langqc_run_url,
        irods_docs_url=irods_docs_url,
        qc_outcome=qc_outcome,
        well_data=get_well_data(libraries),
        libraries=libraries,
    )

    content = (
        "Study 1234: PacBio data is available\n\n"
        + "Study name: Reference Genomes_ DNA\n"
        + "Run: TRACTION-RUN-1333\n"
        + "Well label: D1\n"
        + "Plate number: 1\n"
        + "QC outcome: Passed With Distinction (Pass)\n"
        + "Samples:\n"
        + "\t1234STDY13618009,\n"
        + "\t1 sample in total\n\n"
    )

    assert subject == "Study 1234: PacBio data is available"
    assert generated_content == (
        content + footer.replace("TRACTION-RUN", "TRACTION-RUN-1333")
    )

    qc_outcome["outcome"] = None
    qc_outcome["qc_state"] = "Nobody can tell"
    (subject, generated_content) = generate_email(
        domain=domain,
        langqc_run_url=langqc_run_url,
        irods_docs_url=irods_docs_url,
        qc_outcome=qc_outcome,
        well_data=get_well_data(libraries),
        libraries=libraries,
    )

    content = (
        "Study 1234: PacBio data is available\n\n"
        + "Study name: Reference Genomes_ DNA\n"
        + "Run: TRACTION-RUN-1333\n"
        + "Well label: D1\n"
        + "Plate number: 1\n"
        + "QC outcome: Nobody can tell (Undefined)\n"
        + "Samples:\n"
        + "\t1234STDY13618009,\n"
        + "\t1 sample in total\n\n"
    )

    assert generated_content == (
        content + footer.replace("TRACTION-RUN", "TRACTION-RUN-1333")
    )


def test_generate_email_two_libraries():
    qc_outcome = {
        "qc_state": "Failed (Instrument)",
        "is_preliminary": False,
        "qc_type": "sequencing",
        "outcome": False,
        "id_product": id_product,
        "date_created": "2024-06-28T14:22:18",
        "date_updated": "2024-06-28T14:24:47",
        "user": "user1@langqc.com",
        "created_by": "LangQC",
    }

    libraries = [
        {
            "study_id": "1234",
            "study_name": "Reference Genomes_ DNA",
            "sample_id": "778655549",
            "sample_name": f"1234STDY13618009{i}",
            "tag_sequence": [generate_tag()],
            "library_type": "Pacbio_HiFi",
            "pool_name": "TRAC-2-3818",
        }
        for i in range(0, 5)
    ]

    (subject, generated_content) = generate_email(
        domain=domain,
        langqc_run_url=langqc_run_url,
        irods_docs_url=irods_docs_url,
        qc_outcome=qc_outcome,
        well_data=get_well_data(libraries),
        libraries=libraries[0:2],
    )

    content = (
        "Study 1234: PacBio data is available\n\n"
        + "Study name: Reference Genomes_ DNA\n"
        + "Run: TRACTION-RUN-1333\n"
        + "Well label: D1\n"
        + "Plate number: 1\n"
        + "QC outcome: Failed (Instrument) (Fail)\n"
        + "Samples:\n"
        + "\t1234STDY136180090,\n"
        + "\t1234STDY136180091,\n"
        + "\t2 samples in total\n\n"
    )

    assert subject == "Study 1234: PacBio data is available"
    assert generated_content == (
        content + footer.replace("TRACTION-RUN", "TRACTION-RUN-1333")
    )


def test_generate_email_seven_libraries():
    qc_outcome = {
        "qc_state": "Failed",
        "is_preliminary": False,
        "qc_type": "sequencing",
        "outcome": False,
        "id_product": id_product,
        "date_created": "2024-06-28T14:22:18",
        "date_updated": "2024-06-28T14:24:47",
        "user": "user1@langqc.com",
        "created_by": "LangQC",
    }
    libraries = [
        {
            "study_id": "1234",
            "study_name": "Reference Genomes_ DNA",
            "sample_id": "778655549",
            "sample_name": f"1234STDY13618009{i}",
            "tag_sequence": [generate_tag()],
            "library_type": "Pacbio_HiFi",
            "pool_name": "TRAC-2-3818",
        }
        for i in range(0, 7)
    ]

    (subject, generated_content) = generate_email(
        domain=domain,
        langqc_run_url=langqc_run_url,
        irods_docs_url=irods_docs_url,
        qc_outcome=qc_outcome,
        well_data=get_well_data(libraries),
        libraries=libraries,
    )

    content = (
        "Study 1234: PacBio data is available\n\n"
        + "Study name: Reference Genomes_ DNA\n"
        + "Run: TRACTION-RUN-1333\n"
        + "Well label: D1\n"
        + "Plate number: 1\n"
        + "QC outcome: Failed (Fail)\n"
        + "Samples:\n"
        + "\t1234STDY136180090,\n"
        + "\t1234STDY136180091,\n"
        + "\t1234STDY136180092,\n"
        + "\t1234STDY136180093,\n"
        + "\t1234STDY136180094,\n"
        + "\t.....\n"
        + "\t7 samples in total\n\n"
    )

    assert subject == "Study 1234: PacBio data is available"
    assert generated_content == (
        content + footer.replace("TRACTION-RUN", "TRACTION-RUN-1333")
    )
