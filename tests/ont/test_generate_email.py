import json

from pytest import mark as m
from structlog import get_logger

from npg_notify.db.mlwh import (
    find_flowcells_for_ont_run,
    find_plates_for_ont_run,
    get_study_contacts,
)
from npg_notify.ont.event import ContactEmail, EventType
from ont.conftest import ont_synthetic_mlwh

logger = get_logger()


@m.describe("Generate ONT email")
class TestGenerateONTEmail:
    @m.context("When an ONT event is serialized and deserialized")
    @m.it("Retains the correct values")
    def test_serialize_deserialize_event(self):
        expt = "experiment1"
        slot = 1
        flowcell_id = "FAKE12345"
        path = f"/testZone/home/irods/{expt}_{slot}_{flowcell_id}"
        event_type = EventType.UPLOADED

        event1 = ContactEmail(
            experiment_name=expt,
            instrument_slot=slot,
            flowcell_id=flowcell_id,
            path=path,
            event=event_type,
        )

        event2 = ContactEmail.from_serializable(
            json.loads(json.dumps(event1.to_serializable()))
        )

        assert event2.experiment_name == expt
        assert event2.instrument_slot == slot
        assert event2.flowcell_id == flowcell_id
        assert event2.path == path
        assert event2.event == event_type

    @m.context("When an ONT email is generated")
    @m.it("Has the correct subject and body")
    def test_generate_email(self, ont_synthetic_mlwh):
        expt = "multiplexed_experiment_000"
        slot = 1
        flowcell_id = "flowcell_m000"
        path = f"/testZone/home/irods/{expt}_{slot}_{flowcell_id}"
        event_type = EventType.UPLOADED
        domain = "no-such-domain.sanger.ac.uk"

        event = ContactEmail(
            experiment_name=expt,
            instrument_slot=slot,
            flowcell_id=flowcell_id,
            path=path,
            event=event_type,
        )

        assert (
            event.subject()
            == f"Update: ONT run {expt} flowcell {flowcell_id} has been {event_type}"
        )

        flowcells = find_flowcells_for_ont_run(
            ont_synthetic_mlwh,
            experiment_name=expt,
            instrument_slot=slot,
            flowcell_id=flowcell_id,
        )

        with open("tests/data/ont_email_body.txt", "r", encoding="utf-8") as f:
            expected = [line.rstrip() for line in f]
            assert event.body(flowcells, domain=domain).splitlines() == expected


class TestONTMLWH:
    @m.context("When the plate (stock resources) for a run are queried")
    @m.it("Returns the correct plates")
    def test_find_plates_for_run(self, ont_synthetic_mlwh):
        def assert_found(
            expt_name: str, slot: int, flowcell_id: str, expected_length: int
        ):
            plates = find_plates_for_ont_run(
                ont_synthetic_mlwh, expt_name, slot, flowcell_id
            )
            assert len(plates) == expected_length

        assert_found("simple_experiment_000", 1, "flowcell_s000", 1)
        assert_found("multiplexed_experiment_000", 1, "flowcell_m000", 12)
        assert_found("multiplexed_experiment_001", 1, "flowcell_m001", 12)

    @m.context("When contacts for a multiplexed run are queried")
    @m.it("Returns the correct contacts")
    def test_contacts_for_run(self, ont_synthetic_mlwh):
        expt = "multiplexed_experiment_000"
        slot = 1
        flowcell_id = "flowcell_m000"

        flowcells = find_flowcells_for_ont_run(
            ont_synthetic_mlwh,
            experiment_name=expt,
            instrument_slot=slot,
            flowcell_id=flowcell_id,
        )

        studies = {fc.study for fc in flowcells}
        assert len(studies) == 1
        assert get_study_contacts(ont_synthetic_mlwh, studies.pop().id_study_lims) == [
            "owner@sanger.ac.uk",
            "user1@sanger.ac.uk",
            "user2@sanger.ac.uk",
            "user3@sanger.ac.uk",
        ]
