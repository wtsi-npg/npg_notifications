import json

from pytest import mark as m

from npg_notify.ont.event import ContactEmail, EventType


@m.describe("Generate ONT email")
class TestGenerateONTEmail:

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
    @m.it("Has the correct subject")
    def test_generate_email(self):
        expt = "experiment1"
        slot = 1
        flowcell_id = "FAKE12345"
        path = f"/testZone/home/irods/{expt}_{slot}_{flowcell_id}"
        event_type = EventType.UPLOADED
        studies = ["study1", "study2"]

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

        study_lines = "\n".join(studies)

        assert event.body(*studies) == (
            f"The ONT run for experiment {expt}, flowcell {flowcell_id} has been {event_type}.\n"
            "The data are available in iRODS at the following path:\n"
            "\n"
            f"{path}\n"
            "\n"
            "This is an automated email from NPG. You are receiving it because you are registered\n"
            "as a contact for one or more of the Studies listed below:\n"
            "\n"
            f"{study_lines}\n"
        )
