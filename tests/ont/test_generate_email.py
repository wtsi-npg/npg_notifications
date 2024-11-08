import json

from pytest import mark as m

from npg_notify.db.mlwh import Study
from npg_notify.ont.event import ContactEmail, EventType


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
    def test_generate_email(self):
        expt = "experiment1"
        slot = 1
        flowcell_id = "FAKE12345"
        path = f"/testZone/home/irods/{expt}_{slot}_{flowcell_id}"
        event_type = EventType.UPLOADED

        domain = "no-such-domain.sanger.ac.uk"
        studies = [Study(name="study1"), Study(name="study2")]

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

        study_descs = [f"{s.id_study_lims} ({s.name})" for s in studies]
        study_lines = "\n".join(study_descs)

        assert event.body(studies, domain=domain) == (
            f"The ONT run for experiment {expt}, flowcell {flowcell_id} has been {event_type}. The data are available in iRODS at the following path:\n"
            "\n"
            f"{path}\n"
            "\n"
            "This is an automated email from NPG. You are receiving it because you are registered as a contact for one or more of the Studies listed below:\n"
            "\n"
            f"{study_lines}\n"
            "\n"
            f"If you have any questions or need further assistance, please feel free to contact a Scientific Service Representative at dnap-ssr@{domain}.\n"
            "\n"
            "NPG on behalf of DNA Pipelines.\n"
        )
