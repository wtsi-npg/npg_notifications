import json

from npg_notify.ont.event import add_email_tasks_from_paths


class FakePipeline:
    def __init__(self):
        self.tasks = []

    def add(self, task):
        self.tasks.append(task)
        return True


def test_add_email_tasks_from_paths_opens_input_and_output_files(tmp_path):
    input_path = tmp_path / "input.json"
    output_path = tmp_path / "output.json"
    collection = {
        "avus": [
            {"attribute": "ont:experiment_name", "value": "ONTRUN-346"},
            {"attribute": "ont:flowcell_id", "value": "PBM31273"},
            {"attribute": "ont:instrument_slot", "value": "11"},
        ],
        "collection": "/seq/ont/promethion/ONTRUN-346/PBM31273",
    }
    input_path.write_text(json.dumps(collection) + "\n", encoding="utf-8")
    pipeline = FakePipeline()

    assert (
        add_email_tasks_from_paths(
            pipeline, "BASECALLED_SUP", str(input_path), str(output_path)
        )
        == (1, 1, 0)
    )

    assert len(pipeline.tasks) == 1
    task = pipeline.tasks[0]
    assert task.experiment_name == "ONTRUN-346"
    assert task.instrument_slot == 11
    assert task.flowcell_id == "PBM31273"
    assert task.path == "/seq/ont/promethion/ONTRUN-346/PBM31273"
    assert task.event == "BASECALLED_SUP"

    written_collection = json.loads(output_path.read_text(encoding="utf-8"))
    assert written_collection["collection"] == collection["collection"]
