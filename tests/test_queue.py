import pathlib
import shutil
from typing import Any, Dict, List, Optional

import dvc.repo
import pytest
import yaml

from neuromancer import queue


def test_handle_outputs(tmp_path: pathlib.Path):
    project_dir = tmp_path / "project"
    output_dir = project_dir / "results"
    output_dir.mkdir(exist_ok=True, parents=True)

    for session_id, pipeline, stages in [
        (
            "session_one",
            "foo",
            {"a@session_one": "session_one", "b@session_one": "session_one"},
        ),
        (
            "session_one",
            "goo",
            {"c@session_one": "session_one"},
        ),
        (
            "session_two",
            "foo",
            {"a@session_two": "session_two", "b@session_two": "session_two"},
        ),
    ]:
        lock_file = output_dir / f"{session_id}/pipelines/{pipeline}/dvc.lock"
        lock_file.parent.mkdir(exist_ok=True, parents=True)
        lock_file.write_text(yaml.safe_dump({"stages": stages}))

    for pipeline, stages in [("foo", ["a", "b", "d"]), ("goo", ["c", "e"])]:
        lock_file = project_dir / f"pipelines/{pipeline}/dvc.lock"
        lock_file.parent.mkdir(exist_ok=True, parents=True)
        lock_file.write_text(
            yaml.safe_dump(
                {
                    "schema": "2.0",
                    "stages": {
                        f"{stage}@{session_id}": "old"
                        for stage in stages
                        for session_id in [
                            "session_one",
                            "session_two",
                            "session_three",
                        ]
                    },
                }
            )
        )

    non_cached_out = (
        output_dir
        / "session_one/pipelines/foo/metrics/sessions/session_one/metrics.json"
    )
    non_cached_out.parent.mkdir(exist_ok=True, parents=True)
    non_cached_out.write_text("HELLO")

    queue._handle_outputs(output_dir, {"session_one", "session_two"}, project_dir)

    for pipeline, expected_stages in [
        (
            "foo",
            {
                "a@session_one": "session_one",
                "b@session_one": "session_one",
                "d@session_one": "old",
                "a@session_two": "session_two",
                "b@session_two": "session_two",
                "d@session_two": "old",
                "a@session_three": "old",
                "b@session_three": "old",
                "d@session_three": "old",
            },
        ),
        (
            "goo",
            {
                "c@session_one": "session_one",
                "e@session_one": "old",
                "c@session_two": "old",
                "e@session_two": "old",
                "c@session_three": "old",
                "e@session_three": "old",
            },
        ),
    ]:
        assert yaml.safe_load(
            (project_dir / f"pipelines/{pipeline}/dvc.lock").read_text()
        ) == {
            "schema": "2.0",
            "stages": expected_stages,
        }

    assert (
        project_dir / "pipelines/foo/metrics/sessions/session_one/metrics.json"
    ).read_text() == "HELLO"


@pytest.mark.parametrize(
    ["pipelines", "expected_stages"],
    [
        (
            None,
            {
                **{
                    session_id: [
                        ("first", "a1"),
                        ("first", "a2"),
                        ("second", "b1"),
                        ("second", "b2"),
                        ("second", "b3"),
                        ("second", "b4"),
                        ("third", "c1_single"),
                        ("third", "c2_all"),
                        ("third", "c3_all"),
                    ]
                    for session_id in ["single_1", "single_2", "single_3"]
                },
                **{
                    session_id: [
                        ("first", "a1"),
                        ("first", "a2"),
                        ("second", "b1"),
                        ("second", "b2"),
                        ("second", "b3"),
                        ("second", "b4"),
                        ("third", "c1_paired"),
                        ("third", "c2_all"),
                        ("third", "c2_paired"),
                        ("third", "c3_all"),
                        ("third", "c3_paired"),
                    ]
                    for session_id in [
                        "paired_1a",
                        "paired_1b",
                        "paired_2a",
                        "paired_2b",
                    ]
                },
            },
        ),
        (
            ["pipelines/first"],
            {
                session_id: [
                    ("first", "a1"),
                    ("first", "a2"),
                ]
                for session_id in [
                    "single_1",
                    "single_2",
                    "single_3",
                    "paired_1a",
                    "paired_1b",
                    "paired_2a",
                    "paired_2b",
                ]
            },
        ),
        (
            ["pipelines/first", "pipelines/second"],
            {
                session_id: [
                    ("first", "a1"),
                    ("first", "a2"),
                    ("second", "b1"),
                    ("second", "b2"),
                    ("second", "b3"),
                    ("second", "b4"),
                ]
                for session_id in [
                    "single_1",
                    "single_2",
                    "single_3",
                    "paired_1a",
                    "paired_1b",
                    "paired_2a",
                    "paired_2b",
                ]
            },
        ),
        (
            ["pipelines/second"],
            {
                session_id: [
                    ("second", "b1"),
                    ("second", "b2"),
                    ("second", "b3"),
                    ("second", "b4"),
                ]
                for session_id in [
                    "single_1",
                    "single_2",
                    "single_3",
                    "paired_1a",
                    "paired_1b",
                    "paired_2a",
                    "paired_2b",
                ]
            },
        ),
        (
            ["pipelines/third"],
            {
                **{
                    session_id: [
                        ("third", "c1_single"),
                        ("third", "c2_all"),
                        ("third", "c3_all"),
                    ]
                    for session_id in ["single_1", "single_2", "single_3"]
                },
                **{
                    session_id: [
                        ("third", "c1_paired"),
                        ("third", "c2_all"),
                        ("third", "c2_paired"),
                        ("third", "c3_all"),
                        ("third", "c3_paired"),
                    ]
                    for session_id in [
                        "paired_1a",
                        "paired_1b",
                        "paired_2a",
                        "paired_2b",
                    ]
                },
            },
        ),
    ],
)
def test_get_session_stages(
    tmp_path: pathlib.Path,
    pipelines: Optional[List[str]],
    expected_stages: Dict[str, Any],
):
    shutil.copytree(
        pathlib.Path(__file__).parent / "data_fixtures/repo",
        tmp_path,
        dirs_exist_ok=True,
        ignore=lambda src_dir, names: [
            name
            for name in names
            if pathlib.Path(src_dir, name).is_file() and not name.endswith(".yaml")
        ],
    )
    dvc_repo: dvc.repo.Repo = dvc.repo.Repo.init(str(tmp_path), no_scm=True, force=True)

    session_stages = queue._get_session_stages(dvc_repo, tmp_path, pipelines=pipelines)

    assert session_stages == {
        session_id: {
            f"pipelines/{pipeline}/dvc.yaml:{stage}" for pipeline, stage in stages
        }
        for session_id, stages in expected_stages.items()
    }


@pytest.mark.parametrize("fetch", [False, True])
def test_get_terraform_hcl(tmp_path: pathlib.Path, fetch: bool):
    completions = 10
    workers = 4
    image = "foobar:test"
    terraform_hcl = queue._get_terraform_hcl(
        completions=completions,
        image=image,
        project_dir=tmp_path,
        fetch=fetch,
        workers=workers,
    )

    task = next(iter(terraform_hcl["resource"]["iterative_task"].values()))
    assert task["image"] == image
    assert task["completions"] == completions
    assert task["parallelism"] == workers

    expected_flag = "--fetch" if fetch else "--no-fetch"
    assert expected_flag in task["script"].split(" ")
