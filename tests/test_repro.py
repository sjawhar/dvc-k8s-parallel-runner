from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Callable, List, Tuple

import dvc.repo
import dvc.stage
import git.repo
import pytest
import yaml

from neuromancer import path, repro

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture(name="setup_repo")
def fixture_setup_repo():
    def setup_repo(
        project_dir: pathlib.Path, sessions: List[str], is_new_project: bool = False
    ):
        project_dir.mkdir(exist_ok=True, parents=True)

        git_repo = git.repo.Repo.init(project_dir)
        dvc_repo: dvc.repo.Repo = dvc.repo.Repo.init(project_dir)

        (project_dir / "sessions.yaml").write_text(
            yaml.safe_dump({"sessions": [*sessions, "other", "sessions"]})
        )

        with open(project_dir / "dvc.yaml", "w") as file:
            yaml.safe_dump(
                {
                    "vars": ["sessions.yaml:sessions"],
                    "stages": {
                        "foo": {
                            "foreach": "${sessions}",
                            "do": {
                                "cmd": "echo foo ${item} ${value} > out-foo-${item}.txt",
                                "outs": ["out-foo-${item}.txt"],
                            },
                        },
                        "bar": {
                            "foreach": "${sessions}",
                            "do": {
                                "cmd": [
                                    "echo bar ${item} ${value} 1 > out-bar-${item}-1.txt",
                                    "echo bar ${item} ${value} 2 > out-bar-${item}-2.txt",
                                ],
                                "outs": [
                                    "out-bar-${item}-1.txt",
                                    {"out-bar-${item}-2.txt": {"cache": False}},
                                ],
                            },
                        },
                        "baz": {
                            "foreach": "${sessions}",
                            "do": {
                                "cmd": "echo baz ${item} ${value} > out-baz-${item}.txt",
                                "outs": ["out-baz-${item}.txt"],
                            },
                        },
                    },
                },
                file,
            )

        params_file = project_dir / "params.yaml"
        if not is_new_project:
            params_file.write_text(yaml.safe_dump({"value": "old"}))
            with path.run_in_dir(project_dir):
                dvc_repo.reproduce()

        params_file.write_text(yaml.safe_dump({"value": "new"}))
        git_repo.index.add(git_repo.untracked_files)
        git_repo.index.commit("Initial commit")

        return git_repo, dvc_repo

    return setup_repo


@pytest.mark.parametrize("is_new_project", [True, False])
@pytest.mark.parametrize("fetch", [True, False])
def test_repro(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    setup_repo: Callable[..., Tuple[git.repo.Repo, dvc.repo.Repo]],
    is_new_project: bool,
    fetch: bool,
):
    project_dir = tmp_path / "project"
    sessions = ["a", "b", "c", "d"]

    setup_repo(project_dir, sessions, is_new_project=is_new_project)

    worklist_file = project_dir / "sessions.txt"
    worklist_file.write_text(
        "\n".join(
            [
                ",".join([session_id, "dvc.yaml:foo", "dvc.yaml:bar"])
                for session_id in sessions
            ]
        )
    )

    idx_work_item = 2
    output_dir = project_dir / "results"

    spy_fetch = mocker.spy(dvc.repo.Repo, "fetch")
    spy_checkout = mocker.spy(dvc.repo.Repo, "checkout")
    spy_push = mocker.spy(dvc.repo.Repo, "push")

    repro.neuromancer(
        worklist_file, idx_work_item, output_dir, project_dir=project_dir, fetch=fetch
    )

    assert output_dir.exists()
    assert {
        str(file.relative_to(output_dir))
        for file in output_dir.rglob("*")
        if file.is_file()
    } == {
        "c/dvc.lock",
        "c/out-bar-c-2.txt",
    }
    assert yaml.safe_load((output_dir / "c/dvc.lock").read_text()) == {
        "schema": "2.0",
        "stages": {
            "foo@c": {
                "cmd": "echo foo c new > out-foo-c.txt",
                "outs": [
                    {
                        "path": "out-foo-c.txt",
                        "md5": mocker.ANY,
                        "size": 10,
                    }
                ],
            },
            "bar@c": {
                "cmd": [
                    "echo bar c new 1 > out-bar-c-1.txt",
                    "echo bar c new 2 > out-bar-c-2.txt",
                ],
                "outs": [
                    {
                        "path": "out-bar-c-1.txt",
                        "md5": mocker.ANY,
                        "size": 12,
                    },
                    {
                        "path": "out-bar-c-2.txt",
                        "md5": mocker.ANY,
                        "size": 12,
                    },
                ],
            },
        },
    }

    assert (output_dir / "c/out-bar-c-2.txt").read_text().strip() == "bar c new 2"

    assert spy_fetch.call_count == int(fetch)
    spy_checkout.assert_called_once()
    spy_push.assert_called_once()


def test_save_lock_files(
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    setup_repo: Callable[..., Tuple[git.repo.Repo, dvc.repo.Repo]],
):
    working_dir = tmp_path / "work"
    _, dvc_repo = setup_repo(working_dir, ["foo"])

    expected_stages = [
        stage for stage in dvc_repo.stages if stage.name.endswith("@foo")
    ]
    reproduced = [
        *expected_stages,
        dvc.stage.Stage(
            dvc_repo,
            path=working_dir / "data/raw/foo",
            dvcfile=mocker.Mock(path=working_dir / "data/raw/foo.dvc"),
        ),
    ]
    output_dir = tmp_path / "output"

    repro._save_lock_files(reproduced, working_dir, output_dir)

    assert {
        str(file.relative_to(output_dir))
        for file in output_dir.rglob("*")
        if file.is_file()
    } == {"dvc.lock"}
    assert {*yaml.safe_load((output_dir / "dvc.lock").read_text())["stages"]} == {
        stage.name for stage in expected_stages
    }
