from __future__ import annotations

import itertools
import logging
import pathlib
import shutil
import tempfile
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, cast

import dvc.repo
import git
import yaml

from neuromancer import path

if TYPE_CHECKING:
    from dvc.repo import Repo as DvcRepo
    from dvc.stage import Stage as DvcStage
    from git.repo import Repo as GitRepo

logger = logging.getLogger(__name__)


def neuromancer(
    worklist_file: pathlib.Path,
    idx_work_item: int,
    output_dir: pathlib.Path,
    project_dir: Optional[pathlib.Path] = None,
    fetch: bool = True,
):
    """Reproduce a set of parametrized stages for a single session and store
    the results in output_dir, for collection and aggregation by `wintermute`.

    Parameters
    ----------
    worklist_file : pathlib.Path
        Path to a file containing the entire worklist being reproduced by the
        parallel work group, with one work item per line. A work item is in
        format
            session_id,path/to/dvc.yaml:stage_a,path/to/dvc.yaml:stage_b,...
    idx_work_item : int
        The 0-indexed position of the work item in `worklist_file` which this
        worker should reproduce.
    output_dir : pathlib.Path
        Directory where results of this run should be stored
    project_dir : pathlib.Path | None, optional
        Directory containing the DVC project to reproduce. If not provided, the
        current directory is used.
    fetch : bool, optional
        If true (default), try to fetch from DVC cache before checking out.
    """
    output_dir.mkdir(exist_ok=True, parents=True)
    if project_dir is None:
        project_dir = pathlib.Path.cwd().resolve()

    work_item: str = worklist_file.read_text().strip().splitlines()[idx_work_item]
    session_id, *targets = work_item.split(",")
    logger.info("Using session_id %s", session_id)

    targets = [f"{target}@{session_id}" for target in targets]
    logger.debug("targets to reproduce: %s", targets)

    with tempfile.TemporaryDirectory() as tmp_dir, path.run_in_dir(tmp_dir):
        working_dir = pathlib.Path(tmp_dir)
        git_repo, dvc_repo = _setup_working_dir(project_dir, working_dir)

        if fetch:
            try:
                logger.info("Fetching DVC cache...")
                dvc_repo.fetch(targets=targets, with_deps=True)
            except Exception as error:
                logger.warning("Failed to fetch DVC cache", exc_info=error)

        try:
            dvc_repo.checkout(targets=targets, with_deps=True)
        except Exception as error:
            logger.warning(
                "Failed to checkout targets. This might be expected if stages have never been repro'd.",
                exc_info=error,
            )

        reproduced: List[DvcStage] = []
        try:
            logger.info("Reproducing targets for %s...", session_id)
            reproduced = dvc_repo.reproduce(targets=targets)
            logger.info("Reproduced %s", reproduced)
        finally:
            # Always try to push cache, even in case of error
            try:
                logger.info("Pushing cache...")
                dvc_repo.push(
                    targets=[stage.addressing for stage in reproduced] or targets
                )
            except Exception as error:
                logger.error("Failed to push DVC cache", exc_info=error)

        session_output_dir = output_dir / session_id
        _save_lock_files(reproduced, working_dir, session_output_dir)
        _save_uncached_files(git_repo, session_output_dir)

    logger.info("Done!")


def _setup_working_dir(
    project_dir: pathlib.Path, working_dir: pathlib.Path
) -> Tuple[GitRepo, DvcRepo]:
    logger.debug("Cloning %s to %s", project_dir, working_dir)
    git_repo = git.Repo.clone_from(project_dir, working_dir)

    with dvc.repo.Repo(working_dir).config.edit(level="local") as config:
        cast(Dict[str, Any], config).update(
            dvc.repo.Repo(project_dir).config.read(level="local")
        )
        config["core"]["autostage"] = True
    dvc_repo = dvc.repo.Repo(working_dir)
    logger.debug("new repo config: %s", dvc_repo.config)

    return git_repo, dvc_repo


def _save_lock_files(
    reproduced: List[DvcStage],
    working_dir: pathlib.Path,
    session_output_dir: pathlib.Path,
):
    stage_locks: Dict[pathlib.Path, Set[str]] = defaultdict(set)
    for stage in reproduced:
        dvc_file = pathlib.Path(stage.dvcfile.path)
        if dvc_file.name != "dvc.yaml":
            continue

        lock_file = dvc_file.with_suffix(".lock")
        stage_file, is_split, stage_name = stage.addressing.partition(":")
        if not is_split:
            stage_name = stage_file
        logger.debug("Adding %s from %s", stage_name, lock_file)
        stage_locks[lock_file].add(stage_name)

    for lock_file, stages in stage_locks.items():
        dvc_lock: Dict[str, Dict[str, str]] = yaml.safe_load(lock_file.read_text())
        dvc_lock["stages"] = {
            stage: stage_lock
            for stage, stage_lock in dvc_lock["stages"].items()
            if stage in stages
        }

        lock_file = session_output_dir / lock_file.relative_to(working_dir)
        logger.info("Saving lock_file: %s", lock_file)
        lock_file.parent.mkdir(exist_ok=True, parents=True)
        lock_file.write_text(yaml.safe_dump(dvc_lock))


def _save_uncached_files(git_repo: GitRepo, session_output_dir: pathlib.Path):
    for file in itertools.chain(
        git_repo.untracked_files,
        (
            diff.b_path
            for diff in cast(List[git.Diff], git_repo.index.diff(None))
            if diff.change_type == "M"
        ),
    ):
        file = pathlib.Path(cast(str, file))
        if file.is_dir() or file.name in {"dvc.lock", ".gitignore"}:
            continue

        file_dst = session_output_dir / file
        logger.info("Saving %s", file_dst)
        file_dst.parent.mkdir(exist_ok=True, parents=True)
        shutil.copy(file, file_dst, follow_symlinks=True)
