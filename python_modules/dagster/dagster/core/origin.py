from collections import namedtuple
from typing import List

from dagster import check
from dagster.core.code_pointer import CodePointer
from dagster.serdes import create_snapshot_id, whitelist_for_serdes
from dagster.utils import frozenlist

DEFAULT_DAGSTER_ENTRY_POINT = frozenlist(["dagster"])


def get_python_environment_entry_point(executable_path: str) -> List[str]:
    return frozenlist([executable_path, "-m", "dagster"])


@whitelist_for_serdes
class RepositoryPythonOrigin(
    namedtuple(
        "_RepositoryPythonOrigin", "executable_path code_pointer container_image entry_point"
    ),
):
    """
    Derived from the handle structure in the host process, this is the subset of information
    necessary to load a target RepositoryDefinition in a "user process" locally.
    """

    def __new__(cls, executable_path, code_pointer, container_image=None, entry_point=None):
        return super(RepositoryPythonOrigin, cls).__new__(
            cls,
            check.str_param(executable_path, "executable_path"),
            check.inst_param(code_pointer, "code_pointer", CodePointer),
            check.opt_str_param(container_image, "container_image"),
            (
                frozenlist(check.list_param(entry_point, "entry_point", of_type=str))
                if entry_point != None
                else None
            ),
        )

    def get_id(self):
        return create_snapshot_id(self)

    def get_pipeline_origin(self, pipeline_name):
        check.str_param(pipeline_name, "pipeline_name")
        return PipelinePythonOrigin(pipeline_name, self)


@whitelist_for_serdes
class PipelinePythonOrigin(namedtuple("_PipelinePythonOrigin", "pipeline_name repository_origin")):
    def __new__(cls, pipeline_name, repository_origin):
        return super(PipelinePythonOrigin, cls).__new__(
            cls,
            check.str_param(pipeline_name, "pipeline_name"),
            check.inst_param(repository_origin, "repository_origin", RepositoryPythonOrigin),
        )

    def get_id(self):
        return create_snapshot_id(self)

    @property
    def executable_path(self):
        return self.repository_origin.executable_path

    def get_repo_pointer(self):
        return self.repository_origin.code_pointer
