from __future__ import annotations
from typing import Optional, Dict, Any


class Dataset:
    """Represents a dataset with input/output paths and stage metadata."""

    def __init__(self, dataset_path: str) -> None:
        """
        Args:
            dataset_path: Input path to the dataset (required)
        """
        self._input_path = dataset_path
        self._output_path: Optional[str] = None
        self._stage_info: Dict[str, Any] = {}  # 使用字典存储结构化信息

    @property
    def input_path(self) -> str:
        """Read-only access to input path"""
        return self._input_path

    @property
    def output_path(self) -> Optional[str]:
        """Current output path (None if not set)"""
        return self._output_path

    @output_path.setter
    def output_path(self, value: str) -> None:
        """Set output path with type validation"""
        if not isinstance(value, str):
            raise TypeError(f"Expected str for output_path, got {type(value)}")
        self._output_path = value

    @property
    def stage_info(self) -> Dict[str, Any]:
        """Copy of current stage metadata"""
        return self._stage_info.copy()  # 返回拷贝避免外部修改

    def update_stage_info(self, updates: Dict[str, Any], overwrite: bool = False) -> None:
        """
        Update stage metadata

        Args:
            updates: Key-value pairs to add/update
            overwrite: If True, replace existing info instead of merging
        """
        if not isinstance(updates, dict):
            raise TypeError("Stage updates must be a dictionary")

        if overwrite:
            self._stage_info = updates.copy()
        else:
            self._stage_info.update(updates)

    def __repr__(self) -> str:
        """Developer-friendly representation"""
        return (
            f"<Dataset(input={self._input_path!r}, "
            f"output={self._output_path!r}, "
            f"stages={len(self._stage_info)})>"
        )

    def validate_paths(self) -> bool:
        """Check if output path is set (basic validation example)"""
        return self._output_path is not None