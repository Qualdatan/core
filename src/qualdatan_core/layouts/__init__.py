# SPDX-License-Identifier: AGPL-3.0-only
"""Configurable folder-tree layouts for qualitative samples."""

from .folder import (
    DEFAULT_LAYOUT,
    Company,
    CompanyProject,
    FolderLayout,
    Subject,
    SubjectFolder,
    list_companies,
    list_subjects,
    parse_folder,
    parse_project_folder,
    scan_company,
    scan_subject,
)

__all__ = [
    "DEFAULT_LAYOUT",
    "FolderLayout",
    "Subject",
    "SubjectFolder",
    "list_subjects",
    "parse_folder",
    "scan_subject",
    # Backward-compat aliases:
    "Company",
    "CompanyProject",
    "list_companies",
    "scan_company",
    "parse_project_folder",
]
