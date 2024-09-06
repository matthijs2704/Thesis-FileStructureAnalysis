import pandas as pd
import numpy as np
import regex as re
import os
from pathlib import Path

name_filters = ["days", "example", "demo", "tutorial", "challenge", "guide"]

description_filters = [
    "(curated|comprehensive|awesome|documented)?\ ?(list|collection)( of )?( awesome )?(.*)(projects|recipes|resources|algorithm|proof-of-concept|links|notes|sample|data structure|example|tutorial|tweets)",
    "interview questions",
    "design pattern",
    "starter kit",
    "\\bguide\\b",
    "pre-?(built|compiled) (binary|binaries)",
    "排行榜",
    "example",
    # " list "
    # "tutorial", "template"
    # UPDATED:
    "sample",
    "skeleton",
    "playground",
    "demonstrat(e|ing)",
    "accompany .* (book|series)",
]
description_filter_regex = "(" + ")|(".join(description_filters) + ")"

topic_filters = [
    # "list", Unsuitable: Too broad
    "lists",
    "tutorial",
    "awesome-list",
    "awesome-lists",
    "interview",
    "interview-questions",
    "interview-preparation-kit",
    "interview-prep",
    "interview-practice",
    # "blog", Unsuitable: Also filters blog software
    # "demo" Unsuitable: Too broad
    # "test", Unsuitable: Also filters projects for software testing
    "coding_challenge",
    "questions",
    "practice",
    "challenge",
    "leetcode",
    "best-practices",
    "algorithms",
    "roadmap",
    "template",
    "boilerplate",
    "guide",
    "guidelines",
    "styleguide",
    "style-guide",
    "cheatsheet",
    "wheel",  # Filter python wheels, very big binary repos,
    # UPDATED:
    "playground",
]

# Manually remove some of big projects, without source code
# Format: (owner_name, project_name)
excluded_projects = [
    ("covid19india", "api"),  # API: Dataset of covid data of india
    ("owid", "covid-19-data"),  # covid-19-data
    (
        "Hexxeh",
        "rpi-firmware",
    ),  # Compiled raspberry pi firmware, binaries, and repo is obsolete
    ("commaai", "comma10k"),  # Dataset containing only images, no software
]


def is_project_filtered(project: dict) -> bool:
    topics = project["topics"].lower().split("|")
    description = (
        "\\n".join(project["description"].splitlines())
        if isinstance(project["description"], str)
        else ""
    ).lower()
    name = project["name"].lower()

    # Check for ignored names, topics and descriptions
    ignored_name = any(i_name in name for i_name in name_filters)
    ignored_topic = any(topic.lower() in topic_filters for topic in topics)
    ignored_description = re.search(description_filter_regex, description) is not None

    ignored_exclusion = (project["owner.name"], project["name"]) in excluded_projects

    return ignored_name or ignored_topic or ignored_description or ignored_exclusion


def filter_projects(input_dataset: Path, output_dataset: Path):
    df = pd.read_csv(input_dataset, keep_default_na=False)

    # Use is_project_filtered to filter out projects
    df["ignored"] = df.apply(lambda project: is_project_filtered(project), axis=1)

    df.to_csv(output_dataset, index=False)
