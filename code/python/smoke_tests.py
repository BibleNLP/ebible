"""
Smoke tests for checking the extract files.
These are intended to be run when the extract files are updated.
By default it will use the `corpus` dir in the base of the project,
but you can set the `CORPUS_PATH` environment variable to point it somewhere else -
this might be useful during development.
See https://github.com/BibleNLP/ebible/issues/53 for more context.

Tests are intended to be run from this directory, e.g.
  python smoke_tests.py
    or
  poetry run python smoke_tests.py

Currently it doesn't have any special dependencies so should not need poetry or any particular
python version.

Note these tests are quite CPU intensive so some thought is needed if we decide
to integrate them into the build pipeline.
"""

import unittest
import os
from pathlib import Path
from typing import List
import regex


class SmokeTest(unittest.TestCase):
    def setUp(self):
        corpus_path_lookup = os.getenv("CORPUS_PATH")
        if corpus_path_lookup:
            self.corpus_path = Path(corpus_path_lookup)
        else:
            self.corpus_path = Path(".") / os.pardir / os.pardir / "corpus"

    def test_number_of_files(self) -> None:
        all_extract_filenames = os.listdir(self.corpus_path)
        self.assertGreater(
            len(all_extract_filenames),
            1000,
            "There should be at least 1000 corpus files",
        )

    def test_all_files_have_at_least_400_verses(self) -> None:

        def verse_count(path: Path) -> int:
            with open(path, "r") as fp:
                lines = fp.readlines()
                return sum(map(lambda line: 1 if line.strip() else 0, lines))

        paths_with_less_than_400_verses = [
            path
            for path in os.listdir(self.corpus_path)
            if verse_count(self.corpus_path / path) < 400
        ]

        self._report_possible_failures(
            paths_with_less_than_400_verses, "file has at least 400 non-empty verses"
        )

    def test_filename_format_correct(self) -> None:

        def is_valid(filename: str) -> bool:
            parts = filename.split("-")
            if len(parts) < 2:
                return False
            language_code = parts[0]
            if not regex.match("[a-z]{2,3}", language_code):
                return False
            if not filename.endswith(".txt"):
                return False
            return True

        invalid_filenames = [
            filename
            for filename in os.listdir(self.corpus_path)
            if not is_valid(filename)
        ]

        self._report_possible_failures(
            invalid_filenames,
            "filename adheres to format `{language code}-{project}.txt`",
        )

    def test_certain_bibles_exist_in_corpus(self):
        filenames = [
            "eng-eng-kjv.txt",
            # TODO - add more on advice of someone familiar with stable versions
            # We want versions that are unlikely to be removed from eBible.org over time
        ]

        missing_bibles = [
            filename
            for filename in filenames
            if not (self.corpus_path / filename).exists()
        ]

        self._report_possible_failures(
            missing_bibles, "extract file exists for the Bible"
        )

    def test_certain_bibles_have_complete_OT_NT(self):
        filenames = [
            "eng-engylt.txt",
            "eng-engULB.txt",
            "fra-fraLSG.txt",
            # TODO - add more on advice of someone familiar with stable versions
            # We want versions that are unlikely to be removed from eBible.org over time
            # and have complete OT and NT (and no ranges)
        ]

        # Note that the extract file is structured:
        # - ot
        # - nt
        # - deuterocanonical
        # So we're just checking the front of the extract file has verses
        num_ot_nt_verses = 31170

        def is_99_percent_complete(path: Path) -> int:
            """
            Returns if the extract at the path has 99% OT and NT.
            We don't require 100% to cater for random glitches like missing verses,
            slightly different versifications and other weird little differences
            """
            with open(path, "r") as fp:
                lines = fp.readlines()
                ot_nt_verses = lines[0:num_ot_nt_verses]
                return (
                    sum(map(lambda line: 1 if line.strip() else 0, ot_nt_verses))
                    / num_ot_nt_verses
                    > 0.99
                )

        incomplete_bibles = [
            filename
            for filename in filenames
            if not is_99_percent_complete(self.corpus_path / filename)
        ]

        self._report_possible_failures(
            incomplete_bibles,
            "has at least 99% of the verses corresponding to the OT and NT",
        )

    def test_all_files_have_correct_number_of_lines(self):
        expected_num_lines = 41899

        def has_correct_number_lines(path: Path) -> int:
            with open(path, "rb") as fp:
                num_lines = sum(1 for _ in fp)
                return num_lines == expected_num_lines

        bibles_with_incorrect_num_lines = [
            filename
            for filename in os.listdir(self.corpus_path)
            if not has_correct_number_lines(self.corpus_path / filename)
        ]

        self._report_possible_failures(
            bibles_with_incorrect_num_lines,
            f"has expected number of lines ({expected_num_lines})",
        )

    def _report_possible_failures(self, failures: List, condition: str) -> None:
        if failures:
            self.fail(
                f"{len(failures)} found not matching condition: {condition}.\n"
                + f"{','.join(failures)}"
            )


if __name__ == "__main__":
    unittest.main()
