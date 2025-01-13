import os
import sys
import unittest
from pathlib import Path

path = os.path.realpath(__file__)
dir = os.path.dirname(path)
dir = dir.replace("tests", "code\python")
os.chdir(dir)
sys.path.insert(0, dir)

from settings_file import get_book_names, get_versification, get_vrs_diffs

vrs_diffs = get_vrs_diffs()

dir = dir.replace("code\python", "tests")
os.chdir(dir)


class Test_Get_Versification(unittest.TestCase):

    def setUp(self):
        self.versifications = Path("versifications")

    def test_lxx(self):
        versification = get_versification(
            self.versifications / "eng-lxx2012", vrs_diffs
        )
        self.assertEqual(versification, "Septuagint")


class Test_Misc(unittest.TestCase):

    def test_get_book_names(self):
        book_names_path = Path("book_names")
        aai_books = get_book_names(book_names_path / "aai")
        self.assertTupleEqual(aai_books[0], ("MAT", "46-MATaai.usfm"))
        ahr_books = get_book_names(book_names_path / "ahr-NTAii20-Ahirani-Devanagari")
        self.assertTupleEqual(ahr_books[0], ("MAT", "MAT.usfm"))
        am_books = get_book_names(book_names_path / "am_ulb")
        self.assertTupleEqual(am_books[0], ("GEN", "01-GEN.usfm"))


if __name__ == "__main__":
    unittest.main()
