import os
import shutil
import sys
import unittest
from os import listdir, remove
from pathlib import Path

from bs4 import BeautifulSoup

path = os.path.realpath(__file__)
dir = os.path.dirname(path)
dir = dir.replace("tests", "code\python")
os.chdir(dir)
sys.path.insert(0, dir)

from ebible import (create_folders, create_license_entry, download_bible,
                    download_usfm, download_usfx, get_bibles, get_book_names,
                    get_metadata, get_versification, get_vrs_diffs)

dir = dir.replace("code\python", "")
os.chdir(dir)


class Test_Downloads(unittest.TestCase):

    def setUp(self):
        self.temp = Path("tests/temp")
        self.temp.mkdir(parents=True, exist_ok=True)
        self.catalog, self.public_bibles, self.private_bibles = get_bibles(
            Path("metadata")
        )

    def test_download_usx(self):
        download_usfx(self.public_bibles[0], self.temp)
        self.assertTrue(os.path.isdir(self.temp / "aai_usfx"))
        self.assertTrue(os.path.isfile(self.temp / "aai_usfx.zip"))
        download_usfx(self.private_bibles[0], self.temp)
        self.assertTrue(os.path.isdir(self.temp / "aai_usfx"))
        self.assertTrue(os.path.isfile(self.temp / "aai_usfx.zip"))

    def test_download_usfm(self):
        log = []
        test_bible = self.public_bibles[0]
        log = download_usfm(test_bible, log, self.temp, self.temp)
        self.assertTrue(os.path.isdir(self.temp / test_bible))
        self.assertTrue(os.path.isfile(self.temp / f"{test_bible}.zip"))
        test_bible = self.private_bibles[0]
        log = download_usfm(test_bible, log, self.temp, self.temp)
        self.assertTrue(os.path.isdir(self.temp / test_bible))
        self.assertTrue(os.path.isfile(self.temp / f"{test_bible}.zip"))

    def tearDown(self) -> None:
        shutil.rmtree(self.temp)


class Test_Metadata(unittest.TestCase):

    def setUp(self):
        self.base = Path("tests")
        with open(
            self.base / "metadata" / "aai_usfx" / "aaimetadata.xml",
            "r",
            encoding="utf-8",
        ) as in_file:
            file = in_file.read()
            self.soup = BeautifulSoup(file, "lxml")

    def test_get_metadata(self):
        self.assertEqual(get_metadata("aai", self.base / "metadata"), self.soup)

    def test_download_bible(self):
        bible_path = self.base / "download_test" / "aai"
        download_path = self.base / "download_bible_True"
        bible_download_path = download_path / "aai"
        not_download_path = self.base / "download_bible_False"
        download_path.mkdir(parents=True, exist_ok=True)
        self.assertTrue(download_bible("aai", self.soup, download_path))
        shutil.copytree(bible_path, bible_download_path)
        remove(bible_download_path / "46-MATaai.usfm")
        self.assertTrue(download_bible("aai", self.soup, download_path))
        self.assertEqual(len(listdir(download_path)), 1)
        shutil.rmtree(download_path)
        shutil.copytree(bible_path, not_download_path / "aai")
        self.assertFalse(download_bible("aai", self.soup, not_download_path))
        shutil.rmtree(not_download_path)

    def test_create_license_entry(self):
        licenses = []
        licenses = create_license_entry(
            "aai", self.soup, licenses, self.base / "license_data"
        )
        test_license = licenses[0]
        self.assertEqual(test_license["ID"], "aai")
        self.assertEqual(test_license["Scope"], "NT")
        self.assertEqual(test_license["Script"], "Latin")
        self.assertEqual(test_license["Name"], "Miniafia")
        self.assertEqual(test_license["License Type"], "by-nc-nd")
        self.assertEqual(test_license["License Version"], "4.0")
        self.assertEqual(
            test_license["License Link"],
            "http://creativecommons.org/licenses/by-nc-nd/4.0/",
        )
        self.assertEqual(test_license["Copyright"], "Copyright © 2009.")


class Test_Get_Versification(unittest.TestCase):

    def setUp(self):
        self.vrs_difs = get_vrs_diffs()
        self.versifications = Path("tests/versifications")

    def test_guess_eng(self):
        versification, versifications = get_versification(
            "ahr-NTAii20-Ahirani-Devanagari",
            self.vrs_difs,
            self.versifications,
        )
        self.assertEqual(versification, "English")
        self.assertListEqual(
            versifications,
            [
                "English",
                "Original",
                "Russian Protestant",
                "Russian Orthodox",
                "Septuagint",
            ],
        )

    def test_eng(self):
        versification, versifications = get_versification(
            "pes-OPCB-Persian, Iranian-Arabic",
            self.vrs_difs,
            self.versifications,
        )
        self.assertEqual(versification, "English")
        self.assertListEqual(versifications, ["English"])

    def test_guess_rsc(self):
        versification, versifications = get_versification(
            "kca-KhPB-Khanty-Cyrillic", self.vrs_difs, self.versifications
        )
        self.assertEqual(versification, "Russian Protestant")
        self.assertListEqual(versifications, ["Russian Protestant", "Russian Orthodox"])

    def test_rsc(self):
        versification, versifications = get_versification(
            "bel-BBL-Belarusian-Cyrillic", self.vrs_difs, self.versifications
        )
        self.assertEqual(versification, "Russian Protestant")
        self.assertListEqual(versifications, ["Russian Protestant"])

    def test_guess_org(self):
        versification, versifications = get_versification(
            "fuf-PFJV-Pular-Latin", self.vrs_difs, self.versifications
        )
        self.assertEqual(versification, "Original")
        self.assertListEqual(versifications, ["Original", "Septuagint"])

    def test_org(self):
        versification, versifications = get_versification(
            "deu-ELBBK-German, Standard-Latin",
            self.vrs_difs,
            self.versifications,
        )
        self.assertEqual(versification, "Original")
        self.assertListEqual(versifications, ["Original"])

    def test_vul(self):
        versification, versifications = get_versification(
            "heb-OHD-Hebrew, Modern-Hebrew",
            self.vrs_difs,
            self.versifications,
        )
        self.assertEqual(versification, "Vulgate")
        self.assertListEqual(versifications, ["Vulgate"])

    def test_lxx(self):
        versification, versifications = get_versification(
            "eng-lxx2012", self.vrs_difs, self.versifications
        )
        self.assertEqual(versification, "Septuagint")
        self.assertListEqual(versifications, ["Septuagint"])


class Test_Misc(unittest.TestCase):

    def test_create_folders(self):
        (
            base,
            downloads,
            projects,
            private_projects,
            temp,
            logs,
            metadata_folder,
            corpus,
            private_corpus,
            public_extract_log,
            private_extract_log,
        ) = create_folders("test")
        for folder in [
            base,
            downloads,
            projects,
            private_projects,
            temp,
            logs,
            metadata_folder,
            corpus,
            private_corpus,
            public_extract_log,
            private_extract_log,
        ]:
            self.assertTrue(os.path.isdir(folder))
        shutil.rmtree(base)

    def test_get_book_names(self):
        book_names_path = Path("tests/book_names")
        aai_books = get_book_names("aai", True, book_names_path)
        self.assertTupleEqual(aai_books[0], ("MAT", "46-MATaai.usfm"))
        ahr_books = get_book_names(
            "ahr-NTAii20-Ahirani-Devanagari", True, book_names_path
        )
        self.assertTupleEqual(ahr_books[0], ("MAT", "MAT.usfm"))
        am_books = get_book_names("am_ulb", True, book_names_path)
        self.assertTupleEqual(am_books[0], ("GEN", "01-GEN.usfm"))


if __name__ == "__main__":
    unittest.main()
