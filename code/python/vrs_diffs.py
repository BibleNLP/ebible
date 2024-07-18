import os

import yaml
from machine.scripture.canon import ALL_BOOK_IDS

test_books = ALL_BOOK_IDS[:66]


def get_line(file) -> str:
    """
    gets the next line in a file
    param file: the opened file
    return: the next line
    """
    while True:
        line = file.readline()
        name = line[:3]
        line = line.strip()
        if name[:2] == "##":
            line = line[2:]
        if name in test_books and not "=" in line:
            if name[:2] == "##":
                line = line[2:]
            return line


def get_data(file) -> tuple[str, list[str]]:
    """
    gets the book and chapters of the next line in a file
    param file: the opened file
    return: the book and a list of the chapters
    """
    line = get_line(file)
    book = line[:3]
    chapters = line[4:].split(" ")
    return book, chapters


def compare_data(
    file, holder: list[str | list[str]], curr_book: str
) -> tuple[list[str], list[str | list[str]]]:
    """
    compares the next line in the file to the current book
    param file: the opened file
    param holder: a list containing a book and the chapters
    param curr_book: the current book
    return: a list of the chapters
    """
    chapters = ""
    if not holder:
        book, chapters = get_data(file)
        if book != curr_book:
            holder = [book, chapters]
            chapters = ""
    elif holder[0] == curr_book:
        book, chapters = holder
        holder = []
    return chapters, holder


def add_versification(
    versifications: dict[str, list[str]], versification: str, chapters: list[str]
) -> dict[str, list[str]]:
    """
    adds the versification to the list if appropriate
    param versifications: the versifications with their chapters
    param versification: the versification to consider
    param chapters: the list of chapters to consider
    return: the updated versifications
    """
    if chapters:
        versifications[versification] = chapters
    return versifications


def get_book(
    file,
    holder: list[str | list[str]],
    book: str,
    versification: str,
    versifications: dict[str, list[str]],
) -> tuple[list[str | list[str]], dict[str, list[str]]]:
    """
    gets all the chapters of a book and adds the versification to the list if appropriate
    param file: the opened file
    param holder: a list containing a book and the chapters
    param book: the current book to consider
    param versification: the current versification to consider
    param versifications: the list of versifications
    return: a list containing a book and the chapters and the list of versifications
    """
    chapters, holder = compare_data(file, holder, book)
    versifications = add_versification(versifications, versification, chapters)
    return holder, versifications


def get_vrs_diffs() -> dict[str, dict[int, dict[int, list[str]]]]:
    """
    gets the differences in versifications
    return: the differnces in the versifications
    """
    books = {}
    with open("eng.vrs", "r") as eng, open("lxx.vrs", "r") as lxx, open(
        "org.vrs", "r"
    ) as org, open("rsc.vrs", "r") as rsc, open("rso.vrs", "r") as rso, open(
        "vul.vrs", "r"
    ) as vul:
        eng_holder = []
        lxx_holder = []
        org_holder = []
        rsc_holder = []
        rso_holder = []
        vul_holder = []
        for book in test_books:
            versifications = {}
            eng_holder, versifications = get_book(
                eng, eng_holder, book, "English", versifications
            )
            org_holder, versifications = get_book(
                org, org_holder, book, "Original", versifications
            )
            rsc_holder, versifications = get_book(
                rsc, rsc_holder, book, "Russian Protestant", versifications
            )
            rso_holder, versifications = get_book(
                rso, rso_holder, book, "Russian Orthodox", versifications
            )
            lxx_holder, versifications = get_book(
                lxx, lxx_holder, book, "Septuagint", versifications
            )
            vul_holder, versifications = get_book(
                vul, vul_holder, book, "Vulgate", versifications
            )
            largest = 0
            for chapters in versifications.values():
                num_chptrs = len(chapters)
                if num_chptrs > largest:
                    largest = num_chptrs
            chptr_difs = {}
            last_chapters = {}
            versif_list = [
                "English",
                "Original",
                "Russian Protestant",
                "Russian Orthodox",
                "Septuagint",
                "Vulgate",
            ]
            last_chapter = False
            for i in range(0, largest):
                prev = 0
                dif = False
                verse_counts = {}
                for versification, chapters in versifications.items():
                    try:
                        last_verse = chapters[i].split(":")[1]
                    except:
                        pass
                    if prev != 0 and last_verse != prev:
                        dif = True
                        break
                    else:
                        prev = last_verse
                if dif:
                    for versification, chapters in versifications.items():
                        try:
                            parts = chapters[i].split(":")
                            chapter = int(parts[0])
                            last_verse = int(parts[1])
                            try:
                                verse_counts[last_verse].append(versification)
                            except:
                                verse_counts[last_verse] = [versification]
                        except:
                            if versification in versif_list:
                                try:
                                    last_chapters[i].append(versification)
                                except:
                                    last_chapters[i] = [versification]
                                versif_list.remove(versification)
                            last_chapter = True
                        if (
                            last_chapter
                            and i == largest - 1
                            and versification in versif_list
                        ):
                            try:
                                last_chapters[largest].append(versification)
                            except:
                                last_chapters[largest] = [versification]
                            versif_list.remove(versification)
                    if verse_counts and len(verse_counts.keys()) > 1:
                        chptr_difs[chapter] = verse_counts
                    if last_chapters:
                        chptr_difs["last_chapter"] = last_chapters
            if chptr_difs:
                books[book] = chptr_difs
    return books


def main() -> None:
    """
    writes the vrs_difs.yaml file
    """
    path = os.path.realpath(__file__)
    dir = os.path.dirname(path)
    dir = dir.replace("code\python", "metadata")
    os.chdir(dir)
    vrs_difs = get_vrs_diffs()
    vrs_yaml = yaml.safe_load(vrs_difs.__str__())
    dir = dir.replace("metadata", "code\python")
    os.chdir(dir)
    with open("vrs_diffs.yaml", "w") as file:
        yaml.safe_dump(vrs_yaml, file)


if __name__ == "__main__":
    main()
