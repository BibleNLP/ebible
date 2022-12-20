# eBible
Curated corpus of parallel data derived from versions of the Bible provided by eBible.org.

## Copyright Restrictions

The Bibles in this repository have been collected from eBible.org. To the best of our ability, we have ensured the resources in this repository are releasable in this form, either belonging to the Public Domain, with a permissive Creative Commons license, or with permission of the rights holder. Subsequent use of this material is subject to the license terms of the original source, found in the [Copyright and License information file](https://github.com/BibleNLP/bible-parallel-corpus/blob/corpus-addition/metadata/Copyright%20and%20license%20information.xlsx). If you believe we are in error in our determination, we will respond to requests via pull request or e-mail from the copyright holders to takedown the offending material.

## Data Format

The Bibles in this repository contain **only** the verse text.  All other text (introductory paragraphs, comments, footnotes, etc) have been removed.

Each Bible is presented in a verse-per-line format.  The sequence of the verses in each Bible is aligned to a canonical list of Biblical books, chapters, and verses.  This canonical verse list is based on the Original versification, and the verse reference list can be found in the file _vref.txt_.  The verse reference and verse text for a Bible can be correlated by iterating in parallel through the lines of the _vref.txt_ file and the lines of a Bible.

USFM files are download from [eBible](https://ebible.org/), one zip file per Bible. We use bulk_extract_corpora.py from [SIL's NLP repo](https://github.com/sillsdev/silnlp/tree/master/silnlp/common/) to extract the verse text into the one verse per line format. 

### File Naming Convention

The extracted verse text from each Paratext resource is stored in a separate file.  The file naming convention follows the format:

  - \<languageCode\>-\<ParatextProject\>.txt (e.g., 'en-KJV.txt')

where:

  - \<languageCode\> is the 2- or 3-character ISO-639 abbreviation of the language of the verse text in the extract file;
  - \<ParatextProject\> is the Paratext project name from which the verse text was extracted. 

### Verse References

Verse references in the _vref.txt_ file follow the format:

  - \<book\> \<chapter\>:\<verse\> (e.g., 'GEN 1:1')

where:

  - \<book\> is the 3 letter book abbreviation ([per USFM 3.0](https://ubsicap.github.io/usfm/identification/books.html));
  - \<chapter\> is the numeric chapter number;
  - \<verse\> is the numeric verse number.

### Missing Verses

Blank lines in the Bible text file indicate that the verse was not part of the source Bible.
  
### Verse Ranges
 
If a source Bible contained a verse range, with the text of multiple verses grouped together, then all of the verse text from the verse range will be found in the Bible text file on the line corresponding to the first verse in the verse range.  For each additional verse in the verse range, the token '&lt;range&gt;' will be found on the corresponding line of the Bible text file.  For example, if a source Bible contained Gen. 1:1-3 as a verse range, then the first 3 lines of its Bible text file will appear as follows:

    ...verse range text...
    <range>
    <range>
