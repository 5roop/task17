# task17
BA TEI

# First impressions

The structure of the files is different than RS and HR parliament. The bicameralism will have to be encoded carefully.

MP table is riddled with missing data (14% of `codemp` is NaN).

Parties have also the attribute `entity` and `ethnic affiliation`.

I found official webpage: https://www.parlament.ba/ and the parlametar/otvoreni parlament  alternative: www.javnarasprava.ba .

21 parties from MP table are not present in partiesdf. 

Two parties share one abbreviation.

Sentence splitting is done with `HR` (Note from 2022-11-14T10:49:21: Nikola says this is OK.)

Notable transcriber notes:
* `Hoćemo li ponoviti? ____________(?) /nije uključen mikrofon/`
* ` /PAUZA/`