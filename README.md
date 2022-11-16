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
* `/INTONIRANJE HIMNE/`

## 2022-11-14T14:46:25

The dates of terms are not easily accessible. I again opted for extracting min-max dates from the metadata I have in its stead.


## 2022-11-15T15:13:17

It seems there are a plethora of people in the data which I do not know anything (i.e., they are not in MP file.) As with HR I will add them manually with no additional information.

There are also two non-human names; `#External` and `#Protocol`. These are present from the original data forward. I think I will just inhibit this problem on the TEI component level with sed, sth like

```shell
sed -i 's/<u who="#Protocol"/<u/g' $file
```

For the speakers, presumed to be human (e.g. DžonsonRasel(lord)) I will preserve the names given in the metadata, meaning that I will have to input them in the root TEI.

I found that for Term 8 I simply do not have MPs with assigned codemps. I will impute this as best I can, and with irritated humours.

## 2022-11-15T18:36:16

Upon further investigation it was discovered that not only `#External` and `#Protocol` are missing. Newly found problematic references are also `#Predstavnik`, `#Podnositeljizvješća`, `#Poslanice`, `#Poslanici`, `#Prijedlogrezolucije`, `#Ministarstvo`, `#Predstavnikministarstva` `#Delegati`. There is also a two people portmanteau: `#LučićMiloš;JovićNedeljko` and even `#OlsunBajmarŠerifMubarek` [sic! Nta `Bajmar`]...






