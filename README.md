# manuscripts-tracker

manuscripts-tracker is a set of functions that collect submissions from multiple preprints platforms online. Those include: arXiv, bioRxiv, medRxiv, EarthArXiv, SocArXiv, PsyArXiv, NBER, Preprints.org and F1000Research. Those utilities will also infer the gender of the authors and identify those that are covid related.

# Installation
The only step required to use the code in the repository is to install Firefox on your computer if you don't have it, and download the geckdriver (https://github.com/mozilla/geckodriver/releases) in the tools/ directory.

# Usage
The first time you launch the code, you need to launch it with the `init` option, and you need to pass it a date in the format YYYY-mm-dd where it will start to collect the manuscripts metadata.

```bash
python main_gender.py init 2019-01-01
```

After that, you can either update the files by running the script with the `all` mode sporadically, or with the `periodic` mode to have the script update the data automatically every 24 hours. The script will only collect the data for the days since the last collected data for each repository.

```bash
python main_gender.py all
```

