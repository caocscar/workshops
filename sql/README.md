# Introduction to SQL
Here is the [workshop slide deck](http://nbviewer.jupyter.org/format/slides/github/caocscar/workshops/blob/master/sql/SQLslides.ipynb#/)

## Converting Jupyter Notebook into Slide Deck
The following command will render your Jupyter Notebook into a **reveal.js** slide deck. 

`jupyter nbconvert SQLslides.ipynb --to slides --post serve`

The `--post serve` command starts up a local server to host it. 

**Tip**: Make sure your Jupyter notebook is closed before running the command.

### Reference
More options available at https://nbconvert.readthedocs.io/en/latest/config_options.html

## Posting your Slide Deck online
1. Go to http://nbviewer.jupyter.org
2. Enter url where the Jupyter Notebook file can be located.
3. Make sure **nbviewer** is in *slide mode* and not *notebook mode* among the icons in the top right.
