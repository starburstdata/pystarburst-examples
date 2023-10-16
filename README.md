# PyStarburst DataFrame API examples

This repository contains samples for using [PyStarburst](https://docs.starburst.io/starburst-galaxy/python/pystarburst.html). In order to use them,
just sign up for a free Galaxy account (if you don't have one already) and try
out the notebooks.

## Notebooks

The easiest way to use the notebooks is to start a cloud notebook environment by clicking
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/starburstdata/pystarburst-examples/HEAD).

## Apps

You can also run the apps locally. To do so, you need to install the dependencies first. Testing has been done with Python 3.10, 3.11 and on MacOS and Linux.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Then, you can run the apps:
```bash
python app.py
```