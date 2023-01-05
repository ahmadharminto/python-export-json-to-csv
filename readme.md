# [Experiment] Export data from API (JSON) to CSV using Python (3.10.5)

## How to
- copy .env.example to .env, fill the missing value
- create google service account (json) with `drive` and `sheets` API enabled, named as `google-service.json`, put on root directory
- create venv : `python -m venv .venv`
- activate venv : `source .venv/bin/activate`
- install dependencies : `pip install -r requirements.txt`
- execute : `python export.py`
- help : `python export.py --help`