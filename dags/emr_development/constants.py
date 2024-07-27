import os

BUCKET_NAME = 'panzer-development'

absdir = os.path.dirname(os.path.abspath(__file__))
script_path = os.path.join(absdir, 'scripts')
script_files = [os.path.relpath(os.path.join(script_path, f)) for f in os.listdir(script_path) if os.path.isfile(os.path.join(script_path, f))]

SCRIPTS = [{'s3_script': f.replace('dags/', ''), 'local_script': f} for f in script_files]
