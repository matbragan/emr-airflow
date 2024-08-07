from emr_development.utils import get_project_name

BUCKET_NAME = 'unudutri-development'
PROJECT_NAME = get_project_name()
SCRIPT_TYPES = ['bootstrap', 'steps']
BOOTSTRAP_DIR = 'bootstrap'
STEPS_DIR = 'steps'