import yaml
import os
CONFIG = {}

def load_config(env: str = "local"):
	global CONFIG
	with open('config.yaml', 'r') as f:
		CONFIG.update(yaml.safe_load(f).get(env))

env = os.getenv('APP_ENV', 'stage')
load_config(env)