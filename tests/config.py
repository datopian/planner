import os

import yaml

curdir = os.path.dirname(__file__)
specs_dir = os.path.join(curdir, 'specs')


def load_spec(spec):
    return yaml.load(open(os.path.join(specs_dir, spec+'.yaml')))

# Different specs
SPECS = [
    load_spec('empty'),
    load_spec('local'),
]

# Different configs
CONFIGS = [
    dict(allowed_types=['derived/json']),
    dict(allowed_types=['derived/csv'])
]