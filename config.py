# -*- coding: utf-8 -*-
"""config loader and updater for shazbuckbot"""

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

CONFIG_VERSION = 1


def load_config() -> CommentedMap:
    yaml = YAML()
    config: CommentedMap = yaml.load(open('config.yml'))
    config_changed = False

    if 'config_version' not in config:
        config['config_version'] = 0
        config_changed = True

    if config['config_version'] < 1:
        # convert default bet window value
        if 'bet_window' in config:
            position = tuple(config.keys()).index('bet_window')
            config.insert(position, 'default_bet_window', f"{config['bet_window']}m")
            del config['bet_window']
        else:
            config['default_bet_window'] = '10m'
        config['config_version'] = 1
        config_changed = True

    if config_changed:
        yaml.dump(config, open('config.yml', 'w'))

    return config
