# Core packages
import os
import grp
import pwd
import shutil
import subprocess
import tarfile
from hashlib import md5

# Third-party packages
from charmhelpers.core.hookenv import (
    log,
    resource_get,
    status_set
)
from charms.reactive import hook, set_state
import yaml


# Read layer config, with defaults
if not os.path.exists('untar-resources.yaml'):
    raise Exception('untar-resources.yaml not found')

with open('untar-resources.yaml') as layer_config_yaml:
    layer_config = yaml.safe_load(layer_config_yaml.read())


def _create_user(username, groupname):
    # Get or create user
    try:
        user_id = pwd.getpwnam(username).pw_uid
    except KeyError:
        subprocess.check_call(['useradd', username])
        user_id = pwd.getpwnam(username).pw_uid

    # Get or create group
    try:
        group_id = grp.getgrnam(groupname).gr_gid
    except KeyError:
        subprocess.check_call(['groupadd', groupname])
        group_id = grp.getgrnam(groupname).gr_gid

    return (user_id, group_id)


def _chown_recursive(path, username, groupname):
    user_id, group_id = _create_user(username, groupname)

    for root, dirs, files in os.walk(path):
        for momo in files + dirs:
            os.chown(os.path.join(root, momo), user_id, group_id)

    os.chown(path, user_id, group_id)


def _log(message):
    log('[untar-resources] {message}'.format(**locals()))


@hook('config-changed')
def update():
    resources_config = layer_config['resources']

    for resource_name, resource_config in resources_config.items():
        destination_path = resource_config['destination_path']
        username = resource_config['username']

        resource_path = resource_get(resource_name)

        if not resource_path:
            status_set(
                'blocked',
                '[untar-resources] Resource "{}" missing'.format(resource_name)
            )
            return

        status_set(
            'maintenance',
            '[untar-resources] Extracting resource "{}"'.format(resource_name)
        )

        target_path = destination_path.rstrip('/')
        next_path = destination_path + '.next'
        hash_path = destination_path + '.hash'
        previous_path = destination_path + '.previous'

        _log('Reading hash of {resource_path}'.format(**locals()))
        resource_hash = md5()
        with open(resource_path, "rb") as resource:
            # Load file half a MP at a time
            for chunk in iter(lambda: resource.read(524288), b""):
                resource_hash.update(chunk)
        resource_hex = resource_hash.hexdigest()

        existing_hex = None
        if os.path.exists(hash_path):
            with open(hash_path) as hash_file:
                existing_hex = hash_file.read()

        if resource_hex == existing_hex:
            _log((
                '{resource_name} hash {resource_hex} already extracted'
            ).format(**locals()))

            set_state('resources.{}.available'.format(resource_name))

            return
        else:
            _log((
                'Extracting {resource_name} with hash: {resource_hex}'
            ).format(**locals()))

        _log('Creating {next_path}'.format(**locals()))
        os.makedirs(next_path, exist_ok=True)

        _log('Extracting {resource_path} into {next_path}'.format(**locals()))
        tar = tarfile.open(resource_path)
        tar.extractall(next_path)
        tar.close()

        _log('Setting ownership to {username}'.format(**locals()))
        _chown_recursive(next_path, username, username)

        # Remove previous version
        if os.path.isdir(previous_path):
            _log('Removing previous version from {previous_path}'.format(
                **locals()
            ))
            shutil.rmtree(previous_path)

        _log((
                'Installing new version: Moving {next_path} -> {target_path} '
                'and {target_path} -> {previous_path}'
        ).format(**locals()))

        subprocess.check_call(
            [
                "mv",
                "--no-target-directory", "--backup", "--suffix", ".previous",
                next_path,
                target_path
            ]
        )

        _log('Setting {hash_path} to {resource_hex}'.format(**locals()))
        with open(hash_path, 'w') as hash_file:
            hash_file.write(resource_hex)

        set_state('resources.{}.available'.format(resource_name))
