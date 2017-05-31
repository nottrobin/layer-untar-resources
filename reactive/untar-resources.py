# Core packages
import os
import grp
import pwd
import shutil
import subprocess
import tarfile

# Third-party packages
from charmhelpers.core.hookenv import log, resource_get, status_set
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
        previous_path = destination_path + '.previous'

        # Stop if the archive is older than the currently deployed version
        if os.path.exists(target_path):
            current_mtime = os.path.getmtime(target_path)
            archive_mtime = os.path.getmtime(resource_path)

            if current_mtime > archive_mtime:
                log(
                    '[untar-resources] Already at most recent version. '
                    'Stopping'
                )

                set_state('resources.{}.available'.format(resource_name))

                return

        log(
            '[untar-resources] Creating {next_path}'.format(
                **locals()
            )
        )
        os.makedirs(next_path, exist_ok=True)

        log(
            (
                '[untar-resources] Extracting '
                '{resource_path} into {next_path}'
            ).format(**locals())
        )
        tar = tarfile.open(resource_path)
        tar.extractall(next_path)
        tar.close()

        log('[untar-resources] Setting ownership to {} '.format(username))
        _chown_recursive(next_path, username, username)

        # Remove previous version
        if os.path.isdir(previous_path):
            log(
                (
                    '[untar-resources] Removing previous version from'
                    '{previous_path}'
                ).format(**locals())
            )
            shutil.rmtree(previous_path)

        log(
            (
                '[untar-resources] Installing new version: Moving '
                '{next_path} -> {target_path} and '
                '{target_path} -> {previous_path}'
            ).format(**locals())
        )
        subprocess.check_call(
            [
                "mv",
                "--no-target-directory", "--backup", "--suffix", ".previous",
                next_path,
                target_path
            ]
        )

        set_state('resources.{}.available'.format(resource_name))
