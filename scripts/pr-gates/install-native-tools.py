#!/usr/bin/env python3
"""Install only immutable, digest-verified native CI contract tools."""
import argparse
import hashlib
import io
import os
from pathlib import Path
import platform
import tarfile
import urllib.request

TOOLS = {
    'kustomize': (
        'https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize/v5.8.3/kustomize_v5.8.3_linux_amd64.tar.gz',
        'cb9e31198d3f63b44848bd0afc4c8efc56e6cfef9c93a4a39a211940e6decd61',
    ),
    'toxiproxy-server': (
        'https://github.com/Shopify/toxiproxy/releases/download/v2.13.0/toxiproxy-server-linux-amd64',
        'bf529e8bb9a05cd293f45a3f1d0aa66ff9469680dcdca7e0ebeae21f1b83de89',
    ),
}


def verified_download(url, expected):
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError('native tool digest mismatch: ' + url)
    return data


def executable(name, data):
    if name != 'kustomize':
        return data
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        members = archive.getmembers()
        if len(members) != 1 or members[0].name != 'kustomize' or not members[0].isfile():
            raise ValueError('unexpected kustomize archive layout')
        return archive.extractfile(members[0]).read()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--dest', type=Path, required=True)
    args = parser.parse_args(argv)
    if os.environ.get('GITHUB_ACTIONS') != 'true' or platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise ValueError('native runtime tools are installed only in authorized Linux x86_64 hosted CI')
    args.dest.mkdir(parents=True, exist_ok=False)
    for name, (url, expected) in TOOLS.items():
        data = verified_download(url, expected)
        output = args.dest/name
        with output.open('xb') as file:
            file.write(executable(name, data))
        output.chmod(0o755)
        print(name + ' artifact_sha256=' + expected + ' url=' + url, flush=True)


if __name__ == '__main__':
    main()
