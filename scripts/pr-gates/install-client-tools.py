#!/usr/bin/env python3
"""Install the fixed TypeScript compiler in an owned hosted-CI directory."""
import argparse
import base64
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import urllib.request

URL='https://registry.npmjs.org/typescript/-/typescript-5.9.3.tgz'
INTEGRITY='jl1vZzPDinLr9eUt3J/t7V6FgNEw9QjvBPdysz9KfQDD41fQrC2Y4vKQdiaUpFT4bXlb1RHhLpp8wtm6M5TgSw=='


def verified(data):
    if base64.b64encode(hashlib.sha512(data).digest()).decode()!=INTEGRITY:
        raise ValueError('TypeScript compiler integrity mismatch')
    return data


def unpack(data,destination):
    with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz')as archive:
        members=archive.getmembers();names=set()
        for member in members:
            path=PurePosixPath(member.name)
            if (path.is_absolute() or '..'in path.parts or '\\'in member.name
                    or path.parts[0]!='package' or member.name in names
                    or not(member.isfile()or member.isdir())):
                raise ValueError('unsafe TypeScript compiler archive')
            names.add(member.name)
        if not {'package/bin/tsc','package/lib/_tsc.js','package/package.json'}<=names:
            raise ValueError('incomplete TypeScript compiler archive')
        for member in members:
            path=destination.joinpath(*PurePosixPath(member.name).parts[1:])
            if member.isdir():path.mkdir(parents=True,exist_ok=True)
            else:
                path.parent.mkdir(parents=True,exist_ok=True)
                with path.open('xb')as output:output.write(archive.extractfile(member).read())


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    parser.add_argument('--dest',type=Path,required=True);args=parser.parse_args(argv)
    if os.environ.get('GITHUB_ACTIONS')!='true':raise ValueError('client runtime tools install only in authorized hosted CI')
    subprocess.run(['node','--version'],check=True)
    args.dest.mkdir(parents=True,exist_ok=False)
    with urllib.request.urlopen(URL,timeout=60)as response:data=verified(response.read())
    unpack(data,args.dest/'typescript')
    wrapper=args.dest/'tsc'
    wrapper.write_text('#!/bin/sh\nexec node "$(dirname "$0")/typescript/bin/tsc" "$@"\n')
    wrapper.chmod(0o755)
    result=subprocess.run([str(wrapper),'--version'],check=True,text=True,capture_output=True)
    if result.stdout.strip()!='Version 5.9.3':raise ValueError('unexpected TypeScript compiler identity')
    print('typescript=5.9.3 integrity=sha512-'+INTEGRITY+' source='+URL,flush=True)


if __name__=='__main__':main()
