#!/usr/bin/env python3
"""Download artifacts for a workflow run given a GitHub Actions event JSON.

This script reads the event file produced by GitHub Actions (usually
available at $GITHUB_EVENT_PATH), extracts the triggering `workflow_run.id`,
lists artifacts for that run and downloads & extracts them into `--out`.

Usage:
  python tools/download_artifacts.py --event-path .github/event.json --repo owner/repo --token $GITHUB_TOKEN --out downloads

If args are not provided, the script reads `GITHUB_EVENT_PATH`,
`GITHUB_REPOSITORY` and `GITHUB_TOKEN` from the environment.
"""
import os
import sys
import json
import argparse
import requests
import io
import zipfile


def download_and_extract_artifact(repo, token, artifact_id, out_dir):
    url = f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}/zip"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(out_dir)


def main():
    p = argparse.ArgumentParser(description='Download artifacts from a workflow run')
    p.add_argument('--event-path', default=os.getenv('GITHUB_EVENT_PATH'), help='Path to the GitHub event JSON')
    p.add_argument('--repo', default=os.getenv('GITHUB_REPOSITORY'), help='owner/repo')
    p.add_argument('--token', default=os.getenv('GITHUB_TOKEN'), help='GitHub token with repo access')
    p.add_argument('--out', default='downloads', help='Output directory to extract artifacts into')
    args = p.parse_args()

    if not args.event_path or not os.path.exists(args.event_path):
        print('Missing or invalid --event-path and GITHUB_EVENT_PATH; aborting', file=sys.stderr)
        return 2
    if not args.repo:
        print('Missing --repo and GITHUB_REPOSITORY; aborting', file=sys.stderr)
        return 2
    if not args.token:
        print('Missing --token and GITHUB_TOKEN; aborting', file=sys.stderr)
        return 2

    with open(args.event_path, 'r', encoding='utf-8') as f:
        evt = json.load(f)

    run_id = evt.get('workflow_run', {}).get('id')
    if not run_id:
        print('No workflow_run.id found in event payload', file=sys.stderr)
        return 3

    artifacts_url = f"https://api.github.com/repos/{args.repo}/actions/runs/{run_id}/artifacts"
    headers = {"Authorization": f"token {args.token}", "Accept": "application/vnd.github+json"}
    resp = requests.get(artifacts_url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    os.makedirs(args.out, exist_ok=True)
    artifacts = data.get('artifacts', [])
    if not artifacts:
        print('No artifacts found for run', run_id)
        return 0

    for art in artifacts:
        aid = art.get('id')
        name = art.get('name')
        print(f"Downloading artifact {name} (id={aid})")
        try:
            # extract into a subdir named after the artifact to avoid collisions
            dest = os.path.join(args.out, name)
            os.makedirs(dest, exist_ok=True)
            download_and_extract_artifact(args.repo, args.token, aid, dest)
        except Exception as e:
            print(f"Failed to download/extract artifact {name}: {e}", file=sys.stderr)
            return 4

    print('Artifacts downloaded to', args.out)
    return 0


if __name__ == '__main__':
    sys.exit(main())
