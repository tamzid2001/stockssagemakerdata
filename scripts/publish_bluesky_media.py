#!/usr/bin/env python3
"""Preview or explicitly publish approved text/image posts to Quantura Bluesky.
Credentials come from the environment or Google Secret Manager; never a manifest.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def secret(name, project):
    value = os.environ.get(name)
    if not value:
        value = subprocess.check_output(['gcloud', 'secrets', 'versions', 'access', 'latest', '--secret', name, '--project', project], stderr=subprocess.DEVNULL).decode().strip()
    return value


def request(url, data=None, token='', content_type='application/json'):
    headers = {'Content-Type': content_type}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    body = data if isinstance(data, bytes) else json.dumps(data).encode() if data is not None else None
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=body, headers=headers), timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        # Provider errors may include credentials in echoed request bodies.
        raise RuntimeError(f'Bluesky request failed: HTTP {error.code}') from None


def tid():
    value = int(time.time() * 1_000_000) << 10
    alphabet = '234567abcdefghijklmnopqrstuvwxyz'
    return ''.join(alphabet[(value >> shift) & 31] for shift in range(60, -1, -5))


def facets(text):
    return [{'index': {'byteStart': len(text[:m.start()].encode()), 'byteEnd': len(text[:m.end()].encode())}, 'features': [{'$type': 'app.bsky.richtext.facet#link', 'uri': m.group()}]} for m in re.finditer(r'https://[^\s]+', text)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--media-dir', type=Path, required=True)
    parser.add_argument('--ledger', type=Path, required=True)
    parser.add_argument('--project', default='quantura-e2e3d')
    parser.add_argument('--publish', action='store_true', help='Publish the reviewed manifest; preview is the default.')
    args = parser.parse_args()
    posts = json.loads(args.manifest.read_text())
    for post in posts:
        if not post.get('text') or len(post['text']) > 300 or len(post['text'].encode()) > 3000:
            raise ValueError('Post text exceeds the supported preview limit.')
        file = (args.media_dir / post['image']).resolve()
        if not file.is_relative_to(args.media_dir.resolve()) or not file.is_file() or file.stat().st_size > 1_000_000:
            raise ValueError('Choose an existing JPEG/PNG image under 1 MB in the media directory.')
        if file.suffix.lower() not in ['.jpg', '.jpeg', '.png'] or not post.get('alt'):
            raise ValueError('Choose a JPEG/PNG and descriptive alt text.')
    if not args.publish:
        print(json.dumps({'preview': True, 'posts': posts}, indent=2))
        return
    session = request('https://bsky.social/xrpc/com.atproto.server.createSession', {'identifier': secret('BLUESKY_IDENTIFIER', args.project), 'password': secret('BLUESKY_APP_PASSWORD', args.project)})
    if session['handle'] != 'quantura.bsky.social':
        raise ValueError('Authenticated account is not Quantura.')
    service = next(s for s in session['didDoc']['service'] if s['id'].endswith('#atproto_pds'))['serviceEndpoint']
    host = urllib.parse.urlparse(service)
    if host.scheme != 'https' or not (host.hostname.endswith('.bsky.network') or host.hostname == 'bsky.social'):
        raise ValueError('Unexpected Quantura PDS.')
    ledger = json.loads(args.ledger.read_text()) if args.ledger.exists() else {}
    def save():
        args.ledger.parent.mkdir(parents=True, exist_ok=True)
        args.ledger.write_text(json.dumps(ledger, indent=2) + '\n')
    for post in posts:
        file = args.media_dir / post['image']
        identity = hashlib.sha256(post['text'].encode() + file.read_bytes()).hexdigest()
        state = ledger.setdefault(identity, {'rkey': tid(), 'text': post['text'], 'image': post['image']})
        if state.get('url'):
            print(json.dumps({'already_published': state['url']})); continue
        save()  # Retrying a timed-out mutation uses the same repository record.
        query = urllib.parse.urlencode({'repo': session['did'], 'collection': 'app.bsky.feed.post', 'rkey': state['rkey']})
        try:
            existing = request(service + '/xrpc/com.atproto.repo.getRecord?' + query, token=session['accessJwt'])
        except RuntimeError as error:
            if 'HTTP 400' not in str(error) and 'HTTP 404' not in str(error):
                raise
            existing = None
        if not existing:
            blob = request(service + '/xrpc/com.atproto.repo.uploadBlob', file.read_bytes(), session['accessJwt'], 'image/png' if file.suffix.lower() == '.png' else 'image/jpeg')['blob']
            record = {'$type': 'app.bsky.feed.post', 'text': post['text'], 'langs': ['en'], 'createdAt': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'), 'facets': facets(post['text']), 'embed': {'$type': 'app.bsky.embed.images', 'images': [{'alt': post['alt'], 'image': blob, 'aspectRatio': post['aspect_ratio']}]}}
            request(service + '/xrpc/com.atproto.repo.createRecord', {'repo': session['did'], 'collection': 'app.bsky.feed.post', 'rkey': state['rkey'], 'record': record}, session['accessJwt'])
        verified = request(service + '/xrpc/com.atproto.repo.getRecord?' + query, token=session['accessJwt'])
        if verified['value']['text'] != post['text']:
            raise ValueError('Published record did not match the approved post.')
        state.update({'uri': verified['uri'], 'url': 'https://bsky.app/profile/' + session['handle'] + '/post/' + state['rkey'], 'verified': True})
        save(); print(json.dumps({'published': state['url'], 'verified': True}))


if __name__ == '__main__':
    main()
