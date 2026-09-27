"""Small REST transport for the opt-in compatibility experiment (stdlib only)."""
import json
import os
from pathlib import Path
import re
import shlex
import urllib.error
import urllib.parse
import urllib.request

MAX_REQUEST = 4 * 1024 * 1024  # Below the published 6 MB write limit.
MAX_RESPONSE = 32 * 1024 * 1024


class ProbeError(Exception):
    """Messages must never contain server bodies, credentials, or signed URLs."""


class HttpFailure(ProbeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f'HTTP {status}')


def require(condition, message):
    if not condition:
        raise ProbeError(message)


def load_settings(path):
    names = ('LAMBDADB_BASE_URL', 'LAMBDADB_PROJECT_NAME', 'LAMBDADB_PROJECT_API_KEY')
    values = {}
    if path is not None:
        try:
            lines = Path(path).read_text().splitlines()
        except OSError:
            raise ProbeError('Cannot read environment file') from None
        for line in lines:
            line = line.strip().removeprefix('export ')
            if not line or line.startswith('#'):
                continue
            key, sep, raw = line.partition('=')
            if key.strip() not in names:
                continue
            require(bool(sep), 'Invalid environment assignment')
            try:
                parts = shlex.split(raw, comments=True)
            except ValueError:
                raise ProbeError('Invalid environment quoting') from None
            require(len(parts) == 1, 'Environment values must be nonempty, quoted if needed')
            require(key.strip() not in values, 'Duplicate environment assignment')
            values[key.strip()] = parts[0]
    # An explicitly supplied file is authoritative; no accidental mixing of projects.
    if path is None:
        values = {key: os.environ.get(key, '') for key in names}
    require(all(values.get(key) for key in names), 'Missing LambdaDB connection settings')
    parsed = urllib.parse.urlsplit(values[names[0]])
    require(parsed.scheme == 'https' and parsed.netloc and not parsed.username
            and not parsed.password and not parsed.query and not parsed.fragment,
            'Base URL must be HTTPS without userinfo, query, or fragment')
    require(not parsed.path.strip('/'), 'Base URL must be an origin; project is supplied separately')
    require(bool(re.fullmatch(r'[A-Za-z0-9_-]+', values[names[1]])), 'Invalid project name')
    return values


def encoded(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def batches(items, field='docs', limit=MAX_REQUEST):
    """Preserve order; split only at the byte budget, never one request per row."""
    batch = []
    for item in items:
        if len(encoded({field: batch + [item], 'branch': 'main'})) > limit:
            require(bool(batch), 'One document exceeds the request budget')
            yield batch
            batch = []
        require(len(encoded({field: [item], 'branch': 'main'})) <= limit,
                'One document exceeds the request budget')
        batch.append(item)
    if batch:
        yield batch


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, settings, timeout=45):
        self.base = settings['LAMBDADB_BASE_URL'].rstrip('/') + '/projects/' + urllib.parse.quote(settings['LAMBDADB_PROJECT_NAME'], safe='')
        self.key = settings['LAMBDADB_PROJECT_API_KEY']
        self.timeout = timeout
        self.opener = urllib.request.build_opener(NoRedirect())
        self.calls = 0
        self.downloads = 0

    def _json(self, request):
        self.calls += 1
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                status = response.status
                raw = response.read(MAX_RESPONSE + 1)
        except urllib.error.HTTPError as exc:
            status = exc.code
            exc.close()
            # Do not propagate response bodies, signed URLs, headers, or exception repr.
            raise HttpFailure(status) from None
        except (OSError, urllib.error.URLError):
            raise ProbeError('Transport failed; mutation outcome may be unknown') from None
        require(len(raw) <= MAX_RESPONSE, 'Response exceeds the experiment budget')
        try:
            return status, json.loads(raw)
        except (ValueError, UnicodeError):
            raise ProbeError('Response is not valid JSON') from None

    def request(self, method, path, body=None, expected=200):
        require(path.startswith('/collections') and '://' not in path, 'Invalid API path')
        payload = None if body is None else encoded(body)
        require(payload is None or len(payload) <= MAX_REQUEST, 'Request exceeds the experiment budget')
        req = urllib.request.Request(self.base + path, data=payload, method=method,
                                     headers={'x-api-key': self.key, 'Content-Type': 'application/json'})
        status, result = self._json(req)
        require(status == expected, 'Unexpected success status')
        return result

    def items(self, result):
        require(isinstance(result, dict) and isinstance(result.get('isDocsInline'), bool)
                and isinstance(result.get('docs'), list), 'Malformed document response')
        if result['isDocsInline']:
            docs = result['docs']
        else:
            require(result['docs'] == [], 'Offloaded response also contains inline documents')
            url = result.get('docsUrl')
            require(isinstance(url, str), 'Missing result download URL')
            parsed = urllib.parse.urlsplit(url)
            require(parsed.scheme == 'https' and parsed.netloc and not parsed.username
                    and not parsed.password and not parsed.fragment, 'Invalid result download URL')
            # Fresh request: no API credential/header inheritance; no redirect following.
            status, docs = self._json(urllib.request.Request(url, method='GET'))
            require(status == 200 and isinstance(docs, list), 'Invalid offloaded result array')
            self.downloads += 1
        require(all(isinstance(item, dict) and isinstance(item.get('doc'), dict)
                    and isinstance(item['doc'].get('id'), str) for item in docs),
                'Malformed result item or document identity')
        require(len({item['doc']['id'] for item in docs}) == len(docs), 'Duplicate document identity')
        return docs
