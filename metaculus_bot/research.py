from __future__ import annotations

import hashlib
import ipaddress
import re
import socket
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urlparse

import requests


class Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "noscript"}:
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in {"script", "style", "noscript"} and self.skip:
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)


def public_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.port not in (None, 443):
        return False
    if parsed.hostname == "metaculus.com" or parsed.hostname.endswith(".metaculus.com"):
        return False
    try:
        addresses = socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM)
        return bool(addresses) and all(ipaddress.ip_address(row[4][0]).is_global for row in addresses)
    except (OSError, ValueError):
        return False


def public_get(url: str, *, maximum=1_500_000) -> tuple[bytes, str]:
    # No authenticated session, cookies, ambient proxy credentials, or unrestricted
    # redirect. Question links are untrusted data, never instructions to a tool.
    session = requests.Session()
    session.trust_env = False
    for _ in range(4):
        if not public_url(url):
            raise ValueError("NONPUBLIC_SOURCE_URL")
        with session.get(url, headers={"User-Agent": "Quantura-Research/1.0"}, timeout=(8, 20),
                         allow_redirects=False, stream=True) as response:
            if response.is_redirect:
                from urllib.parse import urljoin
                url = urljoin(url, response.headers["Location"])
                continue
            response.raise_for_status()
            raw = bytearray()
            for chunk in response.iter_content(65536):
                raw.extend(chunk)
                if len(raw) > maximum:
                    raise ValueError("SOURCE_TOO_LARGE")
            return bytes(raw), url
    raise ValueError("SOURCE_REDIRECT_LIMIT")


def evidence(question) -> list[dict]:
    text = "\n".join(filter(None, [question.resolution_criteria, question.fine_print, question.background_info]))
    urls = list(dict.fromkeys(unescape(u).rstrip(".,;:") for u in re.findall(r'https://[^\s<>"\)]+', text)))
    results = []
    for url in urls[:6]:
        if len(results) == 3:
            break
        try:
            raw, final = public_get(url)
            parser = Text()
            parser.feed(raw.decode("utf-8", errors="replace"))
            excerpt = re.sub(r"\s+", " ", " ".join(parser.parts)).strip()[:14000]
            if len(excerpt) < 100:
                continue
            results.append({"id": f"S{len(results) + 1}", "url": final, "sha256": hashlib.sha256(raw).hexdigest(),
                            "excerpt": excerpt})
        except (requests.RequestException, ValueError, OSError):
            continue
    return results
