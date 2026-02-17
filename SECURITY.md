# Security Policy

## Reporting a Vulnerability

If you discover a security issue, do not open a public issue with secrets or exploit details.
Contact the maintainer directly and include:
- affected file/path
- impact summary
- reproduction details
- mitigation suggestion

## Secret Handling Rules

- Never commit private keys, service-account JSON, OAuth tokens, `.env` files, or mobile Firebase config files.
- Use environment secrets (GitHub Actions secrets, Firebase/GCP secret managers, Cloud runtime env vars).
- Keep local-only credential files out of Git:
  - `quantura_site/functions/serviceAccountKey.json`
  - `quantura_ios/quantura_ios/GoogleService-Info.plist`
  - `quantura_android/app/google-services.json`

Bootstrap placeholders for local development:

```bash
./scripts/setup_local_firebase_credentials.sh
```

## Incident Response Runbook (Credential Exposure)

1. Revoke compromised keys immediately in Google Cloud Console / Firebase Console.
2. Create replacement credentials with restrictions:
   - iOS: bundle ID + API restrictions
   - Android: package name + SHA-1 + API restrictions
   - Web: HTTP referrer + API restrictions
3. Rotate service-account credentials and remove downloaded JSON keys where possible.
4. Purge leaked files from Git history and force push.

### Git history cleanup (required after public leak)

Use one of these approaches:

- Preferred: `git filter-repo`
- Fallback: BFG Repo-Cleaner

Example with `git filter-repo`:

```bash
pip install git-filter-repo

git filter-repo --invert-paths \
  --path quantura_site/functions/serviceAccountKey.json \
  --path quantura_ios/quantura_ios/GoogleService-Info.plist \
  --path quantura_android/app/google-services.json

git push origin --force --all
git push origin --force --tags
```

5. Invalidate and re-seed CI/CD secrets.
6. Review Cloud Logging, Billing, and API activity for abuse.

## Preventive Controls

- Secret scanning workflow: `.github/workflows/secret-scan.yml`
- `.gitignore` denies known credential filenames.
- Cloud Functions should use Application Default Credentials in production.
