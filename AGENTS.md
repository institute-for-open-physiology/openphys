# OpenPhys agent instructions

## This repository is public

Everything committed here is published to the world, together with its full git history. A commit
cannot be reliably un-published: forks, clones, caches and archives keep it even after a force-push.
Treat every change, commit message, pull request, issue and CI log as public.

### Never commit participant data

OpenPhys records people's hearts. Recordings, and anything derived from them, are personal health
data. Do not commit, paste into a commit message, issue, pull request or CI log, or otherwise publish:

- raw or processed recordings (ECG, accelerometer, heart rate/RR, blood pressure, GPS, audio), WAL
  segments, Parquet archives, SQLite databases, exports, reports or plots made from a real person's
  data, except through the de-identified release process below;
- names, initials, email addresses, phone numbers, dates of birth, addresses or locations of
  participants, beta testers or their families;
- health information about any identifiable person, including the maintainers (diagnoses, findings,
  medications, devices such as pacemakers, incidents);
- absolute recording dates and clock times tied to a person or recording;
- device identifiers: Polar H10 IDs/serials, seca serials, Bluetooth MAC addresses, phone models tied
  to a person, installation or session IDs from a real deployment;
- participant IDs from a real deployment. Use `local`, `p0001`-style placeholders or synthetic IDs in
  code, tests, docs and fixtures; use `A1B2C3D4`/`0A0B0C0D` for example H10 IDs and `example.com` /
  `example.invalid` for hosts and emails.

Data under `data/`, `data_*/` and other ignored directories stays local. Never use `git add -f` to
force ignored data into a commit.

**Releasing example data.** The only real-world data in this repository is under
`examples/readme_figures/data/`. Adding to it requires all of the following, and a maintainer's
explicit approval in the pull request:

1. the participant's documented consent for public release;
2. only the short window needed, relative time only, no identifiers of any kind (see the list above),
   including inside file metadata (PNG text chunks, npz/zip entries, EDF/WFDB headers, CSV columns);
3. an `extract/` script that takes source locations as arguments, with no private paths, names or
   dates inside it.

Test fixtures must be synthetic.

### Never commit secrets or private infrastructure

Do not commit, print in logs, or paste into issues:

- API keys, tokens, passwords, device credentials, signing keys and keystores, App Store Connect /
  Google Play keys, provisioning profiles, `.env*` files, cloud provider credentials;
- private hostnames, IP addresses, VPN/tailnet names, home directory paths (`/Users/...`,
  `/home/...`), deployment state, backup locations or production user lists.

Secrets belong in the environment or a secret manager, and CI secrets in GitHub Actions secrets.
Code must read them from configuration, never contain defaults that are real values. If a secret is
ever committed, treat it as compromised: tell a maintainer and rotate it. Removing the commit is not
enough.

### Before every commit or push

1. Review the full staged diff (`git diff --cached`), not only the files you meant to change.
2. Check for binary or data files you did not intend to add (`git diff --cached --stat`).
3. Search the staged changes for the categories above: names, emails, serials/MAC addresses, dates
   tied to recordings, hostnames, home paths, keys and tokens.
4. If in doubt, leave it out and ask a maintainer.

## Engineering rules

- Run `scripts/check.sh` before reporting Rust, Python or dashboard changes as complete. For Android,
  run the Gradle checks in `TECHNICAL.md`; for mobile screen changes, `python3 scripts/mobile_contract.py --check`.
- Prefer deterministic tests (unit tests with fakes, raw GATT/PMD and WAL replay) over live-sensor
  tests; use live hardware only for the real Bluetooth boundary.
- Coverage targets and exemptions are in `docs/test_coverage.md`.
- Every Bluetooth/CoreBluetooth await needs a timeout, or a comment naming the watchdog that bounds it.
- Avoid panics and `unwrap()` in collector runtime code. Fail loudly rather than lose data silently:
  record disconnects, reconnects, stream gaps, watchdog timeouts and archive failures.
- The Polar H10 does not backfill data missed during a disconnect, and only one collector can own its
  ECG/accelerometer streams at a time.
- Keep raw data canonical: Parquet, indexes and dashboards must be regenerable from the WAL.
