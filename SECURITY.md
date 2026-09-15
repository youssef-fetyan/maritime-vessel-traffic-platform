# Security Policy & Advisory

## Sensitive Files in Git History

> [!WARNING]
> A historical `.env` file containing local development credentials and secret keys was included in earlier commits (e.g. `1b2eb36`).
> Although `.env` is now untracked and excluded via `.gitignore`, any secret present in git history must be treated as **publicly compromised** for production purposes.

### Required Actions for Production / Shared Deployments

1. **Rotate All Secrets**:
   Never use default or historical values in production. Generate fresh secrets for:
   - `POSTGIS_PASSWORD`
   - `SUPERSET_SECRET_KEY`
   - `SUPERSET_ADMIN_PASSWORD`
   - `AIRFLOW_SECRET_KEY`
   - `AIRFLOW_ADMIN_PASSWORD`
   - `AIRFLOW__CORE__FERNET_KEY`

2. **Use `.env.example` as Template**:
   Copy `.env.example` to `.env` and fill in newly generated secure values:
   ```bash
   cp .env.example .env
   ```

3. **Purging `.env` from Git History (Optional / Recommended for Public Forks)**:
   If this repository will be pushed to a public or shared remote, purge `.env` from all git commit trees:
   ```bash
   # Install git-filter-repo
   pip install git-filter-repo

   # Remove .env from all commits in git history
   git filter-repo --invert-paths --path .env
   ```
   > [!CAUTION]
   > Rewriting history alters all subsequent commit SHAs. Coordinate with all team members before force-pushing rewritten branches.
