# PostgreSQL Backup and Restore

This directory contains scripts for PostgreSQL incremental backups and restores using NAS storage.

## Overview

- **Backup Script**: `backup-incremental.sh` - Creates full and incremental PostgreSQL backups directly to NAS
- **Restore Script**: `restore-from-backup.sh` - Restores PostgreSQL backups from NAS to a VM

Both scripts support PostgreSQL 17+ incremental backup features and work with Patroni-managed PostgreSQL clusters.

## Backup Process

### Features

- **Full Backups**: Complete database backups
- **Incremental Backups**: Only changed data since last backup (PostgreSQL 17+)
- **Automatic Backup Type**: Automatically determines if full or incremental backup is needed
- **Smart Replica Selection**: Can automatically select healthy replicas for backup to reduce primary load
- **Compression**: Supports zstd (PostgreSQL 17+) or gzip (PostgreSQL 14+) compression
- **Direct NAS Storage**: Backups are written directly to NAS, no temporary local storage

### Configuration

#### Environment Variables

```bash
# NAS Configuration
export NAS_BACKUP_PATH=/mnt/nas/backups          # NAS mount point (default: /mnt/nas/backups)
export BACKUP_NAMESPACE=db-cluster                # Backup namespace/cluster name (default: unnamed-cluster)

# PostgreSQL Connection
export PGHOST=localhost                           # PostgreSQL host (default: localhost)
export PGBACKUPHOST=localhost                     # Alternative: PGBACKUPHOST
export PGBACKUPPORT=5432                          # PostgreSQL port (default: 5432)
export PGUSER=postgres                            # PostgreSQL user (default: postgres)
export PGPASSWORD=your_password                   # PostgreSQL password
export PGREPLICATIONUSER=replication              # Replication user (default: replication)
export POSTGRES_REPLICATION_PASSWORD=rep_pass    # Replication password

# Backup Configuration
export BACKUP_TYPE=auto                           # auto, full, or incremental (default: auto)
export BACKUP_MODE=smart                          # direct or smart (default: smart)
export MAX_REPLICATION_LAG=60                     # Max replication lag in seconds for smart mode (default: 60)
export BACKUP_COMPRESSION=zstd                    # gzip or zstd (default: zstd)
export FULL_BACKUP_FREQUENCY=7                    # Days between full backups (default: 7)
export RETENTION_DAYS=7                           # Days to keep old logs/manifests (default: 7)

# Retry Configuration
export UPLOAD_RETRY_ATTEMPTS=3                    # Number of retry attempts (default: 3)
export UPLOAD_RETRY_DELAY=10                      # Seconds between retries (default: 10)
```

### Usage

#### Basic Usage

```bash
# Automatic backup (determines full or incremental)
./backup-incremental.sh

# Force full backup
./backup-incremental.sh --full

# Force incremental backup
./backup-incremental.sh --incremental
```

#### Example with Custom Configuration

```bash
export NAS_BACKUP_PATH=/mnt/nas/backups
export BACKUP_NAMESPACE=production-cluster
export BACKUP_COMPRESSION=zstd
export FULL_BACKUP_FREQUENCY=7

./backup-incremental.sh
```

### Backup Structure on NAS

Backups are stored on NAS with the following structure:

```
/mnt/nas/backups/
└── {BACKUP_NAMESPACE}/
    ├── full/
    │   ├── full_backup_20251215_120000/
    │   │   ├── base.tar.zst
    │   │   ├── pg_wal.tar.zst
    │   │   └── backup_manifest
    │   └── full_backup_20251222_120000/
    ├── incremental/
    │   ├── incremental_backup_20251216_120000/
    │   └── incremental_backup_20251217_120000/
    ├── manifests/
    │   ├── backup_chain.txt
    │   ├── full_backup_20251215_120000.manifest
    │   ├── latest_manifest.txt
    │   └── last_full.txt
    └── logs/
        └── backup_20251215_120000.log
```

### Backup Process Flow

1. **Check Prerequisites**: Verifies NAS is mounted and writable
2. **Check Recent Backup**: Prevents duplicate backups if one was created recently (within 30 minutes)
3. **Determine Backup Type**: 
   - If no previous backup exists → Full backup
   - If last full backup is older than `FULL_BACKUP_FREQUENCY` days → Full backup
   - Otherwise → Incremental backup
4. **Select Backup Host**: 
   - `smart` mode: Automatically selects healthy replica if available
   - `direct` mode: Uses specified `PGHOST`
5. **Create Backup**: 
   - Full: `pg_basebackup` with full database
   - Incremental: `pg_basebackup --incremental` based on latest manifest
6. **Store on NAS**: Backup is written directly to NAS
7. **Update Metadata**: Updates backup chain and manifest files
8. **Cleanup**: Removes old backups (keeps only last full backup and its incrementals)

## Restore Process

### Features

- **Interactive Backup Selection**: Lists available backups and lets you choose
- **Full Backup Restore**: Direct extraction to PostgreSQL data directory
- **Incremental Backup Restore**: Uses `pg_combinebackup` to combine backup chain
- **Patroni Integration**: Automatically pauses/stops Patroni and PostgreSQL before restore
- **Hard Links**: Uses `--link` option for faster restore (no copying)
- **Direct Extraction**: Extracts backups directly to PostgreSQL data directory

### Configuration

#### Environment Variables

```bash
# NAS Configuration
export NAS_BACKUP_PATH=/mnt/nas/backups          # NAS mount point (default: /mnt/nas/backups)
export BACKUP_NAMESPACE=db-cluster                # Backup namespace/cluster name (default: db-cluster)

# PostgreSQL Configuration
export PGDATA_DIR=/data/pgdata                   # PostgreSQL data directory (default: /var/lib/postgresql/data)
export PGUSER=postgres                           # PostgreSQL user for ownership (default: postgres)

# Patroni Configuration
export PATRONI_CONFIG=/etc/patroni.yml           # Patroni config file (default: /etc/patroni.yml)
export PATRONI_CLUSTER=15/main                   # Patroni cluster name (default: 15/main)

# Staging Directory (for incremental backups)
export STAGING_DIR=/data/pg_restore_staging      # Staging directory (default: /data/pg_restore_staging)
```

### Usage

#### Basic Usage

```bash
./restore-from-backup.sh
```

The script will:
1. Check prerequisites
2. List available backups from NAS
3. Prompt you to select a backup
4. Ask for PostgreSQL data directory
5. Pause/stop Patroni and PostgreSQL
6. Restore the backup
7. Resume Patroni

#### Example with Custom Configuration

```bash
export NAS_BACKUP_PATH=/mnt/nas/backups
export BACKUP_NAMESPACE=production-cluster
export PGDATA_DIR=/data/pgdata
export PATRONI_CLUSTER=15/main

./restore-from-backup.sh
```

### Restore Process Flow

1. **Check Prerequisites**: 
   - Verifies PostgreSQL tools are available
   - Checks NAS is accessible
   - Detects Patroni if available

2. **List Backups**: Reads `backup_chain.txt` from NAS and displays available backups

3. **Select Backup**: Interactive selection of backup to restore

4. **Determine Backup Chain**: 
   - For full backups: Single backup
   - For incremental backups: Determines full backup + all incrementals leading to selected backup

5. **Get PostgreSQL Data Directory**: Prompts for or uses configured `PGDATA_DIR`

6. **Stop Patroni and PostgreSQL**:
   - Stops Patroni service
   - Pauses Patroni cluster
   - Stops PostgreSQL service
   - Waits for PostgreSQL processes to stop (up to 30 seconds)

7. **Restore Backup**:
   - **Full Backup**: Extracts directly to `PGDATA_DIR`
   - **Incremental Backup**: 
     - Extracts each backup in chain to staging directory (`/data/pg_restore_staging`)
     - Uses `pg_combinebackup --link` to combine backups using hard links
     - Outputs directly to `PGDATA_DIR`
     - Cleans up staging directory

8. **Set Permissions**: Sets proper ownership and permissions

9. **Resume Patroni**: Resumes Patroni cluster management

## Prerequisites

### For Backup

- PostgreSQL 14+ (for gzip compression) or PostgreSQL 17+ (for zstd compression and incremental backups)
- NAS mounted at `/mnt/nas/backups` (or custom `NAS_BACKUP_PATH`)
- PostgreSQL replication user with backup permissions
- `zstd` command (if using zstd compression)

### For Restore

- PostgreSQL 14+ (for full backups) or PostgreSQL 17+ (for incremental backups)
- `pg_combinebackup` command (required for incremental restore, PostgreSQL 17+)
- NAS mounted and accessible
- `zstd` or `gzip` command (for extracting compressed backups)
- Patroni (optional, but recommended for managed clusters)
- Sufficient disk space in staging directory (for incremental backups)

## Backup Types

### Full Backup

A complete copy of the PostgreSQL database. Contains all data files and WAL segments needed to restore the database to the point-in-time of the backup.

**When it's created:**
- First backup (no previous backup exists)
- Last full backup is older than `FULL_BACKUP_FREQUENCY` days (default: 7 days)
- Manually forced with `--full` option

### Incremental Backup

Only contains changes since the last backup (full or incremental). Much smaller than full backups but requires the full backup chain to restore.

**When it's created:**
- Previous backup exists
- Last full backup is less than `FULL_BACKUP_FREQUENCY` days old
- Manually forced with `--incremental` option

**Requirements:**
- PostgreSQL 17+
- Previous backup manifest must exist

## Backup Chain

Backups are tracked in `backup_chain.txt` with the following format:

```
full_backup_20251215_120000|20251215_120000|1702641600
incremental_backup_20251216_120000|20251216_120000|1702728000|full_backup_20251215_120000.manifest
incremental_backup_20251217_120000|20251217_120000|1702814400|incremental_backup_20251216_120000.manifest
```

Format: `backup_name|timestamp|epoch|base_manifest`

## Cleanup

### Automatic Cleanup

The backup script automatically cleans up old backups:
- Keeps only the **last full backup** and **all its incrementals**
- Deletes older full backups and their incrementals
- Cleans up old logs and manifests older than `RETENTION_DAYS` (default: 7 days)

### Manual Cleanup

You can manually clean up old backups on NAS:

```bash
# Remove old full backups (keep only the latest)
rm -rf /mnt/nas/backups/{BACKUP_NAMESPACE}/full/full_backup_YYYYMMDD_HHMMSS

# Remove old incremental backups
rm -rf /mnt/nas/backups/{BACKUP_NAMESPACE}/incremental/incremental_backup_YYYYMMDD_HHMMSS
```

**Warning**: Only delete backups that are no longer part of the current backup chain. Check `backup_chain.txt` first.

## Troubleshooting

### Backup Issues

#### "NAS backup path does not exist"
- Ensure NAS is mounted at the configured path
- Check mount point: `mount | grep nas`
- Verify path is writable: `test -w /mnt/nas/backups`

#### "No healthy replica found"
- Check replication status: `SELECT * FROM pg_stat_replication;`
- Increase `MAX_REPLICATION_LAG` if replicas are slightly behind
- Use `BACKUP_MODE=direct` to use primary instead

#### "pg_basebackup failed"
- Verify replication user has proper permissions
- Check PostgreSQL is running and accessible
- Verify replication connection settings

### Restore Issues

#### "pg_combinebackup is not available"
- Install PostgreSQL 17+ which includes `pg_combinebackup`
- For PostgreSQL 14-16, only full backups can be restored

#### "PostgreSQL processes may still be running"
- Manually stop PostgreSQL: `systemctl stop postgresql`
- Check for remaining processes: `pgrep -f postgres`
- Force stop if needed: `pkill -9 postgres` (use with caution)

#### "Backup not found"
- Verify `BACKUP_NAMESPACE` matches the backup directory structure
- Check NAS is mounted and accessible
- List backups: `ls -la /mnt/nas/backups/{BACKUP_NAMESPACE}/full/`

#### "directory exists but is not empty"
- Ensure `PGDATA_DIR` is cleared before restore
- The script should handle this automatically, but you can manually clear: `rm -rf /data/pgdata/*`

## Best Practices

1. **Regular Full Backups**: Ensure full backups are taken regularly (default: every 7 days)
2. **Monitor Backup Size**: Incremental backups should be much smaller than full backups
3. **Test Restores**: Periodically test restore process to ensure backups are valid
4. **Monitor NAS Space**: Ensure NAS has sufficient space for backups
5. **Backup Verification**: Check backup logs regularly for errors
6. **Patroni Integration**: Always use Patroni pause/resume for managed clusters
7. **Staging Directory**: Ensure `/data` volume has sufficient space for incremental restore staging

## Logs

Backup logs are stored on NAS:
- Location: `/mnt/nas/backups/{BACKUP_NAMESPACE}/logs/`
- Format: `backup_YYYYMMDD_HHMMSS.log`
- Retention: Logs older than `RETENTION_DAYS` are automatically cleaned up

## Examples

### Daily Backup Schedule

Add to crontab for daily backups:

```bash
# Daily backup at 2 AM
0 2 * * * /path/to/backup-incremental.sh >> /var/log/pg-backup.log 2>&1
```

### Restore to New Server

```bash
# 1. Mount NAS
mount -t nfs nas-server:/backups /mnt/nas/backups

# 2. Set configuration
export NAS_BACKUP_PATH=/mnt/nas/backups
export BACKUP_NAMESPACE=production-cluster
export PGDATA_DIR=/data/pgdata

# 3. Run restore
./restore-from-backup.sh

# 4. Start PostgreSQL/Patroni
systemctl start patroni
```

### Verify Backup

```bash
# List available backups
cat /mnt/nas/backups/{BACKUP_NAMESPACE}/manifests/backup_chain.txt

# Check backup files
ls -lh /mnt/nas/backups/{BACKUP_NAMESPACE}/full/full_backup_*/
```

## Support

For issues or questions:
1. Check backup logs in `/mnt/nas/backups/{BACKUP_NAMESPACE}/logs/`
2. Verify NAS mount and permissions
3. Check PostgreSQL and Patroni status
4. Review script output for specific error messages

