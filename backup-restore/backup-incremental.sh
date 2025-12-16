#!/bin/bash
set -euo pipefail
#set -x
# PostgreSQL Incremental Backup to NAS
# Supports full and incremental backups using PostgreSQL 18 features
# Usage: ./backup-incremental.sh [--full|--incremental]

# Backup namespace (used to organize backups on NAS)
BACKUP_NAMESPACE="${BACKUP_NAMESPACE:-db-cluster}"


# Configuration (can be overridden by environment variables)
PGHOST="${PGBACKUPHOST:-localhost}"
PGPORT="${PGBACKUPPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-}"
PGREPLICATIONUSER="${PGREPLICATIONUSER:-replication}"
PGREPLICATIONPASSWORD="${POSTGRES_REPLICATION_PASSWORD:-}"  
# BACKUP_DIR is no longer used (backups go directly to NAS)
RETENTION_DAYS="${RETENTION_DAYS:-7}"
FULL_BACKUP_FREQUENCY="${FULL_BACKUP_FREQUENCY:-7}"  

# Backup host selection mode
BACKUP_MODE="${BACKUP_MODE:-direct}"  # direct: use PGHOST as-is, smart: auto-select healthy replica
MAX_REPLICATION_LAG="${MAX_REPLICATION_LAG:-60}"  # Maximum acceptable replication lag in seconds for smart mode

# Backup destination / type configuration
BACKUP_DEST="${BACKUP_DEST:-nas}"  # nas (write directly to NAS)
BACKUP_TYPE="${1:-auto}"  # auto, full, or incremental

# NAS configuration
NAS_BACKUP_PATH="${NAS_BACKUP_PATH:-/mnt/nas/backups}"

# Compression configuration
BACKUP_COMPRESSION="${BACKUP_COMPRESSION:-zstd}"  # gzip (PG14+) or zstd (PG17+)

# Upload retry configuration
UPLOAD_RETRY_ATTEMPTS="${UPLOAD_RETRY_ATTEMPTS:-3}"  # Number of upload retry attempts
UPLOAD_RETRY_DELAY="${UPLOAD_RETRY_DELAY:-10}"       # Seconds to wait between retries

# NAS directories (backups go directly to NAS)
NAS_BASE_DIR="${NAS_BACKUP_PATH}/${BACKUP_NAMESPACE}"
NAS_LOG_DIR="${NAS_BASE_DIR}/logs"
NAS_MANIFEST_DIR="${NAS_BASE_DIR}/manifests"
NAS_FULL_BACKUP_DIR="${NAS_BASE_DIR}/full"
NAS_INCREMENTAL_BACKUP_DIR="${NAS_BASE_DIR}/incremental"

# Create necessary directories on NAS
mkdir -p "$NAS_LOG_DIR" "$NAS_MANIFEST_DIR" "$NAS_FULL_BACKUP_DIR" "$NAS_INCREMENTAL_BACKUP_DIR" 

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${NAS_LOG_DIR}/backup_${TIMESTAMP}.log"

# Function to log messages
log() {
    local message="[$(date +'%Y-%m-%d %H:%M:%S')] $*"
    echo "$message" >> "$LOG_FILE"
    echo "$message" >&2
}

# Function to select a healthy replica for backup
# Returns the IP address of a healthy replica, or empty string if none found
select_healthy_replica() {
    log "Querying pg_stat_replication to find healthy replicas..."
    
    # Query pg_stat_replication from primary
    local query="
    SELECT client_addr
    FROM pg_stat_replication
    WHERE state = 'streaming'
      AND application_name = 'walreceiver'
      AND replay_lag IS NOT NULL
      AND EXTRACT(EPOCH FROM replay_lag) < ${MAX_REPLICATION_LAG}
    ORDER BY client_addr ASC
    LIMIT 1;
    "
    
    # Execute query on primary
    local replica_host
    replica_host=$(PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -t -A -c "$query" 2>/dev/null | tr -d '[:space:]')
    
    if [ -n "$replica_host" ] && [ "$replica_host" != "" ]; then
        log "Found healthy replica: $replica_host"
        echo "$replica_host"
    else
        log "No healthy replica found meeting criteria: state=streaming, lag < ${MAX_REPLICATION_LAG}s"
        echo ""
    fi
}

# Function to determine backup host based on mode
select_backup_host() {
    if [ "$BACKUP_MODE" = "smart" ]; then
        log "Backup mode: SMART - Attempting to select healthy replica"
        local selected_replica
        selected_replica=$(select_healthy_replica)
        
        if [ -n "$selected_replica" ]; then
            log "Selected replica for backup: $selected_replica"
            log "This reduces load on primary and improves backup performance"
            echo "$selected_replica"
        else
            log "WARNING: No healthy replica available, falling back to primary: $PGHOST"
            echo "$PGHOST"
        fi
    else
        log "Backup mode: DIRECT - Using specified host: $PGHOST"
        echo "$PGHOST"
    fi
}

# Function to get compression flag for pg_basebackup
# Returns: --compress (gzip) or --compress=zstd based on BACKUP_COMPRESSION
get_compression_flag() {
    case "$BACKUP_COMPRESSION" in
        gzip)
            echo "--gzip"
            ;;
        zstd)
            echo "--compress=zstd"
            ;;
        *)
            log "WARNING: Unknown compression format '$BACKUP_COMPRESSION', defaulting to gzip"
            echo "--compress"
            ;;
    esac
}

# Function to cleanup on exit
cleanup() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log "ERROR: Backup failed with exit code $exit_code"
    fi
    exit $exit_code
}
trap cleanup EXIT

# Validate configuration
if [ ! -d "$NAS_BACKUP_PATH" ]; then
    log "ERROR: NAS backup path does not exist: $NAS_BACKUP_PATH"
    log "Please ensure NAS is mounted at $NAS_BACKUP_PATH"
    exit 1
fi
if [ ! -w "$NAS_BACKUP_PATH" ]; then
    log "ERROR: NAS backup path is not writable: $NAS_BACKUP_PATH"
    exit 1
fi
log "Backup destination: NAS -> ${NAS_BACKUP_PATH}/${BACKUP_NAMESPACE}"

# No cleanup needed - backups go directly to NAS

# Determine backup type
determine_backup_type() {
    if [ "$BACKUP_TYPE" != "auto" ]; then
        echo "$BACKUP_TYPE"
        return
    fi
    
    # Check if we should take a full backup
    local last_full_date=""
    if [ -f "${NAS_MANIFEST_DIR}/last_full.txt" ]; then
        last_full_date=$(cat "${NAS_MANIFEST_DIR}/last_full.txt")
    fi
    
    if [ -z "$last_full_date" ]; then
        echo "full"
        return
    fi
    
    local days_since_full=$(( ($(date +%s) - $(date -d "$last_full_date" +%s)) / 86400 ))
    
    if [ $days_since_full -ge $FULL_BACKUP_FREQUENCY ]; then
        echo "full"
    else
        echo "incremental"
    fi
}

# Get latest manifest (from full or incremental backup on NAS)
get_latest_manifest() {
    local latest_manifest=""
    # Check for latest manifest (full or incremental) on NAS
    if [ -f "${NAS_MANIFEST_DIR}/latest_manifest.txt" ]; then
        latest_manifest=$(cat "${NAS_MANIFEST_DIR}/latest_manifest.txt")
    # Fallback to latest full manifest for backward compatibility
    elif [ -f "${NAS_MANIFEST_DIR}/latest_full_manifest.txt" ]; then
        latest_manifest=$(cat "${NAS_MANIFEST_DIR}/latest_full_manifest.txt")
    fi
    echo "$latest_manifest"
}

# Retry copy function (for NAS)
retry_copy() {
    local source="$1"
    local destination="$2"
    local max_attempts="${UPLOAD_RETRY_ATTEMPTS:-3}"
    local retry_delay="${UPLOAD_RETRY_DELAY:-10}"
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        log "Copy attempt $attempt of $max_attempts..."
        
        # Create destination directory if it doesn't exist
        mkdir -p "$(dirname "$destination")"
        
        if cp -r "$source" "$destination" 2>&1 | tee -a "$LOG_FILE"; then
            log "✓ Copy succeeded on attempt $attempt"
            return 0
        else
            log "✗ Copy attempt $attempt failed"
            
            if [ $attempt -lt $max_attempts ]; then
                log "Waiting ${retry_delay}s before retry..."
                sleep "$retry_delay"
                attempt=$((attempt + 1))
            else
                log "✗ All $max_attempts copy attempts failed"
                return 1
            fi
        fi
    done
    
    return 1
}

# Create full backup
create_full_backup() {
    log "=========================================="
    log "Creating FULL Backup"
    log "=========================================="
    
    BACKUP_NAME="full_backup_${TIMESTAMP}"
    # Write backup directly to NAS
    NAS_FULL_BACKUP_PATH="${NAS_FULL_BACKUP_DIR}/${BACKUP_NAME}"
    NAS_MANIFEST_PATH="${NAS_MANIFEST_DIR}/${BACKUP_NAME}.manifest"
    
    log "Backup Name: $BACKUP_NAME"
    log "NAS Backup Path: $NAS_FULL_BACKUP_PATH"
    
    # Select backup host based on mode
    local BACKUP_HOST
    BACKUP_HOST=$(select_backup_host)
    log "Using backup host: $BACKUP_HOST"
    
    # Create backup directory on NAS (must be empty for pg_basebackup)
    mkdir -p "$NAS_FULL_BACKUP_PATH"
    
    # Get compression flag based on configuration
    local compression_flag
    compression_flag=$(get_compression_flag)
    
    # Create full backup with manifest directly on NAS
    log "Starting pg_basebackup (full backup) directly to NAS..."
    
    local pg_basebackup_cmd=(
        pg_basebackup
        -h "$BACKUP_HOST"
        -p "$PGPORT"
        -U "$PGREPLICATIONUSER"
        -D "$NAS_FULL_BACKUP_PATH"
        -X stream
        -Ft           # Tar format
        "$compression_flag"
        -P            # Progress
    )
    
    # Only prompt for password if not set in environment
    if [ -z "$PGREPLICATIONPASSWORD" ]; then
        pg_basebackup_cmd+=(-W)  # Prompt for password
    fi
    
    # Export password if set (to avoid prompts)
    export PGPASSWORD=$PGREPLICATIONPASSWORD
    
    if "${pg_basebackup_cmd[@]}" 2>&1 | tee -a "$LOG_FILE"; then
        log "✓ Full backup completed successfully on NAS"
    else
        log "✗ Full backup failed"
        exit 1
    fi
    
    # Copy manifest to NAS manifests directory
    if [ -f "${NAS_FULL_BACKUP_PATH}/backup_manifest" ]; then
        cp "${NAS_FULL_BACKUP_PATH}/backup_manifest" "$NAS_MANIFEST_PATH"
        # Save as latest manifest (for incremental chain) and as latest full (for tracking)
        echo "$NAS_MANIFEST_PATH" > "${NAS_MANIFEST_DIR}/latest_manifest.txt"
        echo "$NAS_MANIFEST_PATH" > "${NAS_MANIFEST_DIR}/latest_full_manifest.txt"
        echo "$(date +%Y-%m-%d)" > "${NAS_MANIFEST_DIR}/last_full.txt"
        log "✓ Manifest saved to NAS: $NAS_MANIFEST_PATH"
    else
        log "WARNING: Manifest file not found"
    fi
    
    # Calculate size
    BACKUP_SIZE=$(du -sh "$NAS_FULL_BACKUP_PATH" | cut -f1)
    log "Backup size: $BACKUP_SIZE"
    
    # Update backup chain on NAS
    echo "$BACKUP_NAME|$TIMESTAMP|$(date +%s)" >> "${NAS_MANIFEST_DIR}/backup_chain.txt"
    log "✓ Backup registered in chain on NAS"

    unset PGPASSWORD
}

# Create incremental backup
create_incremental_backup() {
    log "=========================================="
    log "Creating INCREMENTAL Backup"
    log "=========================================="
    
    local base_manifest=$(get_latest_manifest)
    
    if [ -z "$base_manifest" ] || [ ! -f "$base_manifest" ]; then
        log "WARNING: No previous manifest found (needs full backup first), creating full backup instead"
        create_full_backup
        return
    fi
    
    BACKUP_NAME="incremental_backup_${TIMESTAMP}"
    # Write backup directly to NAS
    NAS_INCREMENTAL_BACKUP_PATH="${NAS_INCREMENTAL_BACKUP_DIR}/${BACKUP_NAME}"
    NAS_MANIFEST_PATH="${NAS_MANIFEST_DIR}/${BACKUP_NAME}.manifest"
    
    log "Backup Name: $BACKUP_NAME"
    log "Base Manifest: $(basename "$base_manifest")"
    log "NAS Backup Path: $NAS_INCREMENTAL_BACKUP_PATH"
    
    # Select backup host based on mode
    local BACKUP_HOST
    BACKUP_HOST=$(select_backup_host)
    log "Using backup host: $BACKUP_HOST"
    
    # Create backup directory on NAS (must be empty for pg_basebackup)
    mkdir -p "$NAS_INCREMENTAL_BACKUP_PATH"
    
    # Get compression flag based on configuration
    local compression_flag
    compression_flag=$(get_compression_flag)
    
    # Create incremental backup directly on NAS
    log "Starting pg_basebackup (incremental backup) directly to NAS..."
    
    local pg_basebackup_cmd=(
        pg_basebackup
        -h "$BACKUP_HOST"
        -p "$PGPORT"
        -U "$PGREPLICATIONUSER"
        -D "$NAS_INCREMENTAL_BACKUP_PATH"
        -X stream
        -Ft           # Tar format
        "$compression_flag"
        -P            # Progress
        --incremental="$base_manifest" # Incremental mode
    )
    
    # Only prompt for password if not set in environment
    if [ -z "$PGREPLICATIONPASSWORD" ]; then
        pg_basebackup_cmd+=(-W)  # Prompt for password
    fi
    
    # Export password if set (to avoid prompts)
    export PGPASSWORD=$PGREPLICATIONPASSWORD
    
    if "${pg_basebackup_cmd[@]}" 2>&1 | tee -a "$LOG_FILE"; then
        log "✓ Incremental backup completed successfully on NAS"
    else
        log "✗ Incremental backup failed"
        exit 1
    fi
    
    # Copy manifest to NAS manifests directory
    if [ -f "${NAS_INCREMENTAL_BACKUP_PATH}/backup_manifest" ]; then
        cp "${NAS_INCREMENTAL_BACKUP_PATH}/backup_manifest" "$NAS_MANIFEST_PATH"
        echo "$NAS_MANIFEST_PATH" > "${NAS_MANIFEST_DIR}/latest_manifest.txt"
        log "✓ Incremental manifest saved to NAS: $NAS_MANIFEST_PATH"
    fi
    
    # Calculate size
    BACKUP_SIZE=$(du -sh "$NAS_INCREMENTAL_BACKUP_PATH" | cut -f1)
    log "Backup size: $BACKUP_SIZE"
    
    # Update backup chain on NAS
    echo "$BACKUP_NAME|$TIMESTAMP|$(date +%s)|$(basename "$base_manifest")" >> "${NAS_MANIFEST_DIR}/backup_chain.txt"
    log "✓ Backup registered in chain on NAS"

    unset PGPASSWORD
}

# Cleanup old backups on NAS (keep only last full backup and its incrementals)
cleanup_old_backups() {
    log ""
    log "=========================================="
    log "Cleaning up Old Backups on NAS"
    log "=========================================="
    
    # Get the last full backup name from backup_chain.txt on NAS
    if [ ! -f "${NAS_MANIFEST_DIR}/backup_chain.txt" ]; then
        log "No backup chain file found on NAS, skipping cleanup"
        return 0
    fi
    
    local last_full_backup=$(grep "^full_backup_" "${NAS_MANIFEST_DIR}/backup_chain.txt" | tail -1 | cut -d'|' -f1)
    
    if [ -z "$last_full_backup" ]; then
        log "No full backup found in chain, skipping cleanup"
        return 0
    fi
    
    log "Current full backup to keep: $last_full_backup"
    
    # Get all incremental backups for the current full backup
    local current_full_found=false
    local incrementals_to_keep=()
    
    while IFS='|' read -r backup_name timestamp epoch base_manifest; do
        if [ "$backup_name" = "$last_full_backup" ]; then
            current_full_found=true
            continue
        fi
        
        if [ "$current_full_found" = true ]; then
            if [[ "$backup_name" =~ ^incremental_backup_ ]]; then
                incrementals_to_keep+=("$backup_name")
            elif [[ "$backup_name" =~ ^full_backup_ ]]; then
                # Hit next full backup, stop
                break
            fi
        fi
        done < "${NAS_MANIFEST_DIR}/backup_chain.txt"
    
    if [ ${#incrementals_to_keep[@]} -gt 0 ]; then
        log "Incremental backups to keep: ${incrementals_to_keep[*]}"
    else
        log "No incremental backups to keep"
    fi
    
    # Delete old full backups on NAS (except the current one)
    local deleted_count=0
    if [ -d "$NAS_FULL_BACKUP_DIR" ]; then
        for full_backup in "$NAS_FULL_BACKUP_DIR"/full_backup_*; do
            # Skip if glob didn't match anything
            [ ! -e "$full_backup" ] && continue
            
            if [ -d "$full_backup" ]; then
                local backup_name=$(basename "$full_backup")
                if [ "$backup_name" != "$last_full_backup" ]; then
                    log "Deleting old full backup on NAS: $backup_name"
                    if rm -rf "$full_backup" 2>/dev/null; then
                        ((deleted_count++))
                    else
                        log "WARNING: Failed to delete $backup_name (may be in use)"
                    fi
                fi
            fi
        done
    fi
    
    # Delete old incremental backups on NAS (except the ones in current chain)
    if [ -d "$NAS_INCREMENTAL_BACKUP_DIR" ]; then
        for inc_backup in "$NAS_INCREMENTAL_BACKUP_DIR"/incremental_backup_*; do
            # Skip if glob didn't match anything
            [ ! -e "$inc_backup" ] && continue
            
            if [ -d "$inc_backup" ]; then
                local backup_name=$(basename "$inc_backup")
                local should_keep=false
                
                for keep_name in "${incrementals_to_keep[@]}"; do
                    if [ "$backup_name" = "$keep_name" ]; then
                        should_keep=true
                        break
                    fi
                done
                
                if [ "$should_keep" = false ]; then
                    log "Deleting old incremental backup on NAS: $backup_name"
                    if rm -rf "$inc_backup" 2>/dev/null; then
                        ((deleted_count++))
                    else
                        log "WARNING: Failed to delete $backup_name (may be in use)"
                    fi
                fi
            fi
        done
    fi
    
    if [ $deleted_count -gt 0 ]; then
        log "✓ Deleted $deleted_count old backup(s)"
    else
        log "✓ No old backups to delete"
    fi
    
    log "Current backup chain kept locally:"
    log "  Full: $last_full_backup"
    for inc_name in "${incrementals_to_keep[@]}"; do
        log "  Incremental: $inc_name"
    done
    
    return 0
}

# Main execution
log "=========================================="
log "PostgreSQL Incremental Backup to NAS"
log "=========================================="
log "Timestamp: $TIMESTAMP"
log "Destination: NAS -> ${NAS_BACKUP_PATH}/${BACKUP_NAMESPACE}"
log ""

# Check if we have a recent backup (prevent duplicates after OOMKill)
if check_recent_backup; then
    log "Recent backup found - exiting without taking new backup"
    exit 0
fi

# Determine backup type if auto
ACTUAL_BACKUP_TYPE=$(determine_backup_type)
log "Backup Type: $ACTUAL_BACKUP_TYPE"

# Execute backup based on type
case "$ACTUAL_BACKUP_TYPE" in
    full)
        create_full_backup
        ;;
    incremental)
        create_incremental_backup
        ;;
    *)
        log "ERROR: Unknown backup type: $ACTUAL_BACKUP_TYPE"
        exit 1
        ;;
esac

# Cleanup old backups on NAS (keep only current chain)
if ! cleanup_old_backups; then
    log "WARNING: Cleanup function encountered issues, but backup was successful"
fi

# Cleanup old logs and manifests on NAS
log ""
log "Cleaning up old logs and manifests on NAS (older than $RETENTION_DAYS days)..."
find "$NAS_LOG_DIR" -name "backup_*.log" -mtime +$RETENTION_DAYS -delete 2>/dev/null || true
find "$NAS_MANIFEST_DIR" -name "*.manifest" -mtime +$RETENTION_DAYS -delete 2>/dev/null || true

# Summary
log ""
log "=========================================="
log "Backup Summary"
log "=========================================="
log "Backup Type: $ACTUAL_BACKUP_TYPE"
log "Backup Name: $BACKUP_NAME"
log "Backup Size: $BACKUP_SIZE"
if [ "$ACTUAL_BACKUP_TYPE" = "incremental" ]; then
    log "NAS Path: ${NAS_INCREMENTAL_BACKUP_DIR}/${BACKUP_NAME}/"
else
    log "NAS Path: ${NAS_FULL_BACKUP_DIR}/${BACKUP_NAME}/"
fi
log "Log File: $LOG_FILE"
log "Status: ✓ SUCCESS"
log "=========================================="

exit 0

