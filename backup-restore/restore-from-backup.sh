#!/bin/bash
set -euo pipefail

# PostgreSQL Restore from NAS
# This script restores PostgreSQL backups from NAS to a PostgreSQL data directory on VM
# Usage: ./restore-from-backup.sh

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAS_BACKUP_PATH="${NAS_BACKUP_PATH:-/mnt/nas/backups}"
BACKUP_NAMESPACE="${BACKUP_NAMESPACE:-db-cluster}"
PGDATA_DIR="${PGDATA_DIR:-/data/pgdata}"
PGUSER="${PGUSER:-postgres}"
PATRONI_CONFIG="${PATRONI_CONFIG:-/etc/patroni.yml}"
PATRONI_CLUSTER="${PATRONI_CLUSTER:-15/main}"  # Patroni cluster name
STAGING_DIR="${STAGING_DIR:-/data/pg_restore_staging}"  # Staging directory for incremental backups

# Function to print colored output
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

log_header() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$*${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Function to check prerequisites
check_prerequisites() {
    log_header "Checking Prerequisites"
    
    # Check if PostgreSQL tools are available
    if ! command -v pg_combinebackup &> /dev/null; then
        log_warning "pg_combinebackup not found - incremental restore may not work"
        log_info "Full backups will still work"
    fi
    
    if ! command -v zstd &> /dev/null && ! command -v gzip &> /dev/null; then
        log_error "Neither zstd nor gzip found - cannot extract compressed backups"
        exit 1
    fi
    
    # Check if NAS is accessible
    if [ ! -d "$NAS_BACKUP_PATH" ]; then
        log_error "NAS backup path does not exist: $NAS_BACKUP_PATH"
        log_info "Please ensure NAS is mounted at $NAS_BACKUP_PATH"
        exit 1
    fi
    
    if [ ! -r "$NAS_BACKUP_PATH" ]; then
        log_error "NAS backup path is not readable: $NAS_BACKUP_PATH"
        exit 1
    fi
    
    # Check if Patroni is available
    if command -v patronictl &> /dev/null; then
        log_info "Patroni detected - will pause before restore and resume after"
        PATRONI_AVAILABLE=true
        
        # Check if Patroni config file exists
        if [ -f "$PATRONI_CONFIG" ]; then
            log_info "Found Patroni config: $PATRONI_CONFIG"
        else
            log_warning "Patroni config file not found: $PATRONI_CONFIG"
            log_info "Will use cluster name: $PATRONI_CLUSTER"
        fi
    else
        log_info "Patroni not found - assuming standard PostgreSQL installation"
        PATRONI_AVAILABLE=false
    fi
    
    log_success "All prerequisites met"
}

# Function to get Patroni cluster name from config
get_patroni_cluster() {
    if [ -z "$PATRONI_CLUSTER" ] && [ "$PATRONI_AVAILABLE" = true ] && [ -f "$PATRONI_CONFIG" ]; then
        log_info "Reading Patroni cluster name from config: $PATRONI_CONFIG"
        # Try to extract cluster name from patroni.yml (namespace/name format)
        local cluster_from_config=$(grep -E "^\s*scope:\s*" "$PATRONI_CONFIG" 2>/dev/null | head -1 | awk '{print $2}' | tr -d '"' | tr -d "'" || echo "")
        if [ -n "$cluster_from_config" ]; then
            PATRONI_CLUSTER="$cluster_from_config"
            log_success "Found cluster name in config: $PATRONI_CLUSTER"
        else
            log_warning "Could not extract cluster name from config"
            log_info "Using default cluster name: $PATRONI_CLUSTER"
        fi
    fi
    
    if [ -z "$PATRONI_CLUSTER" ]; then
        log_error "Patroni cluster name not set"
        log_info "Set PATRONI_CLUSTER environment variable or ensure config file has 'scope' field"
        exit 1
    fi
}

# Function to stop Patroni and PostgreSQL
pause_patroni() {
    if [ "$PATRONI_AVAILABLE" = false ]; then
        return 0
    fi
    
    log_header "Stopping Patroni and PostgreSQL"
    
    get_patroni_cluster
    
    log_info "Stopping Patroni cluster: $PATRONI_CLUSTER"
    log_info "Using Patroni config: $PATRONI_CONFIG"
    
    # Stop Patroni service first
    log_info "Stopping Patroni service..."
    if systemctl is-active --quiet patroni 2>/dev/null || systemctl is-active --quiet patroni@* 2>/dev/null; then
        if systemctl stop patroni 2>/dev/null || systemctl stop patroni@* 2>/dev/null; then
            log_success "Patroni service stopped"
        else
            log_warning "Could not stop Patroni service via systemctl"
        fi
    fi
    
    # Also pause Patroni cluster using patronictl
    log_info "Pausing Patroni cluster..."
    if patronictl -c "$PATRONI_CONFIG" pause "$PATRONI_CLUSTER" 2>&1 | tee -a /tmp/patroni_pause.log; then
        log_success "Patroni cluster paused successfully"
        PATRONI_PAUSED=true
    else
        log_warning "Failed to pause Patroni cluster via patronictl"
        PATRONI_PAUSED=false
    fi
    
    # Stop PostgreSQL service
    log_info "Stopping PostgreSQL service..."
    local pg_stopped=false
    if systemctl is-active --quiet postgresql 2>/dev/null; then
        if systemctl stop postgresql 2>/dev/null; then
            pg_stopped=true
            log_success "PostgreSQL service stopped"
        fi
    elif systemctl is-active --quiet postgresql@* 2>/dev/null; then
        # Try to find the specific postgresql@ instance
        local pg_service=$(systemctl list-units --type=service --state=running | grep -o "postgresql@[^ ]*" | head -1)
        if [ -n "$pg_service" ]; then
            if systemctl stop "$pg_service" 2>/dev/null; then
                pg_stopped=true
                log_success "PostgreSQL service ($pg_service) stopped"
            fi
        fi
    fi
    
    if [ "$pg_stopped" = false ]; then
        log_warning "PostgreSQL service may not be managed by systemd"
    fi
    
    # Wait for PostgreSQL processes to stop
    log_info "Waiting for PostgreSQL processes to stop..."
    local wait_time=0
    local max_wait=30  # Maximum wait time in seconds
    local pg_processes=0
    
    while [ $wait_time -lt $max_wait ]; do
        # Check for postgres processes (excluding our check)
        pg_processes=$(pgrep -f "postgres.*-D" 2>/dev/null | wc -l 2>/dev/null | tr -d '[:space:]' || echo "0")
        # Ensure it's a valid integer
        pg_processes=$((pg_processes + 0))
        
        if [ "$pg_processes" -eq 0 ]; then
            log_success "PostgreSQL processes stopped"
            break
        fi
        
        sleep 1
        wait_time=$((wait_time + 1))
        
        if [ $((wait_time % 5)) -eq 0 ]; then
            log_info "  Still waiting... ($wait_time/${max_wait}s) - $pg_processes PostgreSQL process(es) running"
        fi
    done
    
    if [ "$pg_processes" -gt 0 ]; then
        log_warning "PostgreSQL processes may still be running after ${max_wait}s wait"
        log_info "Found $pg_processes PostgreSQL process(es) - you may need to stop them manually"
        log_info "Run: pkill -9 postgres (as a last resort)"
    else
        log_success "All PostgreSQL processes stopped"
    fi
    
    # Additional wait to ensure everything is fully stopped
    log_info "Waiting additional 3 seconds to ensure complete shutdown..."
    sleep 3
}

# Function to resume Patroni
resume_patroni() {
    if [ "$PATRONI_AVAILABLE" = false ] || [ "${PATRONI_PAUSED:-false}" != true ]; then
        return 0
    fi
    
    log_header "Resuming Patroni"
    
    get_patroni_cluster
    
    log_info "Resuming Patroni cluster: $PATRONI_CLUSTER"
    log_info "Using Patroni config: $PATRONI_CONFIG"
    
    # Use patronictl with config file and cluster name
    if patronictl -c "$PATRONI_CONFIG" resume "$PATRONI_CLUSTER" 2>&1 | tee -a /tmp/patroni_resume.log; then
        log_success "Patroni cluster resumed successfully"
    else
        log_warning "Failed to resume Patroni cluster - you may need to resume manually"
        log_info "Run: patronictl -c $PATRONI_CONFIG resume $PATRONI_CLUSTER"
    fi
}

# Function to list available backups from NAS
list_nas_backups() {
    log_header "Listing Available Backups from NAS"
    
    local backup_chain_path="${NAS_BACKUP_PATH}/${BACKUP_NAMESPACE}/manifests/backup_chain.txt"
    
    if [ ! -f "$backup_chain_path" ]; then
        log_error "Backup chain file not found at: $backup_chain_path"
        log_info "Make sure BACKUP_NAMESPACE matches the backup directory structure on NAS"
        log_info "Current BACKUP_NAMESPACE: $BACKUP_NAMESPACE"
        exit 1
    fi
    
    # Copy backup chain to temp file
    cp "$backup_chain_path" /tmp/backup_list.txt
    
    if [ ! -s /tmp/backup_list.txt ]; then
        log_error "No backups found in backup_chain.txt"
        exit 1
    fi
    
    # Parse the backup list from backup_chain.txt
    local backups=()
    local index=1
    
    echo ""
    echo "Available Backups from NAS:"
    echo ""
    
    # Read backup_chain.txt format: backup_name|timestamp|epoch|base_manifest
    while IFS='|' read -r backup_name timestamp epoch base_manifest; do
        if [ -n "$backup_name" ]; then
            local date_formatted=$(date -j -f "%Y%m%d_%H%M%S" "$timestamp" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -d "@$epoch" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "$timestamp")
            
            if [[ "$backup_name" == full_backup_* ]]; then
                backups+=("full|$backup_name|$date_formatted")
                echo "  [$index] [FULL] $backup_name ($date_formatted)"
            elif [[ "$backup_name" == incremental_backup_* ]]; then
                backups+=("incremental|$backup_name|$date_formatted")
                echo "  [$index] [INCR] $backup_name ($date_formatted) → based on $(basename "$base_manifest" .manifest)"
            fi
            index=$((index + 1))
        fi
    done < /tmp/backup_list.txt
    
    if [ ${#backups[@]} -eq 0 ]; then
        log_error "No backups found in backup chain"
        log_info "NAS Path: $NAS_BACKUP_PATH/$BACKUP_NAMESPACE"
        log_info "The backup_chain.txt file may be empty or corrupted"
        exit 1
    fi
    
    echo ""
    echo -n "Select backup to restore (1-${#backups[@]}): "
    read -r selection
    
    if ! [[ "$selection" =~ ^[0-9]+$ ]] || [ "$selection" -lt 1 ] || [ "$selection" -gt ${#backups[@]} ]; then
        log_error "Invalid selection"
        exit 1
    fi
    
    local selected="${backups[$((selection-1))]}"
    BACKUP_TYPE="${selected%%|*}"
    BACKUP_NAME=$(echo "$selected" | cut -d'|' -f2)
    BACKUP_DATE=$(echo "$selected" | cut -d'|' -f3)
    
    log_success "Selected backup: $BACKUP_NAME ($BACKUP_TYPE)"
}

# Function to determine backup chain for incremental backups
determine_backup_chain() {
    if [ "$BACKUP_TYPE" = "full" ]; then
        BACKUP_CHAIN=("$BACKUP_NAME")
        log_info "Full backup selected, no chain needed"
        return
    fi
    
    log_header "Determining Backup Chain for Incremental Restore"
    
    # Use the backup list we already have from NAS
    BACKUP_CHAIN=()
    
    # Parse backup chain to find the full backup and all incrementals leading to selected backup
    local found_full=false
    local found_target=false
    
    while IFS='|' read -r backup_name timestamp epoch base_manifest; do
        if [ -n "$backup_name" ]; then
            if [[ "$backup_name" == full_backup_* ]]; then
                # Start a new chain with this full backup
                BACKUP_CHAIN=("$backup_name")
                found_full=true
            elif [[ "$backup_name" == incremental_backup_* ]] && [ "$found_full" = true ]; then
                # Add incremental to chain
                BACKUP_CHAIN+=("$backup_name")
            fi
            
            # Check if we've reached the target backup
            if [ "$backup_name" = "$BACKUP_NAME" ]; then
                found_target=true
                break
            fi
        fi
    done < /tmp/backup_list.txt
    
    if [ "$found_target" = false ]; then
        log_error "Selected backup not found in chain metadata"
        exit 1
    fi
    
    if [ ${#BACKUP_CHAIN[@]} -eq 0 ]; then
        log_error "Could not determine backup chain"
        exit 1
    fi
    
    log_info "Backup chain determined (${#BACKUP_CHAIN[@]} backups):"
    for ((i=0; i<${#BACKUP_CHAIN[@]}; i++)); do
        echo "    $((i+1)). ${BACKUP_CHAIN[$i]}"
    done
}

# Function to get PostgreSQL data directory
get_pgdata_dir() {
    log_header "Configure PostgreSQL Data Directory"
    
    echo ""
    echo "Current PGDATA: $PGDATA_DIR"
    echo ""
    echo -n "Enter PostgreSQL data directory (press Enter to use current): "
    read -r new_pgdata
    
    if [ -n "$new_pgdata" ]; then
        PGDATA_DIR="$new_pgdata"
    fi
    
    if [ ! -d "$PGDATA_DIR" ]; then
        log_warning "Directory does not exist: $PGDATA_DIR"
        echo -n "Create directory? (yes/no): "
        read -r create_dir
        
        if [ "$create_dir" = "yes" ]; then
            mkdir -p "$PGDATA_DIR"
            log_success "Created directory: $PGDATA_DIR"
        else
            log_error "Directory must exist to restore backups"
            exit 1
        fi
    fi
    
    if [ ! -w "$PGDATA_DIR" ]; then
        log_error "Directory is not writable: $PGDATA_DIR"
        exit 1
    fi
    
    # Check if directory is not empty
    if [ "$(ls -A "$PGDATA_DIR" 2>/dev/null)" ]; then
        log_warning "Directory is not empty: $PGDATA_DIR"
        echo -n "Directory will be cleared. Continue? (yes/no): "
        read -r confirm
        
        if [ "$confirm" != "yes" ]; then
            log_info "Restore cancelled"
            exit 0
        fi
    fi
    
    log_success "PostgreSQL data directory: $PGDATA_DIR"
}

# Helper function to format duration
format_duration() {
    local seconds=$1
    if [ $seconds -lt 60 ]; then
        echo "${seconds}s"
    elif [ $seconds -lt 3600 ]; then
        printf "%dm %ds\n" $((seconds/60)) $((seconds%60))
    else
        printf "%dh %dm %ds\n" $((seconds/3600)) $((seconds%3600/60)) $((seconds%60))
    fi
}

# Function to restore from NAS backups
restore_from_nas() {
    log_header "Restoring from NAS Backups"
    
    local restore_start_time=$(date +%s)
    log_info "Restore started at: $(date '+%Y-%m-%d %H:%M:%S')"
    log_info "Restoring backup chain (${#BACKUP_CHAIN[@]} backup(s)) from NAS..."
    
    # Clear PGDATA_DIR before restore
    log_info "Clearing PostgreSQL data directory: $PGDATA_DIR"
    rm -rf "${PGDATA_DIR:?}"/*
    
    # Determine restore approach based on backup count
    local backup_count=${#BACKUP_CHAIN[@]}
    
    if [ "$backup_count" -eq 1 ]; then
        # Single full backup - extract directly to PGDATA_DIR
        local backup="${BACKUP_CHAIN[0]}"
        local backup_type="full"
        
        log_info "Processing full backup: $backup"
        
        local source_path="${NAS_BACKUP_PATH}/${BACKUP_NAMESPACE}/${backup_type}/${backup}"
        
        if [ ! -d "$source_path" ]; then
            log_error "Backup not found: $source_path"
            exit 1
        fi
        
        cd "$PGDATA_DIR"
        
        # Extract base.tar (with any compression) directly to PGDATA_DIR
        log_info "  Extracting base archive..."
        if [ -f "${source_path}/base.tar.zst" ]; then
            zstd -d -c "${source_path}/base.tar.zst" | tar -x
        elif [ -f "${source_path}/base.tar.gz" ]; then
            tar -xzf "${source_path}/base.tar.gz"
        elif [ -f "${source_path}/base.tar" ]; then
            tar -xf "${source_path}/base.tar"
        else
            log_error "base.tar archive not found in: $source_path"
            exit 1
        fi
        
        # Extract pg_wal.tar (with any compression) directly to PGDATA_DIR
        log_info "  Extracting WAL archive..."
        if [ -f "${source_path}/pg_wal.tar.zst" ]; then
            zstd -d -c "${source_path}/pg_wal.tar.zst" | tar -x -C pg_wal/
        elif [ -f "${source_path}/pg_wal.tar.gz" ]; then
            tar -xzf "${source_path}/pg_wal.tar.gz" -C pg_wal/
        elif [ -f "${source_path}/pg_wal.tar" ]; then
            tar -xf "${source_path}/pg_wal.tar" -C pg_wal/
        else
            log_error "pg_wal.tar archive not found in: $source_path"
            exit 1
        fi
        
        # Copy manifest
        if [ -f "${source_path}/backup_manifest" ]; then
            cp "${source_path}/backup_manifest" .
        fi
        
        rm -f "${PGDATA_DIR}/standby.signal" 2>/dev/null || true
        log_success "Full backup restored directly to $PGDATA_DIR"
    else
        # Incremental chain - requires pg_combinebackup from PostgreSQL 17+
        log_info "Incremental backup chain detected - checking for pg_combinebackup..."
        
        if ! command -v pg_combinebackup &> /dev/null; then
            log_error "pg_combinebackup is not available"
            log_error "Incremental backup restore requires PostgreSQL 17 or later"
            log_info "Solutions:"
            log_info "  1. Install PostgreSQL 17+ with pg_combinebackup"
            log_info "  2. Restore a full backup instead - works on all versions"
            exit 1
        fi
        
        log_success "pg_combinebackup found - proceeding with incremental restore"
        log_info "Extracting backups for combination..."
        
        # Create staging directory on /data volume for backup extraction
        log_info "Using staging directory: $STAGING_DIR"
        mkdir -p "$STAGING_DIR"
        
        # Clean up any previous staging directory contents
        rm -rf "${STAGING_DIR:?}"/*
        
        # Extract each backup in the chain to staging directory
        local backup_dirs=()
        for backup in "${BACKUP_CHAIN[@]}"; do
            local backup_type="full"
            if [[ "$backup" == incremental_backup_* ]]; then
                backup_type="incremental"
            fi
            
            log_info "Processing: $backup"
            
            local source_path="${NAS_BACKUP_PATH}/${BACKUP_NAMESPACE}/${backup_type}/${backup}"
            local dest_path="${STAGING_DIR}/${backup}"
            
            if [ ! -d "$source_path" ]; then
                log_error "Backup not found: $source_path"
                exit 1
            fi
            
            mkdir -p "$dest_path"
            cd "$dest_path"
            
            # Extract base.tar (with any compression)
            log_info "  Extracting base archive..."
            if [ -f "${source_path}/base.tar.zst" ]; then
                zstd -d -c "${source_path}/base.tar.zst" | tar -x
            elif [ -f "${source_path}/base.tar.gz" ]; then
                tar -xzf "${source_path}/base.tar.gz"
            elif [ -f "${source_path}/base.tar" ]; then
                tar -xf "${source_path}/base.tar"
            else
                log_error "base.tar archive not found in: $source_path"
                exit 1
            fi
            
            # Extract pg_wal.tar (with any compression)
            log_info "  Extracting WAL archive..."
            if [ -f "${source_path}/pg_wal.tar.zst" ]; then
                zstd -d -c "${source_path}/pg_wal.tar.zst" | tar -x -C pg_wal/
            elif [ -f "${source_path}/pg_wal.tar.gz" ]; then
                tar -xzf "${source_path}/pg_wal.tar.gz" -C pg_wal/
            elif [ -f "${source_path}/pg_wal.tar" ]; then
                tar -xf "${source_path}/pg_wal.tar" -C pg_wal/
            else
                log_error "pg_wal.tar archive not found in: $source_path"
                exit 1
            fi
            
            # Copy manifest
            if [ -f "${source_path}/backup_manifest" ]; then
                cp "${source_path}/backup_manifest" .
            fi
            
            backup_dirs+=("$dest_path")
            log_success "  Extracted: $backup"
        done
        
        # Combine backups directly to PGDATA_DIR using --link option
        log_info ""
        log_info "Combining backups using pg_combinebackup with --link option..."
        log_info "  This uses hard links instead of copying for faster restore"
        local restore_start=$(date +%s)
        
        # Ensure PGDATA_DIR is empty
        log_info "Clearing PGDATA_DIR: $PGDATA_DIR"
        rm -rf "${PGDATA_DIR:?}"/*
        
        # Get backup directories in chronological order
        cd "$STAGING_DIR"
        local backup_dirs_sorted=$(ls -dt */ | tac | xargs)
        log_info "  Backup chain: $backup_dirs_sorted"
        log_info "  Output directory: $PGDATA_DIR"
        
        # Use --link option to create hard links instead of copying
        if pg_combinebackup --link $backup_dirs_sorted -o "$PGDATA_DIR"; then
            log_success "pg_combinebackup completed (using hard links)"
            rm -f "${PGDATA_DIR}/standby.signal" 2>/dev/null || true
            
            # Cleanup staging directory
            log_info "Cleaning up staging directory: $STAGING_DIR"
            rm -rf "${STAGING_DIR:?}"/*
        else
            log_error "pg_combinebackup failed"
            exit 1
        fi
    fi
    
    # Set proper ownership and permissions
    if [ -n "$PGUSER" ]; then
        local pg_uid=$(id -u "$PGUSER" 2>/dev/null || echo "")
        local pg_gid=$(id -g "$PGUSER" 2>/dev/null || echo "")
        
        if [ -n "$pg_uid" ] && [ -n "$pg_gid" ]; then
            log_info "Setting ownership to $PGUSER ($pg_uid:$pg_gid)..."
            chown -R "$pg_uid:$pg_gid" "$PGDATA_DIR"
        else
            log_warning "Could not determine UID/GID for $PGUSER, skipping ownership change"
        fi
    fi
    
    chmod 700 "$PGDATA_DIR"
    
    local restore_end=$(date +%s)
    local restore_duration=$((restore_end - restore_start))
    local total_duration=$((restore_end - restore_start_time))
    
    log_success ""
    log_success "Restore completed successfully at: $(date '+%Y-%m-%d %H:%M:%S')"
    log_info "Restore time: $(format_duration $restore_duration)"
    log_info "Total time: $(format_duration $total_duration)"
}

# Function to display summary
display_summary() {
    log_header "Restore Summary"
    
    echo ""
    echo "Source:             NAS Storage (${NAS_BACKUP_PATH}/${BACKUP_NAMESPACE})"
    echo "Backup Restored:    $BACKUP_NAME"
    echo "Backup Type:        $BACKUP_TYPE"
    echo "Backup Date:        $BACKUP_DATE"
    echo "Backup Chain:       ${#BACKUP_CHAIN[@]} backup(s)"
    echo "PostgreSQL Data:    $PGDATA_DIR"
    echo "Completed at:       $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    
    log_success "Restore completed successfully!"
    log_info ""
    log_info "Next steps:"
    log_info "  1. Verify PostgreSQL configuration"
    log_info "  2. Start PostgreSQL service"
    log_info "  3. Verify database connectivity"
}

# Main execution
main() {
    log_header "PostgreSQL Restore Utility - VM Mode"
    
    echo ""
    log_warning "This script will:"
    echo "  1. List available backups from NAS"
    echo "  2. Pause Patroni (if running) and stop PostgreSQL"
    echo "  3. Restore the selected backup to PostgreSQL data directory"
    echo "  4. Resume Patroni (if it was paused)"
    echo "  5. Set proper ownership and permissions"
    echo ""
    echo -n "Continue? (yes/no): "
    read -r confirm
    
    if [ "$confirm" != "yes" ]; then
        log_info "Restore cancelled"
        exit 0
    fi
    
    # Execute restore workflow
    check_prerequisites
    list_nas_backups
    determine_backup_chain
    get_pgdata_dir
    pause_patroni
    restore_from_nas
    resume_patroni
    display_summary
}

# Run main function
main "$@"
