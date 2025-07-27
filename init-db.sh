#!/bin/bash
set -e

# Define the path for the backup file inside the container
# CORRECTED PATH: /db-backups to match compose.yaml
BACKUP_FILE="/db-backups/latest.dump"

# Check if the backup file exists
if [ -f "$BACKUP_FILE" ]; then
    echo "Backup file found. Restoring database..."
    # Restore the database using the credentials passed as environment variables
    pg_restore --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v "$BACKUP_FILE"
    echo "✅ Database restored."
else
    echo "No backup file found at $BACKUP_FILE. Initializing an empty database."
fi