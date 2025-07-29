#!/bin/bash
set -e

BACKUP_FILE="/db-backups/latest.dump"

if [ -f "$BACKUP_FILE" ]; then
    echo "Backup file found. Waiting for database to be ready..."

    # Loop until the database is ready to accept connections
    until pg_isready -h localhost -U "$POSTGRES_USER"; do
      sleep 1
    done
    echo "Database is ready."
    
    echo "Restoring database..."
    # This part was already correct
    pg_restore -h localhost --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v "$BACKUP_FILE"
    echo "✅ Database restored."
else
    echo "No backup file found at $BACKUP_FILE. Initializing an empty database."
fi