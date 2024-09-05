# MongoDB to Azure Blob Storage Backup

This project backs up MongoDB collections to Azure Blob Storage as JSON files. It ensures that only the most recent 7 backups are retained, and it handles MongoDB data types such as `ObjectId` and `datetime`.

## Requirements

- Python 3.7+
- Required Python packages:
  - `azure-storage-blob`
  - `pymongo`
  - `python-dotenv`
  - `bson` (part of the `pymongo` package)

## Environment Variables

Ensure you have a `.env` file with the following environment variables, please reference to `.env.example` for reference
