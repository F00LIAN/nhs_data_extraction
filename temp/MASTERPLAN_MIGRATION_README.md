# 🏗️ Masterplan Migration Script

## Overview

This script migrates existing masterplan listings from the `communitydata` collection to the new `masterplandata` collection. This is a one-time migration to implement the new masterplan routing system.

## What This Script Does

1. **Scans `communitydata`** for listings with "masterplan" in the `listing_id`
2. **Fetches corresponding data** from `homepagedata` collection 
3. **Transforms data structure** to match `masterplandata` schema:
   - `listing_id` → `masterplanlisting_id`
   - `property_data` → `masterplan_data`
   - `price` → `price_range`
4. **Inserts into `masterplandata`** collection
5. **Removes from `communitydata`** collection
6. **Logs all operations** for audit trail

## Usage

### Prerequisites
- MongoDB connection configured in `.env`
- Backup your database before running

### Run the Migration
```bash
cd src/scraper/temp
python migrate_masterplan_from_communitydata.py
```

## Output

### Console Output
```
🏗️ Masterplan Migration Tool
Migrating masterplan listings from communitydata to masterplandata
============================================================
📋 Masterplan migration started - Log: logs/masterplan_migration_log_20250106_143022.log
🔍 Scanning communitydata collection for masterplan listings...
🏗️ Found masterplan: https://www.newhomesource.com/masterplan/ca/santa-paula/harvest-at-limoneira/152225
✅ Masterplan inserted/updated in masterplandata: https://www.newhomesource.com/masterplan/...
🗑️ Removed masterplan from communitydata: https://www.newhomesource.com/masterplan/...
```

### Migration Summary
```
📊 MASTERPLAN MIGRATION SUMMARY
==================================================
⏱️ Duration: 0:00:05.123456
📋 Total documents checked: 127
🏗️ Masterplans found: 3
✅ Successfully migrated: 3
🗑️ Removed from communitydata: 3
❌ Errors encountered: 0
🎉 All masterplan migrations completed successfully!
```

## Data Transformation Example

### Before (in `communitydata`)
```json
{
  "listing_id": "https://www.newhomesource.com/masterplan/ca/santa-paula/harvest-at-limoneira/152225",
  "community_data": { "communities": [...] },
  "listing_status": "active"
}
```

### After (in `masterplandata`)
```json
{
  "masterplanlisting_id": "https://www.newhomesource.com/masterplan/ca/santa-paula/harvest-at-limoneira/152225",
  "masterplan_data": {
    "price_range": "$699,852 - $854,989",
    "name": "Harvest at Limoneira",
    "url": "https://www.newhomesource.com/masterplan/...",
    "address": {
      "formatted_address": "Santa Paula, CA 93060",
      "county": "Ventura County"
    }
  },
  "migration_info": {
    "migrated_from": "communitydata",
    "migration_date": "2025-01-06T14:30:22.123456"
  }
}
```

## Logging

Detailed logs are saved to `logs/masterplan_migration_log_YYYYMMDD_HHMMSS.log` including:
- All operations performed
- Success/failure for each masterplan
- Complete audit trail
- Error details for troubleshooting

## Safety Features

- **Non-destructive operations**: Inserts to `masterplandata` before removing from `communitydata`
- **Comprehensive logging**: Full audit trail of all operations
- **Error handling**: Continues processing even if individual records fail
- **Validation**: Checks data integrity before migration
- **Migration metadata**: Tracks migration history in documents

## Verification

After migration, verify:
1. **Count check**: Masterplans moved from `communitydata` to `masterplandata`
2. **Data integrity**: Field transformations applied correctly  
3. **No duplicates**: No masterplans remain in `communitydata`
4. **Logs review**: Check migration log for any errors

```javascript
// MongoDB queries to verify migration
db.communitydata.find({"listing_id": /masterplan/i}).count()  // Should be 0
db.masterplandata.find().count()  // Should match migrated count
```

## Rollback (if needed)

If rollback is needed:
1. Use migration logs to identify migrated documents
2. Transform data back to `communitydata` format
3. Remove from `masterplandata`
4. Re-insert into `communitydata`

*Note: Keep migration logs for rollback reference*

## Integration

This migration enables the new routing system where:
- Future masterplan discoveries go directly to `masterplandata`
- `communitydata` only contains regular communities
- Price tracking focuses on regular communities only

---

**⚠️ Important**: Run this migration only once. The routing system will handle masterplans automatically for future scrapes.
