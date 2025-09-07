# Migration: Add offeredBy and accommodationCategory to communitydata

## Overview
This migration adds `offeredBy` and `accommodationCategory` fields from the `homepagedata` collection to each community in the `communitydata` collection.

## Files Modified
- **Stage 2 Data Fetcher**: Added extraction of `offeredBy` and `accommodationCategory` from homepagedata
- **Stage 2 HTML Parser**: Modified to include these fields in each community document  
- **Stage 2 Orchestrator**: Updated to pass the new fields through the processing pipeline
- **Validation Schema**: Added support for optional `offeredBy` and `accommodationCategory` fields

## Migration Script

### Location
`src/scraper/temp/migrate_add_offered_by_accommodation_category.py`

### Usage

#### Dry Run (Recommended First)
```bash
cd src/scraper/temp
python migrate_add_offered_by_accommodation_category.py --dry-run
```

#### Limited Test Run
```bash
python migrate_add_offered_by_accommodation_category.py --limit 10
```

#### Full Migration
```bash
python migrate_add_offered_by_accommodation_category.py
```

### What the Migration Does
1. **Reads all community documents** from the `communitydata` collection
2. **For each document**:
   - Looks up the corresponding `homepagedata` document using `listing_id`
   - Extracts `property_data.offers.offeredBy` and `property_data.accommodationCategory`
   - Adds these fields to each community in the `community_data.communities` array
   - Only adds fields if they don't already exist and have values
3. **Updates the document** with the new fields
4. **Logs comprehensive statistics** about the migration process

### Safety Features
- **Dry run mode** to preview changes without making them
- **Field existence check** - only adds fields if they don't already exist
- **Value validation** - only adds fields if source data has non-null values
- **Comprehensive logging** to track progress and identify issues
- **Error handling** to continue processing if individual documents fail

### Expected Results
- Each community in `communitydata` will have:
  - `offeredBy`: The builder/developer name (e.g., "Richmond American Homes")
  - `accommodationCategory`: The property type (e.g., "Single Family Residence", "Condominium")

### Forward Compatibility
- **New scrapes** will automatically include these fields going forward
- **Validation updated** to accept the new optional fields
- **No breaking changes** to existing code or data structures

## Field Mapping

| Source | Target |
|--------|--------|
| `homepagedata.property_data.offers.offeredBy` | `communitydata.community_data.communities[].offeredBy` |
| `homepagedata.property_data.accommodationCategory` | `communitydata.community_data.communities[].accommodationCategory` |

## Rollback
If needed, the fields can be removed with:
```javascript
// MongoDB shell command to remove the fields
db.communitydata.updateMany(
  {},
  { 
    $unset: { 
      "community_data.communities.$[].offeredBy": "",
      "community_data.communities.$[].accommodationCategory": "" 
    }
  }
)
```
