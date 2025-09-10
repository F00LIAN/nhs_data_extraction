# 🏠 NewHomeSource Database Builder - Complete Scraper System

A comprehensive data extraction system that builds a complete database of new home listings, community details, and price history from NewHomeSource.com.

## 🎯 System Overview

This scraper system is designed to create and maintain a comprehensive database of new home sales data through a **two-stage extraction process** plus **intelligent price tracking**:

1. **Stage 1**: Discovers and catalogs all property listings
2. **Stage 2**: Extracts detailed community and pricing information  
3. **Price Tracking**: Monitors and tracks price changes over time

## 🗄️ Database Architecture

The system builds a MongoDB database with eight main collections:

| Collection | Purpose | Data Source |
|------------|---------|-------------|
| `homepagedata` | Basic property listings and metadata | Stage 1 |
| `homepagedata_archived` | Archived Stage 1 listings | Stage 1→2 Router |
| `communitydata` | Detailed community and neighborhood info | Stage 2 |
| `communitydata_archived` | Archived Stage 2 listings | Stage 2 Processor |
| `masterplandata` | Masterplan community data (separate processing) | Stage 1→2 Router |
| `basiccommunitydata` | Basic community data (separate processing) | Stage 1→2 Router |
| `price_history_permanent` | Long-term price trend analysis | Price Tracker |
| `price_history_permanent_archived` | Archived price histories | Price Tracker |

## 🏗️ System Architecture

```
src/scraper/
├── run_nhs.py              ← Main execution controller
├── scraper_config.json     ← Configuration settings
├── 
├── stageone/               ← STAGE 1: Property Discovery
│   ├── scraping_orchestrator.py    ← Coordinates Stage 1 workflow
│   ├── url_generator.py             ← Generates search URLs
│   ├── http_fetcher.py              ← Downloads web pages
│   ├── listing_parser.py            ← Extracts property data
│   └── database_manager.py          ← Saves to database
│
├── stagetwo/               ← STAGE 2: Community Details
│   ├── orchestrator.py              ← Coordinates Stage 2 workflow
│   ├── data_fetcher.py              ← Gets property list from Stage 1
│   ├── http_client.py               ← Downloads property detail pages
│   ├── html_parser.py               ← Extracts community data
│   └── data_processor.py            ← Processes and saves data
│
├── shared/                 ← SHARED: Common Utilities
│   ├── price_tracker.py             ← Tracks price changes
│   ├── price_history.py             ← Analyzes price trends
│   └── stage_one_and_two_check.py   ← Masterplan detection and routing
│
├── validation/             ← DATA VALIDATION
│   ├── stage_one_structure_validation.py
│   └── stage_two_structure_validation.py
│
└── logging/nhs/           ← EXECUTION LOGS
    └── scraper_log_YYYYMMDD_HHMMSS.log
```

## 🚀 Quick Start

### Prerequisites
- MongoDB database
- Python 3.8+
- `.env` file with `MONGO_DB_URI=your_mongodb_connection_string`

### Installation
```bash
pip install -r requirements.txt
```

### Basic Usage
```bash
# Complete database build (recommended)
python run_nhs.py --stage full

# Stage 1 only (property discovery)
python run_nhs.py --stage 1

# Stage 2 only (community details)  
python run_nhs.py --stage 2

# Performance tuning
python run_nhs.py --max-concurrent 15 --browser chrome
```

## 📋 Stage 1 - Property Discovery

**Goal**: Find every property listing and create the foundation database

### Components

#### `scraping_orchestrator.py` - Workflow Controller
- **Purpose**: Coordinates the entire Stage 1 extraction process
- **Input**: URLs and request settings
- **Output**: Scraped listings and session statistics
- **Key Functions**: Session management, logging setup, progress tracking

#### `url_generator.py` - Search URL Creator  
- **Purpose**: Generates URLs for all locations to scrape
- **Input**: Configuration from `scraper_config.json`
- **Output**: List of URLs with location metadata
- **Key Functions**: URL pattern generation, pagination handling

#### `http_fetcher.py` - Web Page Downloader
- **Purpose**: Downloads HTML content from target URLs
- **Input**: URLs with request settings
- **Output**: HTML content for parsing
- **Key Functions**: Concurrent requests, retry logic, browser impersonation

#### `listing_parser.py` - Data Extractor
- **Purpose**: Extracts property data from HTML pages
- **Input**: HTML content and source URLs
- **Output**: Structured property documents
- **Key Functions**: JSON-LD parsing, HTML fallback parsing, data validation

#### `database_manager.py` - Data Persistence
- **Purpose**: Manages database operations for Stage 1
- **Input**: Parsed property documents
- **Output**: Updated database collections
- **Key Functions**: Insert/update logic, archival management, change detection

### Data Flow
1. URL Generator creates search URLs from config
2. HTTP Fetcher downloads pages concurrently  
3. Listing Parser extracts property data from HTML
4. Database Manager saves to `homepagedata` collection
5. Orchestrator tracks progress and handles errors

### Output Collections
- **`homepagedata`**: All discovered properties with basic info
- **`archivedlistings`**: Properties no longer available

## 📊 Stage 2 - Community Details Extraction

**Goal**: Get detailed information about each property's community and pricing

### Components

#### `orchestrator.py` - Stage 2 Controller
- **Purpose**: Manages the complete Stage 2 extraction workflow
- **Input**: Property URLs from Stage 1
- **Output**: Community data and processing statistics  
- **Key Functions**: Batch processing, retry handling, progress reporting

#### `data_fetcher.py` - Stage 1 Data Retriever
- **Purpose**: Retrieves property URLs from Stage 1 results
- **Input**: Database queries
- **Output**: List of properties to process
- **Key Functions**: Database querying, data preparation

#### `http_client.py` - Property Page Downloader
- **Purpose**: Downloads individual property detail pages
- **Input**: Property URLs
- **Output**: HTML content of property pages
- **Key Functions**: Proxy support, browser rotation, concurrent downloading

#### `html_parser.py` - Community Data Extractor
- **Purpose**: Extracts community information from property pages
- **Input**: Property page HTML
- **Output**: Structured community data
- **Key Functions**: Multiple parsing strategies, data normalization

#### `data_processor.py` - Change Detection & Storage
- **Purpose**: Compares new data with existing and saves updates
- **Input**: Parsed community data
- **Output**: Updated database with change tracking
- **Key Functions**: Change detection, data comparison, price snapshot coordination

### Data Flow
1. Data Fetcher gets property list from Stage 1
2. HTTP Client downloads property detail pages
3. HTML Parser extracts community information
4. Data Processor detects changes and updates database
5. Price Tracker captures current pricing data

### Output Collections
- **`communitydata`**: Detailed community information
- **`price_history_permanent`**: Price timeline storage

## 🏗️ Special Community Detection & Routing

**Goal**: Separate special community types (masterplan, basiccommunity) from regular communities for specialized processing

### Special Community Detection Process

Between Stage 1 and Stage 2, the system automatically detects and routes special community types:

#### `stage_one_and_two_check.py` - Intelligent Routing
- **Purpose**: Analyzes Stage 1 results and separates special vs regular communities
- **Detection Logic**: 
  - Checks if `listing_id` contains "masterplan" 
  - Checks if `listing_id` contains "basiccommunity"
- **Routing Decision**: 
  - **Masterplan** → `masterplandata` collection (skip Stage 2)
  - **Basic Community** → `basiccommunitydata` collection (skip Stage 2)
  - **Regular** → Continue to Stage 2 processing

#### Duplicate Prevention & Update Logic

**All collections implement proper upsert patterns to prevent duplicates:**

**Masterplan & Basic Community Collections:**
```python
# Check if document exists
existing_doc = await collection.find_one({"unique_id": listing_id})

if existing_doc:
    # Update existing document
    new_doc["previous_scraped_at"] = existing_doc.get("scraped_at")
    await collection.replace_one({"unique_id": listing_id}, new_doc)
else:
    # Insert new document
    await collection.insert_one(new_doc)
```

**Community Data Collection:**
```python
# Upsert with change detection
await communitydata_collection.update_one(
    {"listing_id": listing_id},
    {"$set": community_doc},
    upsert=True
)
```

**Key Benefits:**
- **No Duplicates**: Each listing_id appears only once per collection
- **Update Tracking**: `previous_scraped_at` field tracks last update
- **Change Detection**: Only meaningful changes trigger updates
- **Atomic Operations**: Database consistency maintained

#### Archiving & Lifecycle Management

**Missing Listing Detection & Archiving:**

**Stage 1 Missing Listings:**
```python
# Detect listings missing from current scrape
missing_listings = previous_active_listings - current_scrape_listings

# Move to archive collection
for listing_id in missing_listings:
    doc["listing_status"] = "archived"
    doc["archived_at"] = datetime.now()
    doc["archive_reason"] = "missing from current Stage 1 scrape"
    
    await homepagedata_archived_collection.insert_one(doc)
    await homepagedata_collection.delete_one({"listing_id": listing_id})
```

**Stage 2 Missing Listings:**
```python
# Move archived communities to separate collection
for listing_id in removed_listing_ids:
    doc["listing_status"] = "archived"
    doc["archived_at"] = datetime.now()
    doc["archive_reason"] = "missing from current Stage 2 scrape"
    
    await communitydata_archived_collection.insert_one(doc)
    await communitydata_collection.delete_one({"listing_id": listing_id})
```

**Price History Archiving:**
```python
# When communities are archived, move price history too
price_doc["archived_at"] = datetime.now()
price_doc["archive_reason"] = "community archived"

await price_history_permanent_archived_collection.insert_one(price_doc)
await price_history_permanent_collection.delete_one({"permanent_property_id": id})
```

**Archive Features:**
- **Automatic Detection**: Missing listings identified daily
- **Safe Archiving**: >50% removal rate triggers safety check
- **Metadata Preservation**: Archive reason and timestamp recorded
- **Price History Sync**: Related price data archived together

#### Database Archiving Rule Enforcement

**CRITICAL RULE**: When `communitydata.listing_status = "archived"` → **AUTOMATICALLY** archive related `price_history_permanent` records

**Implementation:**
```python
# Triggered in two scenarios:
# 1. Immediately after Stage 2 community archiving
# 2. During daily price tracking runs

async def _handle_archived_communities(self):
    """DATABASE RULE: communitydata archived -> price_history_permanent archived"""
    
    # Find all archived communities
    archived_docs = await communitydata_collection.find({"listing_status": "archived"})
    
    for doc in archived_docs:
        for community in doc["community_data"]["communities"]:
            permanent_id = generate_permanent_id(community["community_id"])
            
            # Move price history to archive
            price_doc = await price_history_permanent_collection.find_one({"permanent_property_id": permanent_id})
            if price_doc:
                price_doc["archived_at"] = datetime.now()
                price_doc["archive_reason"] = "community archived"
                
                await price_history_permanent_archived_collection.insert_one(price_doc)
                await price_history_permanent_collection.delete_one({"permanent_property_id": permanent_id})
```

**Enforcement Points:**
- **Stage 2 Archiving**: Immediately after communities are archived
- **Daily Price Runs**: Automatic cleanup during price tracking
- **Atomic Operations**: Archive + delete operations are sequential
- **Audit Trail**: Archive metadata preserved with timestamp and reason

📊 **[View Detailed Architecture Diagram →](architecture/database_archiving_rule_diagram.md)**

#### Data Transformation for Masterplans
```json
{
  "masterplanlisting_id": "https://www.newhomesource.com/masterplan/...",  // Renamed from listing_id
  "masterplan_data": {                                                     // Renamed from property_data
    "price_range": "$699,852 - $854,989",                                 // Renamed from price
    "name": "Harvest at Limoneira",
    "url": "https://www.newhomesource.com/masterplan/...",
    "address": {
      "formatted_address": "Santa Paula, CA 93060",
      "county": "Ventura County"
    }
  },
  "data_source": "html_fallback",
  "listing_status": "active"
}
```

#### Data Transformation for Basic Communities
```json
{
  "basic_community_listing_id": "https://www.newhomesource.com/basiccommunity/...",  // Renamed from listing_id
  "basic_community_data": {                                                       // Renamed from property_data
    "name": "Basic Community Name",
    "url": "https://www.newhomesource.com/basiccommunity/...",
    "address": {
      "addressLocality": "City Name",
      "county": "County Name"
    }
  },
  "data_source": "json_ld",
  "listing_status": "active"
}
```

#### Benefits of Special Community Separation
- **Specialized Processing**: Special communities don't need community detail extraction
- **Data Structure Optimization**: Different schemas optimized for each community type
- **Performance**: Reduces Stage 2 workload by filtering out special communities
- **Clear Separation**: Distinct data types for better analytics

### Integration with Main Workflow

The routing happens automatically during full extraction:
1. **Stage 1** completes → All properties in `homepagedata`
2. **Router** analyzes → Special communities to respective collections, regulars continue
3. **Stage 2** processes → Only regular communities (excludes special types)
4. **Price Tracking** → Monitors only regular communities (special types isolated)

## 💰 Shared - Price Tracking System

**Goal**: Monitor and analyze price changes over time through permanent storage

### Price Tracking Architecture

The system uses a **single collection** for price data management:

#### 🏛️ `price_history_permanent` Collection - Long-term Analytics
**Purpose**: Permanent storage of complete property price histories **with running price ledger**
- **Data Type**: Consolidated property timelines with metadata
- **Lifecycle**: Created when property first seen → Preserved forever
- **Storage Pattern**: One document per unique community (by community_id)
- **Retention**: Permanent (never deleted)
- **Running Ledger**: **YES** - Every price change is appended to `price_timeline` array using `$push`

**Document Structure**:
```json
{
  "permanent_property_id": "md5-hash-of-community-id",
  "community_id": "https://newhomesource.com/plan/plan-4-spec/3015278_Plan_4",
  "accommodationCategory": "Single Family Residence",
  "offeredBy": "Shea Homes",
  "community_name": "Plan 4 - Spec Home",
  "listing_status": "active",
  "address": {
    "county": "Ventura County",
    "addressLocality": "Ventura",
    "addressRegion": "CA",
    "streetAddress": "10767 Bridgeport Walk",
    "postalCode": "93004"
  },
  "price_timeline": [
    {
      "date": "2024-01-15T10:30:00Z",
      "price": 850707,
      "source": "stage2",
      "change_type": "increase",
      "context": {"build_status": ["Move-In Ready"], "build_type": "spec"}
    },
    {
      "date": "2024-01-22T10:30:00Z",
      "price": 860000,
      "source": "stage2", 
      "change_type": "increase",
      "context": {"build_status": ["Move-In Ready"], "build_type": "spec"}
    }
  ],
  "aggregated_metrics": {
    "most_recent_price": 860000,
    "average_price": 855236,
    "min_price": 850707,
    "max_price": 860000,
    "total_days_tracked": 21,
    "moving_averages": {
      "1_day_average": 860000,
      "7_day_average": 855236,
      "30_day_average": 855236,
      "90_day_average": 855236,
      "180_day_average": 855236,
      "365_day_average": 855236
    },
    "percent_change_metrics": {
      "1_day_change": 0,
      "7_day_change": 0,
      "30_day_change": 0,
      "90_day_change": 0,
      "180_day_change": 0,
      "365_day_change": 0
    }
  },
  "created_at": "2025-09-06T02:33:37.966Z",
  "last_updated": "2025-09-06T02:33:37.966Z"
}
```

### Components

#### `price_tracker.py` - Price Snapshot Manager
- **Purpose**: Captures and manages price snapshots for both collections
- **Input**: Property data from Stages 1 & 2
- **Output**: Price history records in both collections
- **Key Functions**: 
  - `capture_price_snapshots_from_stage2()` - Creates daily snapshots
  - `consolidate_to_permanent_storage()` - Archives complete histories
  - `_update_permanent_timelines()` - Maintains permanent records

#### `price_history.py` - Legacy Price Capture (Stage 1)
- **Purpose**: Simple price capture during Stage 1 scraping
- **Input**: Stage 1 scraped documents
- **Output**: Basic price snapshots
- **Key Functions**: `save_scraped_prices()` - Direct price recording
- **Note**: This appears to be legacy code - main price tracking happens via `price_tracker.py`

### Community-Level Price Tracking Architecture

The price tracking system operates at the **individual community level**, not property level. Here's how it works:

#### 📍 Data Source: `communitydata` Collection
Each property listing page contains **multiple individual communities** (homes):
```json
{
  "listing_id": "https://newhomesource.com/community/sunset-village",
  "community_data": {
    "communities": [
      {
        "name": "Plan 4 - Spec Home",
        "price": "850707",
        "community_id": "https://newhomesource.com/specdetail/327-capistrano-dr/3015278_Plan_4",
        "build_status": ["Move-In Ready"],
        "build_type": "spec"
      },
      {
        "name": "Plan 1 - Ready to Build", 
        "price": "754119",
        "community_id": "https://newhomesource.com/plan/plan-1-shea-homes/3062878_Plan_1",
        "build_status": ["Ready to build"],
        "build_type": "plan"
      }
    ]
  }
}
```

#### 🎯 Individual Community Tracking Process

**Step 1: Community Extraction** (`stagetwo/html_parser.py`):
- Parses each housing card from property pages
- Extracts individual community data including prices
- Creates unique `community_id` for each community
- Generates structured data with price, build status, and type

**Step 2: Price Snapshot Creation** (`shared/price_tracker.py` lines 90-100):
```python
for doc in today_docs:
    communities = doc.get("community_data", {}).get("communities", [])
    for community in communities:  # ← Individual community processing
        snapshot = await self._create_price_snapshot(community, listing_id, doc)
```

**Step 3: Change Detection** (per community):
```python
# Check previous price for THIS specific community
permanent_record = await self.price_history_permanent_collection.find_one(
    {"permanent_property_id": permanent_id}  # ← Unique community tracking
)
```

### Execution Flow & Triggers

#### 🔄 When Price Tracking Executes

**Stage 2 Completion** (`stagetwo/orchestrator.py` line 92):
```python
await self.data_processor.capture_price_snapshots()
```
↓ Calls ↓
**Data Processor** (`stagetwo/data_processor.py` line 293):
```python
await price_tracker.capture_price_snapshots_from_stage2()
```
↓ Processes ↓
- **Each community individually** from `communitydata` collection
- **Updates timelines** in `price_history_permanent` (for each community with price changes)

**Property Archival** (Stage 1 archiving process):
```python
await price_tracker.consolidate_to_permanent_storage(listing_id)
```
↓ Results in ↓
- **Properties marked as archived** in `price_history_permanent`
- **Aggregated metrics** maintained per community

### 📊 Price Tracking Logging

The system provides comprehensive logging optimized for GitHub Actions workflows:

**Stage 2 Price Tracking Logging**:
```
💰 Starting Stage 2 price snapshot capture...
📊 Processing 23 community documents for price snapshots
🏠 Analyzed 47 individual communities from 23 properties
✅ Created 8 new price snapshots
💾 Updated 8 permanent timeline entries
✅ Stage 2 price tracking completed successfully
```

**When No Price Changes Detected**:
```
💰 Starting Stage 2 price snapshot capture...
📊 Processing 23 community documents for price snapshots
🏠 Analyzed 47 individual communities from 23 properties
ℹ️ No price changes detected - no new snapshots created
✅ Stage 2 price tracking completed successfully
```

**GitHub Actions Recognition**:
The scraper workflow (`.github/workflows/scraper.yml`) specifically looks for these logging patterns:
- `💰` symbols indicate price tracking activity
- `✅ Stage 2 price tracking completed successfully` confirms price tracking success
- Community-level metrics show granular analysis results

### Data Flow Lifecycle

1. **Property Discovery** (Stage 1) → Basic listing data
2. **Community Extraction** (Stage 2) → Detailed pricing captured
3. **Permanent Timeline Update** → `price_history_permanent` updated (if price changed)
4. **Property Archival** → Properties marked as archived in permanent storage

### Price Tracking Summary

| Aspect | `price_history_permanent` |
|--------|---------------------------|
| **Purpose** | Historical preservation with running ledger |
| **Trigger** | Community price changes only |
| **Granularity** | Complete community timelines |
| **Data Source** | `communitydata` collection |
| **Running Ledger** | **YES** (via `$push` to `price_timeline`) |
| **Tracking Level** | Individual community IDs (persistent) |
| **Retention** | Forever |
| **Data Volume** | Moderate (one timeline per community) |
| **Use Case** | Long-term trend analysis |

### Performance Optimizations

- **Smart Change Detection**: Only creates snapshots when prices actually change
- **Automatic Indexing**: Optimized indexes for permanent collection
- **Archive Management**: Properties automatically marked as archived when removed
- **Permanent ID System**: Properties tracked consistently across URL changes

## 🔧 Configuration

### `scraper_config.json`
Controls which locations to scrape:
```json
{
  "newhomesource": {
    "base_url": "https://www.newhomesource.com/communities",
    "locations": [
      {
        "state": "ca",
        "area_region": "ventura-area", 
        "specific_location": "ventura-county",
        "display_name": "Ventura County, CA"
      }
    ],
    "pagination": {
      "start_page": 1,
      "end_page": 9
    }
  }
}
```

### Environment Variables
```bash
MONGO_DB_URI=mongodb://localhost:27017/newhomesource
```

## 📊 Data Validation

The system includes comprehensive validation to ensure data quality:

### Stage 1 Validation (`validation/stage_one_structure_validation.py`)
- Validates JSON-LD structured data
- Ensures HTML fallback data completeness
- Verifies required fields and data types
- Handles edge cases in pricing data

### Stage 2 Validation (`validation/stage_two_structure_validation.py`)  
- Validates community data structure
- Ensures pricing information accuracy
- Verifies community metadata completeness

## 🎮 Advanced Features

### Smart Change Detection
- Only updates records that actually changed
- Tracks what specifically changed (price, status, details)
- Maintains historical versions of data

### Robust Error Handling
- Automatic retries with exponential backoff
- Multiple parsing strategies as fallbacks
- Comprehensive logging and error reporting

### Performance Optimization
- Concurrent request processing
- Browser impersonation to avoid blocking
- Intelligent request throttling
- Database indexing for fast queries

### Price Intelligence
- Automatic price change detection
- Historical trend analysis
- Market-wide price monitoring
- Snapshot-based tracking system

## 📈 Output Examples

### Stage 1 Success
```
🚀 Starting Stage 1 Listing Extraction...
🔗 Generated 45 URLs to scrape
✅ Found listing: sunset-villa-123 (source: JSON-LD)
✅ Found listing: oak-manor-456 (source: HTML)
📊 Processed 45/45 URLs: 127 listings found
🎉 Stage 1 completed successfully!
```

### Stage 2 Success
```
🚀 Starting Stage 2 Community Extraction...
✅ Found 127 properties to process
📍 Processing sunset-village-url: 8 communities (new)
📊 STAGE 2 CHANGE SUMMARY:
   🆕 New listings: 12
   🔄 Updated listings: 8  
   🏠 New communities: 45
   💰 Price snapshots captured
```

### Full Extraction Summary
```
🎊 FULL EXTRACTION SUMMARY
📊 Stage 1 Status: ✅ SUCCESS
📊 Stage 2 Status: ✅ SUCCESS
🎉 Complete data extraction successful!
💰 Price tracking is now active and historical data preserved
```

## 🚨 Important Notes

### Database Requirements
- MongoDB with sufficient storage for large datasets
- Proper indexing for performance (automatically created)
- Regular backup strategy recommended

### Rate Limiting
- Built-in delays between requests to respect website resources
- Configurable concurrency limits
- Browser rotation to distribute load

### Data Retention
- Archived listings maintained for historical analysis  
- Price history preserved permanently
- Temporary processing data cleaned automatically

## 🛠️ Troubleshooting

### Common Issues

**No data found**: Check `scraper_config.json` locations are valid
**MongoDB connection errors**: Verify `MONGO_DB_URI` in `.env` file
**High error rates**: Reduce `--max-concurrent` parameter
**Missing price data**: Price information may not be available for all properties

### Log Analysis
- Detailed logs in `logging/nhs/scraper_log_YYYYMMDD_HHMMSS.log`
- Error patterns indicate specific issues
- Progress tracking shows completion status

## 🎯 Success Metrics

A successful database build includes:
- ✅ All configured locations scraped
- ✅ Property listings extracted and validated
- ✅ Community details captured
- ✅ Price tracking data current
- ✅ Change detection working
- ✅ Minimal error rates (<5%)

---

**This system creates a comprehensive, continuously updated database of new home market data, enabling powerful analysis of pricing trends, inventory changes, and market dynamics.** 🏡📊