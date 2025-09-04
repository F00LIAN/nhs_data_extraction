# 🏠 NewHomeSource Database Builder - Complete Scraper System

A comprehensive data extraction system that builds a complete database of new home listings, community details, and price history from NewHomeSource.com.

## 🎯 System Overview

This scraper system is designed to create and maintain a comprehensive database of new home sales data through a **two-stage extraction process** plus **intelligent price tracking**:

1. **Stage 1**: Discovers and catalogs all property listings
2. **Stage 2**: Extracts detailed community and pricing information  
3. **Price Tracking**: Monitors and tracks price changes over time

## 🗄️ Database Architecture

The system builds a MongoDB database with five main collections:

| Collection | Purpose | Data Source |
|------------|---------|-------------|
| `homepagedata` | Basic property listings and metadata | Stage 1 |
| `communitydata` | Detailed community and neighborhood info | Stage 2 |
| `pricehistory` | Daily price snapshots | Price Tracker |
| `price_history_permanent` | Long-term price trend analysis | Price Tracker |
| `archivedlistings` | Historical record of removed properties | Stage 1 |

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
│   └── price_history.py             ← Analyzes price trends
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
- **`pricehistory`**: Price snapshots from extraction

## 💰 Shared - Price Tracking System

**Goal**: Monitor and analyze price changes over time through a dual-collection approach

### Price Tracking Architecture

The system uses **two distinct collections** to manage price data efficiently:

#### 📊 `pricehistory` Collection - Active Snapshots
**Purpose**: Real-time price change tracking for active properties
- **Data Type**: Individual price snapshots with change detection
- **Lifecycle**: Active while property is listed → Archived when property removed
- **Storage Pattern**: One document per price change event
- **Retention**: Cleaned up after 365 days (configurable)

**Document Structure**:
```json
{
  "listing_id": "property-url",
  "community_id": "community-identifier", 
  "property_name": "Sunset Villa",
  "price": 325000,
  "snapshot_date": "2024-01-15T10:30:00Z",
  "change_metrics": {
    "previous_price": 320000,
    "change_amount": 5000,
    "change_percentage": 1.56,
    "is_significant": false
  },
  "build_status": ["Now Selling", "Move-In Ready"],
  "is_archived": false
}
```

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
  "original_listing_id": "property-url", // How we store this already in homepagedata collection and communitydata collection
  "community_id": "https://newhomesource.com/plan/plan-4-spec/3015278_Plan_4",
  "community_name": "Plan 4 - Spec Home",
  "property_snapshot": {
    "name": "Sunset Village",
    "location": {
      "county": "Ventura County",
      "city": "Oxnard",
      "coordinates": {"latitude": 34.2, "longitude": -119.2}
    },
    "status": "active|archived"
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
    },
    {
      "date": "2024-02-05T10:30:00Z",
      "price": 855000,
      "source": "stage2",
      "change_type": "decrease", 
      "context": {"build_status": ["Move-In Ready"], "build_type": "spec"}
    }
  ],
  "aggregated_metrics": {
    "min_price": 850707,
    "max_price": 860000,
    "avg_price": 855236,
    "total_days_tracked": 21,
    "volatility_score": 0.54
  }
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
last_snapshot = await self.pricehistory_collection.find_one(
    {"community_id": community_id},  # ← Unique community tracking
    sort=[("snapshot_date", -1)]
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
- **Creates snapshots** in `pricehistory` (only when community prices change)
- **Updates timelines** in `price_history_permanent` (for each community with price changes)

**Property Archival** (Stage 1 archiving process):
```python
await price_tracker.consolidate_to_permanent_storage(listing_id)
```
↓ Results in ↓
- **Complete timeline** for each community moved to `price_history_permanent`
- **Archived snapshots** marked in `pricehistory`
- **Aggregated metrics** calculated per community

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
3. **Price Snapshot Creation** → `pricehistory` updated (if price changed)
4. **Permanent Timeline Update** → `price_history_permanent` always updated
5. **Property Archival** → Complete history consolidated to permanent storage
6. **Cleanup** → Old `pricehistory` records cleaned after 365 days

### Key Differences Summary

| Aspect | `pricehistory` | `price_history_permanent` |
|--------|----------------|---------------------------|
| **Purpose** | Active change tracking | Historical preservation with running ledger |
| **Trigger** | Community price changes only | Every community price change |
| **Granularity** | Individual community snapshots | Complete community timelines |
| **Data Source** | `communitydata` collection | `pricehistory` snapshots + metadata |
| **Running Ledger** | No (snapshots only) | **YES** (via `$push` to `price_timeline`) |
| **Tracking Level** | Individual community IDs | Individual community IDs (persistent) |
| **Retention** | 1 year (configurable) | Forever |
| **Data Volume** | High (many snapshots) | Moderate (one timeline per community) |
| **Use Case** | Daily monitoring alerts | Long-term trend analysis |

### Performance Optimizations

- **Smart Change Detection**: Only creates snapshots when prices actually change
- **Automatic Indexing**: Optimized indexes for both collections
- **Cleanup Automation**: Old snapshots automatically purged
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