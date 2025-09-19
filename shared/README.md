# City Price Snapshot Calculation System

This document provides a detailed explanation of how city price snapshots are calculated in the New Home Source price tracking system. The calculations are performed by the `PriceTracker` class in `price_tracker.py`.

## Overview

City price snapshots aggregate property pricing data at the city level, providing insights into market trends for Single Family Residences (SFR), Condominiums, and overall property markets. The system maintains both real-time current metrics and historical daily averages.

## Data Flow Architecture

### 1. Source Data
- **Input**: `price_history_permanent` collection
- **Properties**: Both active and archived properties with their complete price timelines
- **Grouping**: Properties are grouped by city (addressLocality), county, and addressRegion

### 2. MongoDB Aggregation Pipeline

The city snapshot process begins with a MongoDB aggregation pipeline:

```javascript
pipeline = [
    {
        "$group": {
            "_id": {
                "city": "$address.addressLocality",
                "county": "$address.county", 
                "addressRegion": "$address.addressRegion"
            },
            "properties": {
                "$push": {
                    "accommodation_category": "$accommodationCategory",
                    "listing_status": "$listing_status",
                    "current_price": "$aggregated_metrics.most_recent_price",
                    "price_timeline": "$price_timeline"
                }
            }
        }
    }
]
```

**What this does:**
- Groups all properties by their city/county/region combination
- Creates a unique city identifier from the geographic tuple
- Collects all property data including price timelines for each city group

## Current Active Metrics Calculation

### Step 1: Property Filtering and Categorization

For each city, properties are filtered and categorized:

```python
# Active properties only (for current metrics)
active_properties = [p for p in properties if p["listing_status"] == "active"]

# Separate by property type
active_sfr = [p for p in active_properties if p["accommodation_category"] == "Single Family Residence"]
active_condo = [p for p in active_properties if p["accommodation_category"] == "Condominium"]
```

### Step 2: Current Price Calculations

**Counts:**
- `sfr_count = len(active_sfr)`
- `condo_count = len(active_condo)`
- `total_active = len(active_properties)`

**Average Prices:**
- `sfr_avg_price = sum(p["current_price"] for p in active_sfr) / len(active_sfr)` (if any SFR exist)
- `condo_avg_price = sum(p["current_price"] for p in active_condo) / len(active_condo)` (if any condos exist)
- `overall_avg_price = sum(p["current_price"] for p in active_properties) / len(active_properties)` (if any properties exist)

## Historical Daily Averages Calculation

### Process Overview

The `_calculate_historical_daily_averages()` method reconstructs historical market conditions by analyzing all property price timelines and determining what the market looked like on each historical date.

### Step 1: Date Collection

```python
# Collect all unique dates from all property timelines
all_dates = set()
for prop in properties:
    timeline = prop.get("price_timeline", [])
    for entry in timeline:
        date = entry.get("date")
        if date:
            date_str = date.date().isoformat()  # Convert to YYYY-MM-DD format
            all_dates.add(date_str)
```

### Step 2: Daily Market Reconstruction

For each historical date, the system determines which properties were "active" (had a price) on that date:

```python
for date_str in all_dates:
    target_date = datetime.fromisoformat(date_str).date()
    
    daily_data[date_str] = {
        "sfr": {"prices": [], "properties": set()}, 
        "condo": {"prices": [], "properties": set()}, 
        "all": {"prices": [], "properties": set()}
    }
```

### Step 3: Property State Determination

For each property and each date, find the most recent price entry on or before that date:

```python
# Find the most recent price entry for this property on or before target_date
valid_entries = []
for entry in timeline:
    entry_date_obj = parse_date(entry.get("date"))
    
    # Only include entries on or before target date
    if entry_date_obj <= target_date:
        valid_entries.append((entry_date_obj, entry.get("price")))

# If property has any valid entries, it was active on this date
if valid_entries:
    # Get the most recent price (latest date)
    valid_entries.sort(key=lambda x: x[0], reverse=True)
    latest_price = valid_entries[0][1]
```

### Step 4: Daily Aggregation

```python
# Calculate daily averages with accurate listing counts
for date_str in sorted(daily_data.keys()):
    day_data = daily_data[date_str]
    
    sfr_prices = day_data["sfr"]["prices"]
    condo_prices = day_data["condo"]["prices"] 
    all_prices = day_data["all"]["prices"]
    
    daily_averages.append({
        "date": date_str,
        "sfr_avg_price": round(sum(sfr_prices) / len(sfr_prices), 2) if sfr_prices else None,
        "sfr_listing_count": len(day_data["sfr"]["properties"]),
        "condo_avg_price": round(sum(condo_prices) / len(condo_prices), 2) if condo_prices else None,
        "condo_listing_count": len(day_data["condo"]["properties"]),
        "overall_avg_price": round(sum(all_prices) / len(all_prices), 2) if all_prices else None,
        "overall_listing_count": len(day_data["all"]["properties"])
    })
```

**Key Insight:** This method preserves the actual market state on each historical date, not just when prices changed. A property that had a price entry on Day 1 is considered "active" on Days 1, 2, 3, etc., until either:
- A new price entry updates its price
- The property is archived/removed

## Historical Count Preservation

### The Challenge

When city snapshots are recalculated, we want to:
- ✅ Update historical prices (as property timelines may be corrected)
- ❌ NOT overwrite historical listing counts (these reflect the actual market size when the snapshot was taken)

### The Solution: `_preserve_historical_listing_counts()`

```python
# Create a lookup of existing counts by date
existing_counts_by_date = {}
for entry in existing_historical:
    date = entry.get("date")
    if date:
        existing_counts_by_date[date] = {
            "sfr_listing_count": entry.get("sfr_listing_count"),
            "condo_listing_count": entry.get("condo_listing_count"), 
            "overall_listing_count": entry.get("overall_listing_count")
        }

# Merge calculated historical (prices) with preserved counts
for calc_entry in calculated_historical:
    date = calc_entry.get("date")
    
    # If we have existing counts for this date, preserve them
    if date in existing_counts_by_date:
        preserved_entry = calc_entry.copy()
        # Preserve the original listing counts from when snapshot was taken
        preserved_counts = existing_counts_by_date[date]
        preserved_entry["sfr_listing_count"] = preserved_counts["sfr_listing_count"]
        preserved_entry["condo_listing_count"] = preserved_counts["condo_listing_count"]
        preserved_entry["overall_listing_count"] = preserved_counts["overall_listing_count"]
```

**Result:** Historical listing counts remain accurate to the actual market conditions when they were recorded, while prices can be updated if corrections are made to property timelines.

## Moving Averages and Percentage Change Calculations

### Timeframe Configuration

The system supports configurable timeframes:

```python
# Default timeframes
DEFAULT_MOVING_AVG_TIMEFRAMES = [7, 30, 90]
DEFAULT_PERCENT_CHANGE_TIMEFRAMES = [1, 7, 30, 90]

# Extended timeframes (for future expansion)
EXTENDED_TIMEFRAMES = [1, 3, 7, 14, 30, 60, 90, 180, 365]
```

### Moving Average Calculation

Moving averages are calculated from the most recent N days of historical daily averages:

```python
for days in moving_avg_timeframes:
    if len(price_data) >= days:
        # Use exactly N days of recent data
        recent_prices = [entry["price"] for entry in price_data[-days:]]
        avg = sum(recent_prices) / len(recent_prices)
        moving_averages[f"{days}_day_average"] = round(avg, 2)
    else:
        # Use all available data if we don't have enough days
        all_prices = [entry["price"] for entry in price_data]
        if all_prices:
            avg = sum(all_prices) / len(all_prices)
            moving_averages[f"{days}_day_average"] = round(avg, 2)
```

**Example:**
- 7-day moving average = average of the last 7 daily prices
- 30-day moving average = average of the last 30 daily prices

### Percentage Change Calculation

Percentage changes compare current price to the price N days ago:

```python
current_price = price_data[-1]["price"]  # Most recent price

for days in percent_change_timeframes:
    # Need at least (days + 1) data points to compare
    required_points = days + 1
    if len(price_data) >= required_points:
        past_price = price_data[-(days + 1)]["price"]
        if past_price > 0:
            change = ((current_price - past_price) / past_price * 100)
            percent_changes[f"{days}_day_change"] = round(change, 2)
```

**Example:**
- 7-day change = ((today_price - price_7_days_ago) / price_7_days_ago) × 100
- 30-day change = ((today_price - price_30_days_ago) / price_30_days_ago) × 100

**Data Requirements:**
- For N-day percentage change, need at least (N+1) data points
- For 7-day change, need 8 data points (today + 7 days ago)

## Final City Snapshot Structure

The completed city snapshot document contains:

```python
city_snapshot = {
    "city_id": city_id,  # Generated from city_county_region hash
    "addressLocality": city_info["city"],
    "county": city_info["county"], 
    "addressRegion": city_info["addressRegion"],
    "current_active_metrics": {
        "sfr": {
            "count": sfr_count,                    # Current active SFR count
            "avg_price": sfr_avg_price,           # Current SFR average price
            "moving_averages": sfr_metrics["moving_averages"],    # 7, 30, 90-day averages
            "percent_changes": sfr_metrics["percent_changes"]     # 1, 7, 30, 90-day changes
        },
        "condo": {
            "count": condo_count,                 # Current active condo count
            "avg_price": condo_avg_price,         # Current condo average price
            "moving_averages": condo_metrics["moving_averages"],  # 7, 30, 90-day averages
            "percent_changes": condo_metrics["percent_changes"]   # 1, 7, 30, 90-day changes
        },
        "overall": {
            "total_properties": len(active_properties),           # Total active properties
            "avg_price": overall_avg_price,       # Overall average price
            "moving_averages": overall_metrics["moving_averages"], # 7, 30, 90-day averages
            "percent_changes": overall_metrics["percent_changes"]  # 1, 7, 30, 90-day changes
        }
    },
    "historical_daily_averages": historical_daily_averages,  # Last 30 days of daily data
    "last_snapshot_date": datetime.now(),
    "created_at": datetime.now()
}
```

## Key Features and Design Decisions

### 1. **Accurate Historical Reconstruction**
- Properties are considered "active" on a date if they had any price entry on or before that date
- This preserves the actual market composition on each historical date

### 2. **Count Preservation**
- Historical listing counts reflect the actual market size when snapshots were taken
- Prices can be updated without losing historical market size data

### 3. **Property Type Separation**
- All calculations are performed separately for SFR, Condominiums, and Overall market
- This provides market-specific insights

### 4. **Configurable Timeframes**
- Default timeframes: [7, 30, 90] days for moving averages, [1, 7, 30, 90] for percentage changes
- Extended timeframes available for future expansion: [1, 3, 7, 14, 30, 60, 90, 180, 365]

### 5. **Data Retention**
- Historical daily averages: Last 30 days
- Permanent storage: All property price timelines preserved indefinitely

### 6. **Error Handling**
- Graceful handling of missing data (returns None for unavailable metrics)
- Fallback calculations when insufficient historical data exists

## Usage in the System

City price snapshots are created:
1. **After Stage 2 completion** - When new property data is processed
2. **Following permanent storage updates** - When individual property price histories are updated
3. **On-demand** - Through direct calls to `_create_city_price_snapshots()`

The snapshots are stored in the `price_city_snapshot` collection and are used by the frontend dashboard to display city-level market trends and analytics.
