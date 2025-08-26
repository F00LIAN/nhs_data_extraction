# 🏘️ Stage Two - The Detail Detective

> **What does Stage 2 do?** After Stage 1 finds all the houses, Stage 2 is like a detective who visits each house to get detailed information about the neighborhood and community!

## 🎯 Stage 2's Main Job

**Goal:** Take the house list from Stage 1 and visit each house's detailed page to learn about the community

**Like:** After you have a list of all the toy stores in town, Stage 2 goes into each store to find out what specific toys they have, what they cost, and what's special about each store.

## 🧩 The Team of Detective Robots

Stage 2 has 5 specialized detective robots:

### 🎭 `orchestrator.py` - The Head Detective
**What it does:** Manages the whole investigation and coordinates all other detectives

**Like:** A police captain who assigns cases to different detectives and makes sure all the investigations get completed.

**Key Jobs:**
- Gets the house list from Stage 1
- Assigns each house to be investigated
- Tracks progress and statistics
- Handles failed investigations with retries
- Creates final reports

### 📋 `data_fetcher.py` - The Case File Collector  
**What it does:** Gets the list of houses from Stage 1's database

**Like:** The assistant who brings the detective all the case files to work on.

**Key Jobs:**
- Connects to the database
- Retrieves house URLs from Stage 1
- Organizes house information
- Prepares data for other detectives

**Example:**
```
Fetches data like:
House ID: abc123
URL: newhomesource.com/community/sunset-village
Listing ID: sunset-village-homes
```

### 🌐 `http_client.py` - The Website Detective
**What it does:** Visits each house's detailed webpage to get the information

**Like:** A detective who goes to each location to gather evidence and take photos.

**Key Jobs:**
- Visits house detail pages
- Downloads webpage content
- Uses proxy servers to avoid being blocked
- Can switch between different "disguises" (browsers)
- Retries failed visits automatically

### 📖 `html_parser.py` - The Evidence Analyzer
**What it does:** Reads the webpage content and extracts community information

**Like:** A detective who looks at all the evidence and writes down the important clues.

**Key Jobs:**
- Finds community information on webpages
- Extracts neighborhood details
- Identifies different house types (spec homes, plan homes)
- Organizes information into structured records
- Handles different webpage formats

**Example of what it finds:**
```
Community: "Sunset Village"
Houses Available: 12
Price Range: $300k - $450k
Build Status: "Now Selling"
House Types: ["Plan A", "Plan B", "Spec Home #3"]
```

### 💾 `data_processor.py` - The Records Detective
**What it does:** Compares new information with old information and saves updates

**Like:** A detective who compares today's evidence with yesterday's to see what changed.

**Key Jobs:**
- Compares new data with existing database records
- Detects what changed since last investigation
- Saves new community information
- Archives communities that no longer exist
- Tracks price changes
- Calls the price tracking system

## 🔄 How The Detective Team Works

1. **Head Detective** starts the investigation
2. **Case File Collector** gets list of houses to investigate  
3. **Website Detective** visits each house's detailed page
4. **Evidence Analyzer** extracts community details from each page
5. **Records Detective** compares with old records and saves updates
6. **Head Detective** writes final investigation report

## 📊 What Stage 2 Produces

### 📋 **communitydata** collection (in database)
- Detailed information about each community/neighborhood
- List of available houses in each community
- Build statuses and house types
- Change tracking (what's new, updated, or removed)

### 📈 **Price snapshots** 
- Current prices for all houses
- Price change detection
- Historical price tracking

### 📝 **Investigation reports**
- Statistics about communities found
- Change summaries (new/updated/removed)
- Error reports for failed investigations

## 🎮 Cool Features

- **Smart Change Detection** → Only updates communities that actually changed
- **Browser Rotation** → Uses different browser "disguises" to avoid being blocked
- **Automatic Retries** → If an investigation fails, tries again with different methods
- **Price Tracking Integration** → Automatically tracks price changes
- **Progress Monitoring** → Shows detailed progress as investigations proceed

## 🚨 Important Database Collections

- **Input:** `homepagedata` (from Stage 1)
- **Output:** `communitydata` (detailed community info)
- **Temporary:** `temphtml` (temporary webpage storage)
- **Tracking:** `pricehistory` (price snapshots)

## 📈 Example Output

When Stage 2 runs, you see messages like:
```
🔗 Fetching property data from Stage 1...
✅ Found 150 properties to process
📍 Processing 1/150: sunset-village-url
✅ Processed sunset-village-url: 8 communities (new)
🔄 Retrying 3 failed URLs...
📊 STAGE 2 CHANGE SUMMARY:
   🆕 New listings: 12
   🔄 Updated listings: 8
   🏠 New communities: 45
   💰 Price snapshots captured
```

## 🎯 Success Criteria

Stage 2 is successful when:
- All houses from Stage 1 were investigated
- Community details were extracted and saved
- Price tracking data was captured
- Database shows updated community information
- Investigation report shows minimal failures

## 🔄 Change Detection Magic

Stage 2 is smart about changes:
- **New Community** → "This is a brand new neighborhood!"
- **Updated Community** → "House prices changed in this neighborhood"
- **Removed Community** → "This neighborhood is no longer selling houses"
- **Unchanged Community** → "Nothing changed here, just updating timestamp"

---
*Stage 2 is like having a team of super-detailed detectives who can investigate hundreds of communities and notice even the smallest changes!* 🔍🏘️
