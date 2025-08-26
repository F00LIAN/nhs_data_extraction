# 🏠 NewHomeSource Scraper

> **What does this do?** This is like a smart robot that visits websites to collect information about new houses for sale, just like how you might look through different stores to find the best toys!

## 🎯 What This Scraper Does

This scraper is like a **house-hunting assistant** that:

1. **Visits house websites** (like NewHomeSource.com)
2. **Collects information** about houses for sale
3. **Organizes the data** in a neat database
4. **Tracks price changes** over time
5. **Finds detailed information** about each neighborhood

## 🏗️ How It's Organized

Think of this like organizing your toys into different boxes:

```
scraper/
├── 📦 run_nhs.py          ← The MAIN controller (like the boss of all robots)
├── 📁 stageone/           ← Stage 1: Find all the houses
├── 📁 stagetwo/           ← Stage 2: Get details about each house  
├── 📁 shared/             ← Tools that both stages use
├── 📁 test/               ← Check if everything works correctly
└── 📄 scraper_config.json ← Settings file (like instructions)
```

## 🚀 How to Use It

### Simple Commands (like giving instructions to your robot):

```bash
# Get ALL house information (most common)
python run_nhs.py --stage full

# Only find house listings (Stage 1)
python run_nhs.py --stage 1

# Only get detailed house info (Stage 2) 
python run_nhs.py --stage 2

# Make it work faster (more robots working at once)
python run_nhs.py --max-concurrent 15

# See lots of details while it's working
python run_nhs.py --verbose
```

## 🎪 The Two Main Jobs

### 🏠 Stage 1: House Finder
- **What it does:** Goes to websites and finds ALL the houses for sale
- **Like:** Walking through every street in a neighborhood to count all the houses
- **Result:** Makes a big list of house addresses

### 🏘️ Stage 2: Detail Collector  
- **What it does:** Visits each house from Stage 1 to get detailed information
- **Like:** Knocking on each door to ask about the house (rooms, price, etc.)
- **Result:** Detailed information about each house and neighborhood

## 💾 Where Information Goes

All the house information gets saved in **MongoDB** (like a giant digital filing cabinet):

- **homepagedata** → Basic house listings
- **communitydata** → Detailed neighborhood information  
- **pricehistory** → How prices change over time
- **archivedlistings** → Houses that are no longer for sale

## 📋 What You Need

Before running the scraper:

1. **MongoDB** → Database to store information
2. **Python packages** → Install with `pip install -r requirements.txt`
3. **Environment file** → Create `.env` file with `MONGO_DB_URI=your_database_connection`

## 🎮 Cool Features

- **Smart Retries** → If something fails, it tries again automatically
- **Browser Switching** → Can pretend to be different web browsers
- **Price Tracking** → Remembers how house prices change over time
- **Change Detection** → Only updates information that actually changed
- **Progress Reports** → Shows you what it's doing while it works

## 🛠️ For Developers

Each folder has its own README with more technical details:
- [stageone/README.md](stageone/README.md) - Stage 1 components
- [stagetwo/README.md](stagetwo/README.md) - Stage 2 components  
- [shared/README.md](shared/README.md) - Common utilities

## 🚨 Important Files

- **run_nhs.py** → Main program (start here!)
- **scraper_config.json** → Settings for what to scrape
- **requirements.txt** → List of needed Python packages

## 🎉 Example Output

When it's working, you'll see messages like:
```
🚀 Starting Stage 1 Listing Extraction...
✅ Found 150 properties to process
🏃‍♂️ Starting Stage 2 Community Extraction...  
✅ Community data and price snapshots captured
🎉 Complete data extraction successful!
```

---
*This scraper helps people find and track house information automatically, making house hunting much easier!* 🏡
