# 🏠 Stage One - The House Finder

> **What does Stage 1 do?** It's like sending a scout to explore every street in a big city to make a list of ALL the houses that are for sale!

## 🎯 Stage 1's Main Job

**Goal:** Find every single house for sale on the website and make a big list

**Like:** Imagine you're collecting Pokemon cards and you want to know every card that exists in every store in town. Stage 1 goes to every store and makes a list: "Store A has 5 cards, Store B has 12 cards, Store C has 3 cards..."

## 🧩 The Team of Helper Robots

Stage 1 has 5 different helper robots, each with a special job:

### 🎭 `scraping_orchestrator.py` - The Boss Robot
**What it does:** Tells all the other robots what to do and when

**Like:** The teacher in your classroom who organizes activities and makes sure everyone is doing their job.

**Key Jobs:**
- Decides which websites to visit
- Tells other robots when to start working
- Keeps track of progress
- Writes reports about what was found

### 🔗 `url_generator.py` - The Address Maker
**What it does:** Creates a list of all website addresses to visit

**Like:** Making a list of every house address in your neighborhood before you go trick-or-treating.

**Key Jobs:**
- Reads the settings file (`scraper_config.json`)
- Creates URLs for different cities and areas
- Makes sure we don't miss any locations
- Organizes addresses by region

**Example:**
```
Creates addresses like:
- newhomesource.com/california/los-angeles/page-1
- newhomesource.com/california/los-angeles/page-2  
- newhomesource.com/texas/houston/page-1
```

### 🌐 `http_fetcher.py` - The Website Visitor
**What it does:** Actually visits the websites and downloads the information

**Like:** A friend who goes to different stores and brings back their shopping catalogs.

**Key Jobs:**
- Visits each website address
- Downloads the webpage content
- Handles errors if websites are slow
- Can pretend to be different web browsers
- Retries if something goes wrong

### 📖 `listing_parser.py` - The Information Reader
**What it does:** Reads the webpage content and finds house information

**Like:** Looking through a newspaper and circling all the "For Sale" ads, then writing down the important details.

**Key Jobs:**
- Finds house listings on webpages
- Extracts house names, prices, and URLs
- Creates organized house records
- Handles different webpage formats
- Generates unique house IDs

**Example of what it finds:**
```
House: "Sunset Villa"
Price: $325,000
URL: newhomesource.com/house/sunset-villa
Builder: "ABC Homes"
```

### 💾 `database_manager.py` - The Record Keeper
**What it does:** Saves all the house information in the database

**Like:** A librarian who organizes all the books and makes sure they're in the right place.

**Key Jobs:**
- Saves new house listings to database
- Updates existing house information
- Archives houses that are no longer for sale
- Prevents duplicate entries
- Tracks what changed since last time

## 🔄 How They Work Together

1. **Boss Robot** starts the process
2. **Address Maker** creates list of websites to visit
3. **Website Visitor** goes to each website and gets the content
4. **Information Reader** finds house details from each webpage
5. **Record Keeper** saves everything to the database
6. **Boss Robot** makes sure everything finished correctly

## 📊 What Stage 1 Produces

At the end, Stage 1 creates:

### 📋 **homepagedata** collection (in database)
- List of ALL houses found
- Basic information about each house
- Website URLs where more details can be found

### 📦 **archivedlistings** collection
- Houses that used to be for sale but aren't anymore
- Keeps historical records

### 📝 **Log files**
- Detailed reports of what happened
- Error messages if anything went wrong
- Statistics about how many houses were found

## 🎮 Cool Features

- **Smart Retries** → If a website is slow, it tries again
- **Change Detection** → Only updates houses that actually changed
- **Multi-Browser Support** → Can pretend to be Chrome, Firefox, Safari, etc.
- **Progress Tracking** → Shows how much work is left
- **Error Handling** → Continues working even if some websites fail

## 🚨 Important Files

- **scraper_config.json** → Settings for which cities to search
- **Log files** → Reports of what happened during scraping

## 📈 Example Output

When Stage 1 runs, you see messages like:
```
🔗 Generating URLs...
✅ Generated 45 URLs to scrape
🏃‍♂️ Starting Stage 1 Data Extraction...
✅ Found listing: sunset-villa-123 (source: JSON-LD)
✅ Found listing: oak-manor-456 (source: HTML)
📊 Processed 1/45 URLs: 12 listings found
🎉 Stage 1 completed successfully!
```

## 🎯 Success Criteria

Stage 1 is successful when:
- All planned websites were visited
- House information was extracted and saved
- Database has updated house listings
- Log shows no critical errors

---
*Stage 1 is like having a super-fast assistant who can visit hundreds of house websites in minutes and organize all the information perfectly!* 🏡📋
