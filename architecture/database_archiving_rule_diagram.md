# Database Archiving Rule Architecture

## 🔄 Automatic Price History Archiving Rule

**RULE**: `communitydata.listing_status = "archived"` → **AUTOMATICALLY** move `price_history_permanent` → `price_history_permanent_archived`

## 📊 Architecture Diagram

```mermaid
graph TD
    %% Stage 2 Processing
    A[Stage 2: Community Processing] --> B{Missing Communities Detected?}
    B -->|Yes| C[Move to communitydata_archived]
    B -->|No| D[Continue Normal Processing]
    
    %% Immediate Archiving Trigger
    C --> E[🔄 TRIGGER: _trigger_price_history_archiving()]
    E --> F[Call PriceTracker._handle_archived_communities()]
    
    %% Daily Price Tracking
    G[Daily Price Tracking Run] --> H[capture_price_snapshots_from_stage2()]
    H --> I[🔄 AUTOMATIC: _handle_archived_communities()]
    
    %% Archiving Logic
    F --> J[Find Archived Communities]
    I --> J
    J --> K{archived communities found?}
    K -->|Yes| L[For each archived community]
    K -->|No| M[✅ No archiving needed]
    
    %% Individual Community Processing
    L --> N[Extract community_id from communities array]
    N --> O[Generate permanent_id = md5(community_id)]
    O --> P{price_history_permanent record exists?}
    P -->|Yes| Q[Add archive metadata]
    P -->|No| R[Skip - no price history]
    
    %% Archive Operation
    Q --> S[Insert into price_history_permanent_archived]
    S --> T[Delete from price_history_permanent]
    T --> U[📦 Log: ARCHIVED community_id]
    U --> V{More communities?}
    V -->|Yes| N
    V -->|No| W[✅ All price histories archived]
    
    %% Collections
    subgraph "Active Collections"
        X[(communitydata)]
        Y[(price_history_permanent)]
    end
    
    subgraph "Archive Collections"
        Z[(communitydata_archived)]
        AA[(price_history_permanent_archived)]
    end
    
    %% Data Flow
    C -.-> Z
    S -.-> AA
    T -.-> Y
    
    %% Styling
    classDef triggerNode fill:#ff9999,stroke:#ff0000,stroke-width:2px
    classDef archiveNode fill:#99ccff,stroke:#0066cc,stroke-width:2px
    classDef collectionNode fill:#99ff99,stroke:#00cc00,stroke-width:2px
    
    class E,F,I triggerNode
    class S,T,AA,Z archiveNode
    class X,Y,Z,AA collectionNode
```

## 🔧 Implementation Details

### **Enforcement Points**

1. **Immediate Enforcement** (Stage 2):
   ```python
   # In stagetwo/data_processor.py
   async def handle_removed_listings():
       # Archive communities to communitydata_archived
       # ...
       
       # IMMEDIATE: Trigger price history archiving
       if removed_listing_ids:
           await self._trigger_price_history_archiving()
   ```

2. **Scheduled Enforcement** (Daily):
   ```python
   # In shared/price_tracker.py
   async def capture_price_snapshots_from_stage2():
       # Handle archived communities automatically
       await self._handle_archived_communities()
       # ... continue with price tracking
   ```

### **Data Transformation**

```json
// Before Archiving (price_history_permanent)
{
  "permanent_property_id": "abc123...",
  "community_id": "https://...",
  "price_timeline": [...],
  "aggregated_metrics": {...}
}

// After Archiving (price_history_permanent_archived)
{
  "permanent_property_id": "abc123...",
  "community_id": "https://...",
  "price_timeline": [...],
  "aggregated_metrics": {...},
  "archived_at": "2025-09-09T15:30:00Z",
  "archive_reason": "community archived"
}
```

### **Safety Features**

- **Atomic Operations**: Insert to archive → Delete from active
- **Error Isolation**: Failed individual archives don't break the batch
- **Audit Trail**: Archive metadata with timestamp and reason
- **Dual Triggers**: Both immediate and scheduled enforcement
- **Logging**: Comprehensive logging for debugging and monitoring

### **Database Collections Relationship**

```
communitydata [listing_status: "archived"]
    ↓ (triggers archiving rule)
price_history_permanent [permanent_property_id: md5(community_id)]
    ↓ (automatic migration)
price_history_permanent_archived [+ archive metadata]
```

## 🔍 Monitoring & Logging

The system provides detailed logging at each step:

- `📦 ARCHIVING RULE: Moving price history for X archived communities...`
- `📦 ARCHIVED: {community_id} -> price_history_permanent_archived`
- `✅ ARCHIVING RULE ENFORCED: X price histories moved to archive`

This ensures full visibility into the archiving process and compliance with the database rule.
