Data Warehouse Architecture
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   ETL Pipeline  │    │  Data Warehouse │
│                 │    │                 │    │                 │
│ • APIs          │───▶│ • Extract       │───▶│ • Star Schema   │
│ • CSV Files     │    │ • Transform     │    │ • SCDs          │
│ • JSON Files    │    │ • Load          │    │ • Fact Tables   │
│ • Databases     │    │ • Incremental   │    │ • Dimensions    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Analytics &   │
                       │   Reporting     │
                       │                 │
                       │ • SQL Queries   │
                       │ • Dashboards    │
                       │ • Charts        │
                       └─────────────────┘