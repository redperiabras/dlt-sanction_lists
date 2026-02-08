Sanctions Lists Pipeline - Codebase Walkthrough
Great! I've explored your codebase. This is a sanctions data pipeline that consolidates multiple international sanctions lists into a unified DuckDB database using the dlt (data load tool) framework.

Overview
Purpose: Extract, transform, and load sanctions data from multiple sources (INTERPOL, UN Security Council, US OFAC) into a local DuckDB database.

Tech Stack:

dlt: Core ETL framework
DuckDB: Local database destination
httpx: HTTP client
xmltodict: XML parsing
Python 3.10
Project Structure

/dlt-sanction_lists/
├── sanctions_pipeline.py      # Main entry point - orchestrates the pipeline
├── source/
│   ├── interpol.py           # INTERPOL notices API integration
│   ├── unsc.py              # UN Security Council XML parser
│   └── usofac.py            # OFAC source (placeholder)
├── paginators.py            # Custom pagination logic
├── destinations.py          # Custom destinations (debug/testing)
├── sanctions.duckdb         # Output database (~19 MB)
└── test_sdn_xml.ipynb      # Testing/analysis notebook
Data Flow Architecture

Data Sources → RESTClient → Pagination → Extraction → Transformation → DuckDB
1. Main Entry Point: sanctions_pipeline.py
This file orchestrates the pipeline. Currently configured to run UNSC (INTERPOL is commented out):


pipeline = dlt.pipeline(
    pipeline_name="sanctions",
    destination='duckdb',
    dataset_name="unsc",
    progress=dlt.progress.tqdm(colour="yellow"),
)
load_info = pipeline.run(src_unsc())
Key Concepts:

dlt.pipeline(): Creates a pipeline instance
destination='duckdb': Data goes to local DuckDB
pipeline.run(): Executes the source function
2. Data Sources (source/ directory)
A. INTERPOL - source/interpol.py
What it does: Fetches wanted person/entity notices from INTERPOL's public API

Architecture:

Two-stage fetch:
Get notice list (paginated)
Fetch full details for each notice
Pagination: JSONLinkPaginator follows _links.next.href
Parallelization: Uses parallelized=True for concurrent detail fetching
Code Pattern:


@dlt.source()
def src_interpol():
    # Create REST client with base URL
    client = RESTClient(...)
    
    # Resource extracts list
    @dlt.resource()
    def notices(notice_type: str):
        yield from client.paginate(...)
    
    # Transformer fetches details
    @dlt.transformer(data_from=notices, parallelized=True)
    def persons_details(notice):
        return client.get(notice["_links"]["self"]["href"]).json()
B. UNSC - source/unsc.py
What it does: Downloads and parses UN Security Council consolidated sanctions XML

Architecture:

Single endpoint: One consolidated XML file
Encoding handling: Tries ISO-8859-6 (Arabic), falls back to UTF-8
Two transformers: Separate processing for individuals vs entities
Data normalization:
Flattens nested {VALUE: ...} structures
Converts single items to lists for consistency
Removes null values
Key Transformation Logic (lines 33-89):


# Extract VALUE from nested objects
if isinstance(individual.get("DESIGNATION"), dict):
    individual["DESIGNATION"] = individual["DESIGNATION"].get("VALUE")

# Normalize to lists
for field in ["INDIVIDUAL_ALIAS", "INDIVIDUAL_ADDRESS", ...]:
    if field in individual and not isinstance(individual[field], list):
        individual[field] = [individual[field]]
C. USOFAC - source/usofac.py
Status: Empty placeholder for US Treasury OFAC data (planned but not implemented)

3. Custom Components
paginators.py - YearMonthPathPaginator
Custom paginator for date-based APIs that require year/month path parameters:

Starts from initial date (default: Sept 2023)
Increments monthly until current month
Not currently used, but available for future sources
destinations.py - print_sink
Debug destination that prints data to console instead of loading to database:


@dlt.destination(batch_size=5, name="print")
def print_sink():
    # Prints items by table name
How Data Flows
UNSC Example (Current Active Pipeline):
Extract (source/unsc.py:17-31):


response = client.get(URL)
xml = xmltodict.parse(response.text)
yield xml["CONSOLIDATED_LIST"]["INDIVIDUALS"]["INDIVIDUAL"]
Transform (source/unsc.py:33-61):

Flatten nested fields
Normalize to lists
Remove nulls
Parallelized for performance
Load:

dlt automatically infers schema
Creates tables in DuckDB
Progress shown via tqdm
Output:

Database: sanctions.duckdb
Dataset: unsc
Tables: individuals, entities
How to Use
Run the pipeline:


python sanctions_pipeline.py
Switch between sources - Edit sanctions_pipeline.py:


# For INTERPOL:
load_info = pipeline.run(src_interpol())

# For UNSC:
load_info = pipeline.run(src_unsc())
Query the data - Use DuckDB CLI or Python:


duckdb sanctions.duckdb
Recent Development Activity
Based on git history:

✅ INTERPOL source implemented and optimized
✅ UNSC source implemented with XML parsing
🚧 USOFAC standardization logic started (in notebook)
📝 Currently using UNSC as active pipeline
Key Design Patterns
Resource-Transformer Pattern: Resources extract raw data, transformers normalize it
Parallelization: Both sources use parallelized=True for concurrent processing
Encoding Resilience: UNSC handles multiple character encodings gracefully
Modular Sources: Each data source is independent and can be run separately
dlt Framework: Handles schema inference, incremental loading, and state management automatically
Questions?
The codebase is well-structured and follows dlt best practices. The main areas to explore based on your needs:

Add new sources: Follow the pattern in source/interpol.py or source/unsc.py
Modify transformations: Look at the transformer functions in each source file
Change destination: Modify the destination parameter in sanctions_pipeline.py
What would you like to explore or work on next?
