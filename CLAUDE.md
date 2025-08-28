# Claude Implementation Guidelines

This document provides comprehensive guidelines for implementing additional functionality in the market_data_etl package. Follow these patterns to maintain consistency, quality, and architectural integrity.

## Core Architecture Principles

### 1. Unified File Structure (CRITICAL)
**ALL data types (price, fundamentals, economic, crypto, etc.) MUST follow the same file organization pattern:**

```
data/
├── fetchers.py              # ALL fetchers: PriceFetcher + FundamentalsFetcher + EconomicDataFetcher + NewFetcher
├── financial_standardizer.py
└── models.py               # ALL models: Price + IncomeStatement + EconomicIndicator + NewModel

etl/
├── extract.py              # ALL extractors: FinancialDataExtractor + PriceDataExtractor + EconomicDataExtractor + NewExtractor
├── transform.py            # ALL transformers: FinancialDataTransformer + EconomicDataTransformer + NewTransformer
└── load.py                 # ALL loaders: FinancialDataLoader + EconomicDataLoader + ETLOrchestrator + NewLoader

cli/
├── commands.py             # ALL command functions: fetch_prices_command + fetch_economic_command + new_command
└── main.py                 # ALL CLI parsers and dispatch logic
```

**❌ NEVER DO**:
- Create separate files like `new_data_fetcher.py`, `new_data_extract.py`, `new_data_transform.py`, `new_data_load.py`
- Create separate CLI files for new data types
- Use inconsistent naming patterns

**✅ ALWAYS DO**:
- Add new classes to existing unified files
- Follow exact same patterns as price/fundamentals/economic
- Maintain single import points for related functionality
- Ensure changes works end-to-end from CLI-methods to saving in database, presenting results and update Readme.md to reflect changes

### 2. Separation of Concerns
The codebase follows strict ETL separation:
- **data/**: Data models, business logic, domain expertise (UNIFIED in single files)
- **etl/**: Pure Extract-Transform-Load pipeline (UNIFIED in single files)
- **database/**: Unified data persistence layer
- **utils/**: Cross-cutting concerns (logging, validation, exceptions)

### 2. CLI Integration (MANDATORY)
**ALL new data types MUST be integrated into the unified CLI interface:**

**✅ Required CLI Commands**:
```bash
# Pattern for ALL data types:
market-data-etl fetch-[datatype] --source [source] --indicator [id] --from [date]
market-data-etl [datatype]-info --indicator [id]

# Examples:
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01
market-data-etl fetch-fundamentals --ticker AAPL  
market-data-etl fetch-economic --source eurostat --indicator prc_hicp_midx --from 2024-01-01
market-data-etl economic-info --indicator prc_hicp_midx
```

**Implementation Steps**:
1. Add command functions to `cli/commands.py` (same file, not separate files)
2. Add argument parsers to `cli/main.py` (same file, not separate files)  
3. Add command dispatch logic to main() function
4. Update help examples with new commands
5. Follow exact same patterns as existing commands (error handling, validation, output)

**❌ NEVER create standalone test scripts** - integrate into CLI instead!

### 3. Single Database Principle
**CRITICAL**: The entire application must use a single database file.
- Default: `market_data.db` in the root directory
- All models use the same `Base` from `data/models.py`
- All database operations go through the unified `DatabaseManager`
- Never create separate database files for different data types

### 3. Consistent Data Flow Pattern
```
External Source → data/fetchers → etl/extract → etl/transform → etl/load → database
```

## Implementation Guidelines

### Adding New Data Types (UNIFIED APPROACH)

When adding new data types (crypto, alternative data, etc.), follow this **UNIFIED** pattern:

#### 1. Database Models (`data/models.py` - ADD TO EXISTING FILE)
```python
# Add to existing models.py file - never create separate model files
class NewDataType(Base):
    __tablename__ = 'new_data_type'
    
    # Follow existing patterns:
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Use same indexing patterns
    __table_args__ = (
        Index('ix_newdata_field1_field2', 'field1', 'field2'),
        {'sqlite_autoincrement': True}
    )
```

#### 2. Data Fetcher (`data/fetchers.py` - ADD TO EXISTING FILE)
```python
# ADD to existing fetchers.py - don't create new files!
class NewDataFetcher(DataFetcher):
    """Inherits retry logic, error handling, logging from base class."""
    
    def fetch_new_data(self, params):
        def _fetch():
            # Implementation here
            pass
        return self._retry_with_backoff(_fetch)
```

#### 3. ETL Components (ADD TO EXISTING FILES)

**ADD to `etl/extract.py`** - Pure extraction only:
```python
class NewDataExtractor:
    def __init__(self):
        from ..data.fetchers import NewDataFetcher
        self.fetcher = NewDataFetcher()
        
    def extract_new_data(self, params):
        # ONLY extraction - no transformation
        pass
```

**ADD to `etl/transform.py`** - Pure transformation only:
```python
class NewDataTransformer:
    def transform_new_data(self, raw_data):
        # ONLY transformation - no extraction or loading
        pass
```

**ADD to `etl/load.py`** - Loading and orchestration:
```python
class NewDataLoader:
    def load_new_data(self, transformed_data):
        # Use existing DatabaseManager
        pass

class NewDataETLOrchestrator:
    def __init__(self, db_manager=None):
        # Import from unified locations
        from .extract import NewDataExtractor
        from .transform import NewDataTransformer
        
        self.extractor = NewDataExtractor()
        self.transformer = NewDataTransformer()
        self.loader = NewDataLoader()
    
    def run_new_data_etl(self):
        # Coordinate extract → transform → load
        pass
```

#### 4. Database Manager Extensions (`database/manager.py` - ADD TO EXISTING FILE)
Add methods to the existing `DatabaseManager` class:
```python
def store_new_data(self, new_data: Dict[str, Any]) -> Dict[str, int]:
    """Follow exact same pattern as store_financial_data."""
    pass

def get_new_data_info(self, identifier: str) -> Dict[str, Any]:
    """Follow exact same pattern as get_instrument_info."""
    pass
```

#### 5. CLI Integration (MANDATORY - ADD TO EXISTING FILES)

**ADD to `cli/commands.py`**:
```python
def fetch_new_data_command(source: str, identifier: str, from_date: str) -> int:
    """Handle fetch-new-data command."""
    try:
        # Follow exact same patterns as fetch_economic_command
        from ..etl.load import NewDataETLOrchestrator
        
        # Validate inputs using existing utilities
        start_date = validate_date_string(from_date, "from_date")
        
        # Run ETL pipeline
        etl = NewDataETLOrchestrator()
        results = etl.run_new_data_etl(source, identifier, from_date)
        
        # Report results with same format as other commands
        if results['status'] == 'completed':
            print(f"✅ Successfully processed {source} data for {identifier}")
            return SUCCESS_EXIT_CODE
        else:
            print(f"❌ Failed to fetch {source} data")
            return ERROR_EXIT_CODE
            
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
        
def new_data_info_command(identifier: str) -> int:
    """Handle new-data-info command."""
    # Follow exact same pattern as economic_info_command
    pass
```

**ADD to `cli/main.py`** argument parsers:
```python
# Add to create_parser() function - don't create separate CLI files!
new_data_parser = subparsers.add_parser(
    'fetch-new-data',
    help='Fetch new data from various sources'
)
new_data_parser.add_argument('--source', required=True)
new_data_parser.add_argument('--identifier', required=True) 
new_data_parser.add_argument('--from', dest='from_date', required=True)
```

**ADD to `cli/main.py`** command dispatch:
```python
# Add to main() function command handling
elif args.command == 'fetch-new-data':
    exit_code = fetch_new_data_command(
        source=args.source,
        identifier=args.identifier,
        from_date=args.from_date
    )
```

### Code Quality Standards

#### 1. Type Hints
All functions must have comprehensive type hints:
```python
from typing import Dict, Any, List, Optional
from datetime import date, datetime

def process_data(
    data: Dict[str, Any],
    from_date: Optional[date] = None
) -> List[Dict[str, Any]]:
    pass
```

#### 2. Error Handling
Use existing exception hierarchy:
```python
from ..utils.exceptions import DatabaseError, ValidationError, YahooFinanceError

# Don't create new exception types without strong justification
# Let operations fail clearly - don't mask errors
```

#### 3. Logging
Use existing logging patterns:
```python
from ..utils.logging import get_logger

class MyClass:
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def my_method(self):
        self.logger.info("Starting operation")
        self.logger.debug("Debug details")
        self.logger.error("Error occurred", exc_info=True)
```

#### 4. Configuration
Use existing config patterns:
```python
from ..config import config

# Access database path
db_path = config.database.path

# Access retry settings
max_retries = config.retry.max_retries
```

#### 5. Validation
Use existing validation utilities:
```python
from ..utils.validation import validate_ticker, sanitize_sql_input

# Always validate inputs
ticker = validate_ticker(user_input)
```

### Database Guidelines

#### 1. Single Database File
- **NEVER** create separate database files
- **ALWAYS** use the existing `DatabaseManager`
- **NEVER** create separate `Base` declaratives
- **ALWAYS** add new models to `data/models.py`

#### 2. Model Relationships
- Link to existing entities where logical (e.g., Company)
- Use consistent foreign key patterns
- Include proper back_populates relationships
- Add appropriate indexes for performance

#### 3. Migration Strategy
- New models are automatically created by SQLAlchemy
- Existing data is preserved
- No explicit migrations needed for additive changes

### Testing Requirements

#### 1. Test Structure
```python
def test_new_functionality():
    """Test with mock data following existing patterns."""
    # Use existing test database patterns
    # Test extract, transform, load separately
    # Test error conditions
    pass
```

#### 2. Mock Data
- Create realistic mock data for testing
- Don't require real API calls for basic tests
- Test both success and failure scenarios

### Documentation Requirements

#### 1. README Updates
**MANDATORY**: Update README.md with every implementation:
```markdown
## New Feature: [Feature Name]
- **Purpose**: Brief description
- **APIs Supported**: List of data sources
- **Usage Example**: Code snippet
- **Database Tables**: New tables added
```

#### 2. Code Documentation
- Comprehensive docstrings for all classes and methods
- Type hints for all parameters and return values
- Usage examples in docstrings
- Clear explanation of data flow

## Mandatory Checklist for New Implementations

Before submitting any new functionality:

### ✅ File Structure Consistency (CRITICAL)
- [ ] **NO new fetcher files** - added classes to `data/fetchers.py`
- [ ] **NO new ETL files** - added classes to `etl/extract.py`, `etl/transform.py`, `etl/load.py`
- [ ] **NO new CLI files** - added functions to `cli/commands.py` and `cli/main.py`
- [ ] **NO separate test scripts** - integrated into unified CLI
- [ ] Models added to existing `data/models.py`

### ✅ CLI Integration (CRITICAL)  
- [ ] Added fetch command to `cli/commands.py`
- [ ] Added info command to `cli/commands.py`
- [ ] Added argument parsers to `cli/main.py`
- [ ] Added command dispatch to `cli/main.py`
- [ ] Updated help examples with new commands
- [ ] Follows exact same patterns as existing commands

### ✅ Architecture Compliance
- [ ] Uses single database file (`market_data.db`)
- [ ] Follows ETL separation (extract/transform/load)
- [ ] Extends existing `DatabaseManager`
- [ ] Uses existing base classes (`DataFetcher`, etc.)
- [ ] Imports from unified locations (not separate files)

### ✅ Code Quality
- [ ] Comprehensive type hints
- [ ] Existing error handling patterns
- [ ] Existing logging patterns
- [ ] Input validation
- [ ] No code duplication

### ✅ Testing
- [ ] Unit tests with mock data
- [ ] Integration tests with real APIs (optional)
- [ ] Error condition tests
- [ ] Database operation tests

### ✅ Documentation
- [ ] README.md updated with new features
- [ ] CLAUDE.md updated if patterns change
- [ ] Comprehensive code docstrings
- [ ] Usage examples provided

### ✅ Database Integrity
- [ ] Single database file maintained
- [ ] No breaking changes to existing schemas
- [ ] Proper indexes for performance
- [ ] Consistent relationship patterns

## Anti-Patterns to Avoid

### ❌ File Structure Anti-Patterns (CRITICAL TO AVOID)
- Creating separate fetcher files (`new_data_fetcher.py`)
- Creating separate ETL files (`new_extract.py`, `new_transform.py`, `new_load.py`)
- Creating separate CLI files for new data types
- Creating standalone test scripts instead of CLI integration
- Using inconsistent file naming patterns

### ❌ CLI Anti-Patterns (CRITICAL TO AVOID)  
- Creating standalone test files like `test_implementation.py`
- Not integrating new functionality into the unified CLI
- Creating separate command modules 
- Inconsistent argument patterns
- Missing help text and examples

### ❌ Database Anti-Patterns
- Creating separate database files
- Creating separate `Base` declaratives
- Bypassing the `DatabaseManager`
- Direct database connections
- Separate model files

### ❌ Architecture Anti-Patterns
- Mixing business logic in ETL components
- Extraction logic in transform components
- Loading logic in extract components
- Bypassing the standard data flow
- Importing from separate files instead of unified locations

### ❌ Code Anti-Patterns
- Creating new exception types unnecessarily
- Ignoring or masking errors
- Hardcoding configuration values
- Missing type hints
- Duplicate code patterns

## Configuration Management

### Environment Variables
Use existing environment variable patterns:
```bash
MARKET_DATA_DB_PATH=market_data.db
MARKET_DATA_LOG_LEVEL=INFO
MARKET_DATA_MAX_RETRIES=5
FRED_API_KEY=your_api_key_here
```

### API Keys and Secrets
- Store in environment variables
- Never hardcode in source code
- Provide clear documentation for required keys
- Handle missing keys gracefully

## Performance Considerations

### Database Operations
- Use bulk operations for large datasets
- Implement duplicate detection
- Use transactions for consistency
- Add appropriate indexes

### API Calls
- Use existing retry mechanisms
- Implement rate limiting if needed
- Cache responses when appropriate
- Handle API failures gracefully

## Future Extensions

This architecture supports easy extension to:
- Cryptocurrency data
- Economic indicators
- Alternative data sources
- Real-time data streams
- Data analysis pipelines

Follow these guidelines to ensure your implementations integrate seamlessly with the existing codebase and maintain the high quality and consistency standards of the market_data_etl package.

## Questions and Clarifications

When in doubt:
1. Look at existing implementations (financial data, price data)
2. Follow the exact same patterns
3. Use the same base classes and utilities
4. Maintain single database principle
5. Update documentation

Remember: Consistency is more important than perfection. Follow established patterns even if you think there might be a "better" way.
- Always check the actual CLI interface when providing examples
- ignore notes.md