# Claude Implementation Guidelines

This document provides comprehensive guidelines for implementing additional functionality in the market_data_etl package. Follow these patterns to maintain consistency, quality, and architectural integrity.

## Core Architecture Principles

### 1. Separation of Concerns
The codebase follows strict ETL separation:
- **data/**: Data models, business logic, domain expertise
- **etl/**: Pure Extract-Transform-Load pipeline (no business logic)
- **database/**: Unified data persistence layer
- **utils/**: Cross-cutting concerns (logging, validation, exceptions)

### 2. Single Database Principle
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

### Adding New Data Types

When adding new data types (economic, crypto, etc.), follow this exact pattern:

#### 1. Database Models (`data/models.py`)
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

#### 2. Data Fetcher (`data/new_data_fetcher.py`)
```python
from .fetchers import DataFetcher

class NewDataFetcher(DataFetcher):
    """Inherits retry logic, error handling, logging from base class."""
    
    def fetch_new_data(self, params):
        def _fetch():
            # Implementation here
            pass
        return self._retry_with_backoff(_fetch)
```

#### 3. ETL Components
Follow the three-file pattern:

**`etl/new_data_extract.py`** - Pure extraction only:
```python
class NewDataExtractor(NewDataFetcher):
    def extract_new_data(self, params):
        # ONLY extraction - no transformation
        pass
```

**`etl/new_data_transform.py`** - Pure transformation only:
```python
class NewDataTransformer:
    def transform_new_data(self, raw_data):
        # ONLY transformation - no extraction or loading
        pass
```

**`etl/new_data_load.py`** - Loading and orchestration:
```python
class NewDataLoader:
    def load_new_data(self, transformed_data):
        # Use existing DatabaseManager
        pass

class NewDataETLOrchestrator:
    def run_new_data_etl(self):
        # Coordinate extract → transform → load
        pass
```

#### 4. Database Manager Extensions
Add methods to the existing `DatabaseManager` class:
```python
def store_new_data(self, new_data: Dict[str, Any]) -> Dict[str, int]:
    """Follow exact same pattern as store_financial_data."""
    pass

def get_new_data_info(self, identifier: str) -> Dict[str, Any]:
    """Follow exact same pattern as get_ticker_info."""
    pass
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

### ✅ Architecture Compliance
- [ ] Uses single database file (`market_data.db`)
- [ ] Follows ETL separation (extract/transform/load)
- [ ] Extends existing `DatabaseManager`
- [ ] Uses existing base classes (`DataFetcher`, etc.)
- [ ] Adds models to existing `models.py`

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