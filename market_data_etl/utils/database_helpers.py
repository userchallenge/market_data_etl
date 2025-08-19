"""
Unified database operation utilities to eliminate code duplication.

This module provides common database operation patterns used throughout
the application, reducing code duplication and improving consistency.
"""

from typing import Any, Callable, Dict, List, Optional, Type, TypeVar
from contextlib import contextmanager
from sqlalchemy.orm import Session
from functools import wraps

from ..utils.logging import get_logger
from .error_handlers import handle_database_errors
from .transformation_helpers import create_timestamp

logger = get_logger(__name__)

T = TypeVar('T')


class DatabaseOperationContext:
    """
    Context manager for database operations with consistent session handling.
    
    This consolidates the repeated pattern of:
    - with self.get_session() as session:
    - session.add()
    - session.commit()
    - Exception handling
    """
    
    def __init__(self, db_manager, operation_name: str):
        self.db_manager = db_manager
        self.operation_name = operation_name
        self.session = None
        self.records_added = 0
        self.records_updated = 0
    
    def __enter__(self) -> 'DatabaseOperationContext':
        self.session = self.db_manager.get_session().__enter__()
        logger.debug(f"Started database operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.session.commit()
                logger.info(f"Completed database operation: {self.operation_name} - Added: {self.records_added}, Updated: {self.records_updated}")
            else:
                self.session.rollback()
                logger.error(f"Failed database operation: {self.operation_name} - {exc_val}")
        finally:
            self.session.__exit__(exc_type, exc_val, exc_tb)
    
    def add_record(self, record: Any) -> Any:
        """Add a record to the session and track the addition."""
        self.session.add(record)
        self.records_added += 1
        return record
    
    def merge_record(self, record: Any) -> Any:
        """Merge a record with the session and track the update."""
        merged_record = self.session.merge(record)
        self.records_updated += 1
        return merged_record
    
    def bulk_add_records(self, records: List[Any]) -> List[Any]:
        """Add multiple records to the session."""
        for record in records:
            self.session.add(record)
        self.records_added += len(records)
        return records
    
    def execute_query(self, query) -> Any:
        """Execute a query within the session."""
        return self.session.execute(query)
    
    def get_session(self) -> Session:
        """Get the current session."""
        return self.session


def database_operation(operation_name: str):
    """
    Decorator for database operations that require consistent session handling.
    
    This replaces the repeated pattern of:
    - try/except blocks around database operations
    - Session management
    - Error logging
    
    Args:
        operation_name: Name of the database operation for logging
        
    Usage:
        @database_operation("store_price_data")
        def store_price_data(self, price_data):
            with DatabaseOperationContext(self, "store_price_data") as ctx:
                for price in price_data:
                    ctx.add_record(price)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get the database manager instance (first argument)
                instance = args[0] if args else None
                if hasattr(instance, 'logger'):
                    instance.logger.error(f"Database operation '{operation_name}' failed: {e}", exc_info=True)
                else:
                    logger.error(f"Database operation '{operation_name}' failed: {e}", exc_info=True)
                raise
        return wrapper
    return decorator


def safe_database_query(
    session: Session,
    query_func: Callable,
    operation_name: str = "query",
    default_return: Any = None
) -> Any:
    """
    Safely execute a database query with consistent error handling.
    
    Args:
        session: Database session
        query_func: Function that executes the query
        operation_name: Name of the operation for logging
        default_return: Value to return if query fails
        
    Returns:
        Query result or default_return on failure
        
    Usage:
        result = safe_database_query(
            session,
            lambda: session.query(Price).filter_by(ticker="AAPL").all(),
            operation_name="fetch_prices",
            default_return=[]
        )
    """
    try:
        return query_func()
    except Exception as e:
        logger.error(f"Database query '{operation_name}' failed: {e}", exc_info=True)
        return default_return


def update_record_timestamp(record: Any, field_name: str = 'updated_at'):
    """
    Update a record's timestamp field consistently.
    
    This consolidates the repeated pattern of:
    record.updated_at = datetime.utcnow()
    
    Args:
        record: Database record to update
        field_name: Name of the timestamp field
        
    Usage:
        update_record_timestamp(instrument)
        # Sets instrument.updated_at = datetime.utcnow()
    """
    from datetime import datetime
    if hasattr(record, field_name):
        setattr(record, field_name, datetime.utcnow())


def create_or_update_record(
    session: Session,
    model_class: Type[T],
    filter_conditions: Dict[str, Any],
    update_data: Dict[str, Any],
    create_data: Optional[Dict[str, Any]] = None
) -> tuple[T, bool]:
    """
    Create or update a database record with consistent handling.
    
    Args:
        session: Database session
        model_class: SQLAlchemy model class
        filter_conditions: Conditions to find existing record
        update_data: Data to update on existing record
        create_data: Data for creating new record (defaults to update_data)
        
    Returns:
        Tuple of (record, was_created)
        
    Usage:
        instrument, created = create_or_update_record(
            session=session,
            model_class=Instrument,
            filter_conditions={'ticker': 'AAPL'},
            update_data={'name': 'Apple Inc.', 'updated_at': datetime.utcnow()},
            create_data={'ticker': 'AAPL', 'name': 'Apple Inc.', 'created_at': datetime.utcnow()}
        )
    """
    # Try to find existing record
    existing_record = session.query(model_class).filter_by(**filter_conditions).first()
    
    if existing_record:
        # Update existing record
        for key, value in update_data.items():
            if hasattr(existing_record, key):
                setattr(existing_record, key, value)
        update_record_timestamp(existing_record)
        return existing_record, False
    else:
        # Create new record
        create_data = create_data or update_data.copy()
        new_record = model_class(**create_data)
        session.add(new_record)
        return new_record, True


def batch_upsert_records(
    session: Session,
    model_class: Type[T],
    records_data: List[Dict[str, Any]],
    key_fields: List[str]
) -> Dict[str, int]:
    """
    Batch upsert multiple records efficiently.
    
    Args:
        session: Database session
        model_class: SQLAlchemy model class
        records_data: List of record data dictionaries
        key_fields: Fields to use for identifying existing records
        
    Returns:
        Dictionary with counts of created and updated records
        
    Usage:
        results = batch_upsert_records(
            session=session,
            model_class=Price,
            records_data=price_data_list,
            key_fields=['ticker', 'date']
        )
    """
    created_count = 0
    updated_count = 0
    
    for record_data in records_data:
        # Build filter conditions from key fields
        filter_conditions = {field: record_data[field] for field in key_fields if field in record_data}
        
        # Create or update the record
        record, was_created = create_or_update_record(
            session=session,
            model_class=model_class,
            filter_conditions=filter_conditions,
            update_data=record_data
        )
        
        if was_created:
            created_count += 1
        else:
            updated_count += 1
    
    return {
        'created': created_count,
        'updated': updated_count,
        'total': len(records_data)
    }


def get_or_create_record(
    session: Session,
    model_class: Type[T],
    defaults: Optional[Dict[str, Any]] = None,
    **filter_conditions
) -> tuple[T, bool]:
    """
    Get existing record or create new one if not found.
    
    Args:
        session: Database session
        model_class: SQLAlchemy model class
        defaults: Default values for creating new record
        **filter_conditions: Conditions to find existing record
        
    Returns:
        Tuple of (record, was_created)
        
    Usage:
        instrument, created = get_or_create_record(
            session=session,
            model_class=Instrument,
            ticker='AAPL',
            defaults={'name': 'Apple Inc.', 'currency': 'USD'}
        )
    """
    existing_record = session.query(model_class).filter_by(**filter_conditions).first()
    
    if existing_record:
        return existing_record, False
    
    # Create new record
    create_data = filter_conditions.copy()
    if defaults:
        create_data.update(defaults)
    
    new_record = model_class(**create_data)
    session.add(new_record)
    return new_record, True


class BulkOperationTracker:
    """
    Track bulk database operations for reporting and logging.
    """
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        self.records_processed = 0
        self.records_created = 0
        self.records_updated = 0
        self.errors = []
    
    def start(self):
        """Start tracking the bulk operation."""
        from datetime import datetime
        self.start_time = datetime.utcnow()
        logger.info(f"Starting bulk operation: {self.operation_name}")
    
    def record_created(self, count: int = 1):
        """Record that records were created."""
        self.records_created += count
        self.records_processed += count
    
    def record_updated(self, count: int = 1):
        """Record that records were updated."""
        self.records_updated += count
        self.records_processed += count
    
    def record_error(self, error_msg: str):
        """Record an error during the operation."""
        self.errors.append(error_msg)
        logger.warning(f"Bulk operation error in {self.operation_name}: {error_msg}")
    
    def finish(self) -> Dict[str, Any]:
        """Finish tracking and return summary."""
        from datetime import datetime
        end_time = datetime.utcnow()
        duration = end_time - self.start_time if self.start_time else None
        
        summary = {
            'operation': self.operation_name,
            'records_processed': self.records_processed,
            'records_created': self.records_created,
            'records_updated': self.records_updated,
            'errors': len(self.errors),
            'duration_seconds': duration.total_seconds() if duration else None,
            'timestamp': create_timestamp()
        }
        
        logger.info(f"Completed bulk operation: {self.operation_name} - "
                   f"Processed: {self.records_processed}, Created: {self.records_created}, "
                   f"Updated: {self.records_updated}, Errors: {len(self.errors)}")
        
        return summary


# Convenience functions for common database patterns
def store_price_data_batch(
    db_manager,
    price_records: List[Dict[str, Any]]
) -> Dict[str, int]:
    """Store price data records using unified batch operation."""
    with DatabaseOperationContext(db_manager, "store_price_data_batch") as ctx:
        from ..data.models import Price
        
        results = batch_upsert_records(
            session=ctx.get_session(),
            model_class=Price,
            records_data=price_records,
            key_fields=['ticker', 'date']
        )
        
        return results


def store_economic_data_batch(
    db_manager,
    economic_records: List[Dict[str, Any]]
) -> Dict[str, int]:
    """Store economic data records using unified batch operation."""
    with DatabaseOperationContext(db_manager, "store_economic_data_batch") as ctx:
        from ..data.models import EconomicIndicatorData
        
        results = batch_upsert_records(
            session=ctx.get_session(),
            model_class=EconomicIndicatorData,
            records_data=economic_records,
            key_fields=['indicator_id', 'date']
        )
        
        return results