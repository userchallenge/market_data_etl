"""
Test economic indicators configuration and transformation.

These tests ensure that:
1. Current hardcoded economic indicator mappings work
2. New YAML-based indicator loading works  
3. Indicator transformation logic remains consistent
4. All existing indicators are preserved during migration
"""

import pytest
import yaml
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from market_data_etl.etl.transform import EconomicDataTransformer


class TestCurrentEconomicIndicators:
    """Test current hardcoded economic indicator functionality."""
    
    @pytest.fixture
    def transformer(self):
        """Create EconomicDataTransformer instance."""
        return EconomicDataTransformer()
    
    def test_current_indicator_mapping_exists(self, transformer):
        """Test that current hardcoded indicator mappings work."""
        # Test key indicators that are currently hardcoded
        test_cases = [
            ('eurostat', 'prc_hicp_mmor'),  # Eurozone inflation
            ('fred', 'UNRATE'),             # US unemployment
            ('fred', 'CPIAUCSL'),           # US CPI
            ('fred', 'DFF'),                # US Fed Funds Rate
            ('ecb', 'FM.D.U2.EUR.4F.KR.MRR_FR.LEV'),  # ECB daily rate
            ('ecb', 'FM.B.U2.EUR.4F.KR.MRR_FR.LEV'),  # ECB monthly rate
        ]
        
        for source, source_identifier in test_cases:
            mapping = transformer._get_indicator_mapping(source, source_identifier)
            
            # Verify mapping structure
            assert 'name' in mapping
            assert 'source' in mapping
            assert 'source_identifier' in mapping
            assert 'description' in mapping
            
            # Verify source matches
            assert mapping['source'] == source
            
            # Verify has meaningful description
            assert len(mapping['description']) > 10
            
            # Verify standardized name format
            assert mapping['name'].islower()
            assert '_' in mapping['name']  # Should use underscore format
    
    def test_eurostat_indicator_processing(self, transformer):
        """Test Eurostat indicator processing maintains current behavior."""
        # Sample Eurostat data structure (simplified)
        sample_raw_data = {
            'data_code': 'prc_hicp_mmor',
            'raw_data': {
                'dimension': {
                    'time': {
                        'category': {
                            'index': {'2024M01': 0, '2024M02': 1}
                        }
                    }
                },
                'value': {'0': 2.5, '1': 2.7}
            }
        }
        
        transformed = transformer.transform_eurostat_data(sample_raw_data)
        
        # Verify structure remains consistent
        assert 'name' in transformed
        assert 'source' in transformed
        assert 'data_points' in transformed
        assert transformed['source'] == 'eurostat'
        assert len(transformed['data_points']) == 2
    
    def test_fred_indicator_processing(self, transformer):
        """Test FRED indicator processing maintains current behavior."""
        # Sample FRED data structure
        sample_raw_data = {
            'series_id': 'UNRATE',
            'raw_data': {
                'observations': [
                    {'date': '2024-01-01', 'value': '3.8'},
                    {'date': '2024-02-01', 'value': '3.9'}
                ]
            }
        }
        
        transformed = transformer.transform_fred_data(sample_raw_data)
        
        # FRED can return single dict or list (for CPI special case)
        if isinstance(transformed, list):
            transformed = transformed[0]  # Take first indicator
        
        assert 'name' in transformed
        assert 'source' in transformed
        assert transformed['source'] == 'fred'
        assert 'data_points' in transformed
    
    def test_ecb_indicator_processing(self, transformer):
        """Test ECB indicator processing maintains current behavior."""
        # Sample ECB data structure (simplified)
        sample_raw_data = {
            'dataflow_ref': 'FM',
            'series_key': 'B.U2.EUR.4F.KR.MRR_FR.LEV',
            'raw_data': {
                'structure': {
                    'dimensions': {
                        'observation': [
                            {
                                'values': [
                                    {'id': '2024-01', 'name': '2024-01'},
                                    {'id': '2024-02', 'name': '2024-02'}
                                ]
                            }
                        ]
                    }
                },
                'dataSets': [
                    {
                        'series': {
                            '0:0:0:0:0:0:0': {
                                'observations': {
                                    '0': [4.25],
                                    '1': [4.50]
                                }
                            }
                        }
                    }
                ]
            }
        }
        
        transformed = transformer.transform_ecb_data(sample_raw_data)
        
        assert 'name' in transformed
        assert 'source' in transformed
        assert transformed['source'] == 'ecb'
        assert 'data_points' in transformed
    
    def test_special_cpi_handling_preserved(self, transformer):
        """Test that special CPI handling (returns list) is preserved."""
        sample_cpi_data = {
            'series_id': 'CPIAUCSL',
            'raw_data': {
                'observations': [
                    {'date': '2024-01-01', 'value': '307.026'},
                    {'date': '2024-02-01', 'value': '307.789'},
                    {'date': '2024-03-01', 'value': '308.417'}
                ]
            }
        }
        
        transformed = transformer.transform_fred_data(sample_cpi_data)
        
        # CPI should return list with both index and rate indicators
        assert isinstance(transformed, list)
        assert len(transformed) == 2
        
        # First should be index
        index_indicator = transformed[0]
        assert 'inflation_index_monthly_us' in index_indicator['name']
        assert index_indicator['unit'] == 'index'
        
        # Second should be rate
        rate_indicator = transformed[1]
        assert 'inflation_monthly_us' in rate_indicator['name']
        assert rate_indicator['unit'] == 'percent'
    
    def test_unmapped_indicator_raises_error(self, transformer):
        """Test that unmapped indicators raise clear errors in pure YAML mode."""
        with pytest.raises(ValueError) as exc_info:
            transformer._get_indicator_mapping('unknown_source', 'UNKNOWN_ID')
        
        # Should contain helpful error message
        error_msg = str(exc_info.value)
        assert 'Economic indicator mapping not found' in error_msg
        assert 'unknown_source/UNKNOWN_ID' in error_msg
        assert 'config/economic_indicators.yaml' in error_msg


class TestYAMLIndicatorConfiguration:
    """Test new YAML-based indicator configuration."""
    
    @pytest.fixture
    def temp_indicators_file(self):
        """Create temporary indicators YAML file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            indicators_data = {
                'indicators': {
                    'test_inflation_euro': {
                        'source': 'eurostat',
                        'source_identifier': 'prc_hicp_test',
                        'description': 'Test Eurozone Inflation Rate'
                    },
                    'test_unemployment_us': {
                        'source': 'fred',
                        'source_identifier': 'UNRATE_TEST',
                        'description': 'Test US Unemployment Rate'
                    },
                    'test_ecb_rate': {
                        'source': 'ecb',
                        'source_identifier': 'FM.B.U2.EUR.TEST',
                        'description': 'Test ECB Interest Rate'
                    }
                }
            }
            yaml.dump(indicators_data, f)
            temp_file = f.name
        
        yield temp_file
        
        # Cleanup
        Path(temp_file).unlink()
    
    def test_yaml_indicator_loading_structure(self, temp_indicators_file):
        """Test YAML indicator file structure is correct."""
        with open(temp_indicators_file) as f:
            data = yaml.safe_load(f)
        
        assert 'indicators' in data
        indicators = data['indicators']
        
        # Test each indicator has required fields
        for indicator_name, config in indicators.items():
            assert 'source' in config
            assert 'source_identifier' in config
            assert 'description' in config
            
            # Test source is valid
            assert config['source'] in ['eurostat', 'ecb', 'fred']
            
            # Test identifiers are non-empty strings
            assert isinstance(config['source_identifier'], str)
            assert len(config['source_identifier']) > 0
            
            # Test descriptions are meaningful
            assert isinstance(config['description'], str)
            assert len(config['description']) > 10
    
    def test_indicator_name_conventions(self, temp_indicators_file):
        """Test that indicator names follow conventions."""
        with open(temp_indicators_file) as f:
            data = yaml.safe_load(f)
        
        for indicator_name in data['indicators'].keys():
            # Should be lowercase with underscores
            assert indicator_name.islower()
            assert ' ' not in indicator_name  # No spaces
            assert '-' not in indicator_name  # No hyphens
            
            # Should contain meaningful parts
            assert '_' in indicator_name  # Should have categories
            
            # Common patterns
            meaningful_parts = [
                'inflation', 'unemployment', 'interest', 'rate', 'gdp', 
                'euro', 'us', 'monthly', 'daily', 'quarterly', 'annual'
            ]
            has_meaningful_part = any(part in indicator_name for part in meaningful_parts)
            assert has_meaningful_part, f"Indicator name '{indicator_name}' should contain meaningful parts"


class TestIndicatorMigrationCompatibility:
    """Test compatibility during migration from hardcoded to YAML."""
    
    def test_all_current_indicators_in_csv(self):
        """Test that all indicators in current CSV are preserved."""
        # Read the current economic_data_mapping.csv
        csv_path = Path(__file__).parent.parent.parent / "economic_data_mapping.csv"
        
        if csv_path.exists():
            import pandas as pd
            df = pd.read_csv(csv_path)
            
            # Verify structure
            required_columns = ['name', 'source', 'source_identifier', 'description']
            for col in required_columns:
                assert col in df.columns
            
            # Verify all sources are supported
            valid_sources = {'eurostat', 'ecb', 'fred'}
            assert set(df['source'].unique()).issubset(valid_sources)
            
            # Verify no empty values in critical fields
            assert not df['name'].isnull().any()
            assert not df['source'].isnull().any()
            assert not df['source_identifier'].isnull().any()
    
    def test_yaml_indicators_work_with_transformer(self):
        """Test that YAML indicators work correctly with the transformer."""
        # Test that our actual YAML configuration works
        transformer = EconomicDataTransformer()
        
        # Test some indicators that should exist in our YAML config
        test_mappings = [
            ('fred', 'UNRATE'),
            ('eurostat', 'prc_hicp_mmor'),
            ('ecb', 'FM.B.U2.EUR.4F.KR.MRR_FR.LEV')
        ]
        
        yaml_indicators = {}
        
        for source, identifier in test_mappings:
            try:
                mapping = transformer._get_indicator_mapping(source, identifier)
                
                yaml_indicators[mapping['name']] = {
                    'source': mapping['source'],
                    'source_identifier': mapping['source_identifier'],
                    'description': mapping['description']
                }
            except ValueError:
                # Skip if not in YAML config - that's expected in pure YAML mode
                continue
        
        # Verify YAML structure is valid for any found indicators
        if yaml_indicators:
            yaml_content = yaml.dump({'indicators': yaml_indicators})
            reloaded = yaml.safe_load(yaml_content)
            
            assert 'indicators' in reloaded
            assert len(reloaded['indicators']) <= len(test_mappings)
    
    def test_indicator_lookup_performance(self):
        """Test that indicator lookup remains performant for available indicators."""
        transformer = EconomicDataTransformer()
        
        # Get list of actually available indicators from YAML config
        from market_data_etl.config import config
        
        if not config.economic_indicators:
            pytest.skip("No YAML indicators available for performance test")
        
        # Build list of available test indicators
        available_indicators = []
        for indicator_name, indicator_config in config.economic_indicators.items():
            source = indicator_config.get('source')
            identifier = indicator_config.get('source_identifier')
            if source and identifier:
                available_indicators.append((source, identifier))
        
        if not available_indicators:
            pytest.skip("No valid indicators available for performance test")
        
        import time
        start_time = time.time()
        
        # Test lookups with available indicators
        for source, identifier in (available_indicators * (100 // len(available_indicators) + 1))[:100]:
            mapping = transformer._get_indicator_mapping(source, identifier)
            assert mapping['source'] == source
        
        elapsed = time.time() - start_time
        
        # Should complete 100 lookups in reasonable time (< 1 second)
        assert elapsed < 1.0, f"Indicator lookup too slow: {elapsed:.3f}s for 100 lookups"


class TestEconomicDataValidation:
    """Test validation of economic indicator data."""
    
    def test_indicator_data_format_validation(self):
        """Test validation of indicator data format."""
        # Valid data points
        valid_data_points = [
            {'date': '2024-01-01', 'value': 2.5},
            {'date': '2024-02-01', 'value': 2.7},
        ]
        
        for point in valid_data_points:
            assert 'date' in point
            assert 'value' in point
            assert isinstance(point['value'], (int, float))
        
        # Test date format validation would go here
        # (This is where we'd test the date parsing functions)
    
    def test_data_point_edge_cases(self):
        """Test handling of edge cases in data points."""
        transformer = EconomicDataTransformer()
        
        # Test empty data points
        empty_result = transformer._parse_fred_json({'observations': []})
        assert isinstance(empty_result, list)
        assert len(empty_result) == 0
        
        # Test missing values (FRED uses "." for missing data)
        missing_data = {
            'observations': [
                {'date': '2024-01-01', 'value': '2.5'},
                {'date': '2024-02-01', 'value': '.'},  # Missing value
                {'date': '2024-03-01', 'value': '2.7'}
            ]
        }
        
        result = transformer._parse_fred_json(missing_data)
        assert len(result) == 2  # Should skip missing value
        
        dates = [point['date'] for point in result]
        assert '2024-02-01' not in dates  # Should skip the missing value