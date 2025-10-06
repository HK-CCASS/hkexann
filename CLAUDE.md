# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive Hong Kong Stock Exchange (HKEX) announcement downloader and monitoring system. The project consists of three main architectural components:

1. **Unified Downloader System** (`unified_downloader/`) - Modern modular architecture with enterprise features
2. **Enhanced Monitoring System** (`start_enhanced_monitor.py` + `services/`) - Real-time monitoring with enterprise-grade data processing
3. **Original Downloader** (`main.py`) - Legacy standalone tool (2,100+ lines, preserved for backward compatibility)

## Architecture

### Core Components

#### Unified Downloader System (Modern Architecture)
- **unified_downloader/** - Enterprise-grade modular system:
  - `config/unified_config.py` - Unified configuration management with 8 preset templates
  - `file_manager/unified_file_manager.py` - Smart file organization with 5 directory structures
  - `core/downloader_abstract.py` - Pluggable download strategies and rate limiting
  - `adapters/legacy_adapter.py` - Backward compatibility with existing systems

#### Legacy Components
- **main.py** (2,100+ lines) - Original monolithic implementation (preserved for compatibility)
- **services/** - Modular microservices architecture (56 Python files, 29,000+ lines):
  - `monitor/` - Real-time API monitoring with dual-stage filtering
  - `data_loader/` - Database integration (ClickHouse, MySQL)
  - `milvus/` - Vector database for semantic search
  - `document_processor/` - PDF processing and text extraction
  - `embeddings/` - Text vectorization services with SiliconFlow integration
  - `storage/` - File management utilities

### Data Flow

```
API Monitor → Dual Filter → Download Processor → Document Processor → Vector Storage
     ↓              ↓              ↓                    ↓                ↓
ClickHouse    Stock Filter    PDF Download      Text Extraction     Milvus DB
```

## Common Development Commands

### Running the System

**Unified Downloader System (Recommended for new development):**
```bash
# See unified_downloader/README.md for detailed usage
from unified_downloader.config.unified_config import ConfigManager
from unified_downloader.adapters.legacy_adapter import UnifiedAPIAdapter

# Use legacy-compatible interface
adapter = UnifiedAPIAdapter('config.yaml')
main_adapter = adapter.get_adapter('main')
```

**Original Downloader (Main CLI):**
```bash
# Basic usage
python main.py -s 00700 --start 2024-01-01 --end today

# All original commands still work
python main.py -s 00700 --async
```

**Enhanced Monitoring System:**
```bash
# Start real-time monitoring (primary production use)
python start_enhanced_monitor.py

# Test mode with detailed output
python start_enhanced_monitor.py -t

# Custom configuration
python start_enhanced_monitor.py -c custom_config.yaml

# With forced async mode
HKEX_FORCE_ASYNC=true python start_enhanced_monitor.py
```

**Manual Historical Backfill:**
```bash
# Single stock, last 7 days
python manual_historical_backfill.py -s 00700 -d 7

# Multiple stocks with date range
python manual_historical_backfill.py -s "00700,00939,01398" --date-range 2025-09-10 2025-09-17

# From stock list file
python manual_historical_backfill.py -s stocks.txt -d 30

# Custom batch size and configuration
python manual_historical_backfill.py -s 00700 -d 7 -c custom_config.yaml -b 10

# Disable filtering (get all announcements)
python manual_historical_backfill.py -s 00700 -d 7 --no-filter

# Dry run mode (preview without execution)
python manual_historical_backfill.py -s 00700 -d 7 --dry-run

# Save results to JSON file
python manual_historical_backfill.py -s 00700 -d 7 --output results.json
```

**Async Downloader (Alternative CLI):**
```bash
# High-performance async downloading
python async_downloader.py -s 00700 --start 2024-01-01 --end today

# HKEX Classification Processing
python process_hkex_classification.py
```

### Database Operations
```bash
# Test database connectivity
python main.py --test-db

# Download from database stock list
python main.py --db-stocks

# Custom database query
python main.py --db-query "SELECT stockCode FROM issue WHERE status = 'normal'"
```

### Development and Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Check configuration validity
python main.py --check-config

# List all configured tasks
python main.py --list-tasks

# Run specific task by name
python main.py --run-task "task_name"

# Verify configuration files
python tools/verify_configuration.py

# Test ClickHouse deduplication
python tools/test_deduplication.py

# Run deduplication tool
python tools/run_deduplication.py
```

### Code Quality and Testing
```bash
# Currently no formal pytest framework configured
# Tests are standalone executable Python files
# When adding tests, create them in tests/ directory following the existing pattern
```

## Configuration

### Environment Setup

1. Copy environment template:
```bash
cp .env.template .env
```

2. Required environment variables:
- `SILICONFLOW_API_KEY` - SiliconFlow API key for LLM and embedding services (required)
- `CLICKHOUSE_PASSWORD` - ClickHouse database password (required)
- `CCASS_PASSWORD` - CCASS database password (required)
- `DB_PASSWORD` - MySQL database password (recommended)
- `HKEX_FORCE_ASYNC` - Force async mode for monitoring

### Key Config Files

- **config.yaml** - Main configuration (API endpoints, database, async settings)
- **services/monitor/config/** - Monitoring-specific configurations
- **.env** - Environment variables (not in repo, use template)

## Development Guidelines

### Code Structure Patterns

The project follows multiple architectural patterns:

#### New Modular Architecture (v3.0)
- **Clean Architecture** - Separation of concerns with distinct layers
- **Repository Pattern** - Data access abstraction in `data_access/`
- **Service Pattern** - Business logic encapsulation in `business/services/`
- **Dependency Injection** - IoC container in `infrastructure/dependency_injection/`
- **Strategy Pattern** - Pluggable algorithms for filtering and processing

#### Legacy Patterns
- **Monolithic Pattern** - Single large file (`main.py`) with class-based organization
- **Microservices Pattern** - Modular services in `services/` directory
- **Factory Pattern** - Service initialization through managers
- **Observer Pattern** - Event-driven monitoring system

### Important Conventions

1. **Async/Await Usage**: All new code should use async patterns following the monitoring system style
2. **Configuration-Driven**: All behavior should be configurable through YAML files
3. **Error Handling**: Use tenacity library for retries, comprehensive logging
4. **Database Integration**: Support both MySQL (stock lists) and ClickHouse (time-series data)

### File Organization

- **Root Directory**: Main executables (`main.py`, `start_enhanced_monitor.py`, `async_downloader.py`, `manual_historical_backfill.py`)
- **unified_downloader/**: Modern modular architecture system
  - `config/unified_config.py`: Configuration management with 8 preset templates
  - `file_manager/unified_file_manager.py`: Smart file organization
  - `core/downloader_abstract.py`: Download strategies and rate limiting
  - `adapters/legacy_adapter.py`: Backward compatibility adapters
- **services/**: Microservices architecture (monitoring system)
  - `monitor/`: Core monitoring system with 20+ files
  - `data_loader/`: Database integration utilities
  - `document_processor/`: PDF processing pipeline
  - `embeddings/`: SiliconFlow AI integration
  - `milvus/`: Vector database operations
  - `storage/`: File management utilities
- **config/**: Configuration management (`settings.py`)
- **docs/**: Comprehensive documentation
- **sql/**: Database schema and queries
- **tools/**: Utility tools and scripts
  - `clickhouse_deduplication_tool.py`: ClickHouse data deduplication
  - `verify_configuration.py`: Configuration validation
  - `test_deduplication.py`: Deduplication testing
  - `run_deduplication.py`: Deduplication execution script
- **manual_historical_backfill.py**: Manual historical announcement backfill script
- **hkexann/**: Downloaded announcement storage (auto-created)
- **tests/**: Test suite directory (create when implementing tests)

## Architecture Notes

### Performance Considerations

- The monitoring system processes ~30,000 lines of code across microservices
- Async operations are critical for handling high-volume API polling
- Connection pooling implemented for database efficiency
- Rate limiting prevents API blocking

### Database Schema

- **MySQL**: Stock metadata and lists (`ccass` database)
- **ClickHouse**: Time-series announcement data (`hkex_analysis` database)
- **Milvus**: 4096-dimension vectors for semantic search

### Technology Stack

- **Core**: Python 3.8+ with asyncio for high-performance concurrent processing
- **Web**: aiohttp for async HTTP requests, requests for sync operations
- **Databases**: MySQL (PyMySQL), ClickHouse (clickhouse-driver), Milvus (pymilvus)
- **ML/AI**: SiliconFlow API for embeddings and LLM services
- **Document Processing**: PDF text extraction and vectorization pipeline
- **Configuration**: YAML-based configuration with environment variable substitution
- **Process Management**: psutil for system and resource monitoring

### System Components Comparison

| Component | Lines of Code | Architecture | Use Case |
|-----------|--------------|--------------|----------|
| **unified_downloader/** | ~1,000+ (modular) | Enterprise Architecture | New development, production features |
| **main.py** | 2,100+ (monolithic) | Single file | Backward compatibility, standalone use |
| **services/monitor/** | 29,000+ | Microservices | Real-time monitoring, production system |
| **async_downloader.py** | 450+ | Async Single file | High-performance downloading |

#### Unified Downloader (unified_downloader/) - RECOMMENDED FOR NEW FEATURES
The modern enterprise architecture provides:
- **8 Preset Configuration Templates**: From minimal to enterprise compliance
- **5 File Organization Strategies**: Flat, by-stock, by-date, by-category, hierarchical
- **Pluggable Download Strategies**: Rate limiting, proxy support, concurrent control
- **100% Backward Compatibility**: Adapters for existing systems
- **Enterprise Features**: Smart classification, symlink organization, performance monitoring

#### Monitoring System (services/monitor/)
The monitoring system is the enterprise-grade component with:
- Real-time API polling every 60 seconds
- Dual-stage filtering (stocks + announcement types)
- Enterprise health monitoring
- Vector processing pipeline
- Automatic error recovery

#### Development Priority
1. **New Features**: Implement in `unified_downloader/` modular architecture
2. **Bug Fixes**: Apply to `main.py` for backward compatibility, consider `unified_downloader/` for modern features
3. **Monitoring**: Continue enhancing `services/monitor/` independently
4. **Performance**: Use `async_downloader.py` for high-volume tasks
5. **Migration**: Gradually adopt `unified_downloader/` patterns for new development

## Key Features

### Manual Historical Backfill
The system includes a comprehensive manual historical announcement backfill feature:
- **Script**: `manual_historical_backfill.py` (540 lines of code)
- **Input Methods**: Single stock, multiple stocks, or file-based stock lists
- **Date Ranges**: Flexible date configuration (days back or specific date ranges)
- **Component Reuse**: Leverages existing monitoring system components
- **Async Processing**: Supports concurrent processing with configurable batch sizes
- **Filtering**: Configurable dual-stage filtering (stocks + announcement types)
- **Monitoring**: Detailed progress tracking and performance statistics

### Real-time Monitoring System
Enterprise-grade monitoring system with:
- **API Polling**: 60-second intervals with intelligent caching
- **Dual Filtering**: Stock-based and type-based filtering
- **Vector Processing**: Integration with Milvus for semantic search
- **Error Recovery**: Comprehensive retry mechanisms and fault tolerance
- **Performance Monitoring**: Real-time statistics and health metrics
