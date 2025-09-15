# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive Hong Kong Stock Exchange (HKEX) announcement downloader and monitoring system. The project has undergone a major architectural refactoring (v3.0) and now consists of three main components:

1. **Modularized Architecture** (`legacy/`) - Clean architecture implementation of the downloader (v3.0, refactored 2025-09-13)
2. **Enhanced Monitoring System** (`start_enhanced_monitor.py` + `services/`) - Real-time monitoring with enterprise-grade data processing
3. **Original Downloader** (`main.py`) - Legacy standalone tool (2,100+ lines, preserved for backward compatibility)

## Architecture

### Core Components

#### New Modular Architecture (Recommended)
- **legacy/** - Clean architecture implementation with 31 modular files:
  - `main_cli.py` - New entry point, fully compatible with original CLI
  - `presentation/` - CLI and configuration layer
  - `business/` - Domain services (announcement, download, stock info, classification)
  - `data_access/` - Repository pattern implementation
  - `infrastructure/` - Technical implementation (database, DI, error handling, process management)

#### Legacy Components
- **main.py** (2,100+ lines) - Original monolithic implementation (preserved for compatibility)
- **services/** - Modular microservices architecture (60 Python files):
  - `monitor/` - Real-time API monitoring (18,000+ lines across multiple files)  
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

**New Modular Architecture (Recommended):**
```bash
# Basic usage - fully compatible with original CLI
python legacy/main_cli.py -s 00700 --start 2024-01-01 --end today

# Async mode (recommended for performance)
python legacy/main_cli.py -s 00700 --async

# Configuration-based tasks
python legacy/main_cli.py --check-config
python legacy/main_cli.py --list-tasks
python legacy/main_cli.py --run-task "task_name"
```

**Original Downloader (Backward Compatibility):**
```bash
# Basic usage
python main.py -s 00700 --start 2024-01-01 --end today

# All original commands still work
python main.py -s 00700 --async
```

**Enhanced Monitoring System:**
```bash
# Start real-time monitoring
python start_enhanced_monitor.py

# With forced async mode
HKEX_FORCE_ASYNC=true python main.py --config config.yaml
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

### Daemon Mode
```bash
python main.py --daemon-start
python main.py --daemon-status
python main.py --daemon-stop
python main.py --daemon-restart
python main.py --daemon-test
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

# Test daemon control functionality
python daemon_control.py test
```

### Code Quality and Testing
```bash
# Currently no formal testing framework configured
# When adding tests, create them in tests/ directory following pytest conventions
# No linting or formatting tools currently configured in project
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

- **Root Directory**: Main executables (`main.py`, `start_enhanced_monitor.py`, `daemon_control.py`)
- **legacy/** (NEW v3.0): Clean architecture implementation
  - `main_cli.py`: New modular entry point
  - `presentation/`: CLI, formatters, configuration adapters
  - `business/services/`: Domain services (announcement, download, stock info, classification)
  - `data_access/`: Repository pattern implementations
  - `infrastructure/`: Technical concerns (database, DI, error handling, process management)
- **services/**: Modular microservices (60 files across 6 major modules)
  - `monitor/`: Core monitoring system with 20+ files
  - `data_loader/`: Database integration utilities
  - `document_processor/`: PDF processing pipeline
  - `embeddings/`: SiliconFlow AI integration
  - `milvus/`: Vector database operations
  - `storage/`: File management utilities
- **config/**: Configuration management (`settings.py`)
- **docs/**: Comprehensive documentation
  - `架构文档.md`: Complete architecture documentation
  - `迁移指南.md`: Migration guide from main.py to modular architecture
  - `使用说明.md`: User manual for CLI and features
  - `开发指南.md`: Developer guide for extending the system
  - `测试文档.md`: Testing strategy and implementation
- **sql/**: Database schema and queries
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
- **Scheduling**: Schedule library for daemon mode, psutil for process management

### System Components Comparison

| Component | Lines of Code | Architecture | Use Case |
|-----------|--------------|--------------|----------|
| **legacy/** (v3.0) | ~2,100 (modularized) | Clean Architecture | New development, maintainable code |
| **main.py** | 2,100+ (monolithic) | Single file | Backward compatibility only |
| **services/monitor/** | 18,000+ | Microservices | Real-time monitoring |

#### Modular Architecture (legacy/) - RECOMMENDED
The new clean architecture implementation provides:
- **Separation of Concerns**: Each module has a single responsibility
- **Testability**: All components can be tested in isolation
- **Maintainability**: Changes are localized to specific modules
- **Extensibility**: New features can be added without modifying existing code
- **100% CLI Compatibility**: Drop-in replacement for main.py

#### Monitoring System (services/monitor/)
The monitoring system is the enterprise-grade component with:
- Real-time API polling every 5 minutes
- Dual-stage filtering (stocks + announcement types)
- Enterprise health monitoring
- Vector processing pipeline
- Automatic error recovery

#### Development Priority
1. **New Features**: Implement in `legacy/` modular architecture
2. **Bug Fixes**: Apply to both `legacy/` and `main.py` if affecting both
3. **Monitoring**: Continue enhancing `services/monitor/` independently
4. **Migration**: Gradually move functionality from `main.py` to `legacy/`