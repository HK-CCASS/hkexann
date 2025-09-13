# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive Hong Kong Stock Exchange (HKEX) announcement downloader and monitoring system. The project consists of two main components:

1. **Classic Downloader** (`main.py`) - A standalone tool for downloading historical announcements
2. **Enhanced Monitoring System** (`start_enhanced_monitor.py` + `services/`) - Real-time monitoring with enterprise-grade data processing

## Architecture

### Core Components

- **main.py** (2,100+ lines) - Legacy downloader with CLI interface, needs refactoring due to size
- **services/** - Modular microservices architecture (59 Python files):
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

**Classic Downloader:**
```bash
# Basic usage
python main.py -s 00700 --start 2024-01-01 --end today

# Async mode (recommended for performance)
python main.py -s 00700 --async

# Configuration-based tasks
python main.py --check-config
python main.py --list-tasks
python main.py --run-task "task_name"
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

The project follows a hybrid pattern:
- **Legacy Pattern** - Single large file (`main.py`) with class-based organization
- **Microservices Pattern** - Modular services in `services/` directory
- **Factory Pattern** - Service initialization through managers
- **Observer Pattern** - Event-driven monitoring system

### Important Conventions

1. **Async/Await Usage**: All new code should use async patterns following the monitoring system style
2. **Configuration-Driven**: All behavior should be configurable through YAML files
3. **Error Handling**: Use tenacity library for retries, comprehensive logging
4. **Database Integration**: Support both MySQL (stock lists) and ClickHouse (time-series data)

### File Organization

- Main executables in root directory
- Services organized by functionality in `services/`
- Configuration files in `config/` subdirectory  
- Documentation in `docs/`
- No existing test directory - create `tests/` when adding tests

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

### Monitoring System vs Classic Downloader

The monitoring system (`services/monitor/`) is the core value with 6x more code than the classic downloader. It provides:
- Real-time API polling every 5 minutes
- Dual-stage filtering (stocks + announcement types)
- Enterprise health monitoring
- Vector processing pipeline
- Automatic error recovery

When making changes, prioritize the monitoring system over the legacy downloader unless specifically working on backward compatibility.