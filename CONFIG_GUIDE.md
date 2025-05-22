# Configuration Guide for Video Pipeline

This document explains the configuration system for the video pipeline project.

## Configuration System

The video pipeline uses a centralized configuration system through the `config.py` module. This module:

1. Loads environment variables from a `.env` file (if present)
2. Provides default values for all settings
3. Organizes configuration by component (scrapers, validators, storage, etc.)

## Environment Variables vs. config.py

- **config.py**: The authoritative configuration source for the application
- **Environment Variables**: Override default settings in config.py
- **.env file**: Convenient way to set environment variables locally

There is **no need** for a separate `config.json` file. The `config.py` module handles all configuration needs.

## Setting Up Your Configuration

1. Copy the `.env.template` file to `.env`
2. Fill in your API keys and other settings
3. The application will automatically load these settings

Example:
```
cp .env.template .env
nano .env  # Edit with your preferred editor
```

## Configuration Categories

The configuration is organized into these main sections:

1. **SCRAPER_CONFIG**: Settings for each video source (Pexels, Pixabay, etc.)
2. **VALIDATOR_CONFIG**: Settings for content validation (text detection, resolution, etc.)
3. **STORAGE_CONFIG**: Settings for local and cloud storage
4. **BATCH_CONFIG**: Settings for batch processing
5. **API_KEYS**: API keys for various services
6. **Cloud Credentials**: AWS, GCS, and Azure credentials

## Adding New Configuration

If you need to add new configuration options:

1. Add the environment variable to `.env.template`
2. Add the corresponding entry in `config.py` with a default value
3. Use the configuration in your code by importing from `config.py`

## Testing Configuration

The test suite uses mocked configuration values and does not require actual API keys to run.
