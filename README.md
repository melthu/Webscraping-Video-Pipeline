# Video Collection Pipeline - README

## Project Overview

This project implements a robust and scalable pipeline for collecting, processing, and validating video content from various online sources. It is designed to handle large volumes of video data, ensuring technical specifications and content quality.

## Key Features

- **Multi-source Video Collection**: Scrapes videos from supported online platforms.
- **Supported Sources**: Pexels, Pixabay, Videvo, NASA, Internet Archive, Wikimedia Commons, Coverr, NOAA
- **Technical Processing**: Ensures all videos meet specified resolution (512x512), frame rate (20+ FPS), and format (MP4) requirements.
- **Content Validation**: Detects and filters out videos based on criteria such as cut scenes, text overlays, and unrealistic physics.
- **Parallelized Processing**: Efficiently processes videos using configurable parallel workers and concurrent scrapers.
- **Batch Processing Controls**: Manages local resource overhead with configurable batch sizes, target collection hours, and temporary file cleanup.
- **Comprehensive Testing**: Includes tests to verify pipeline functionality.
- **Scalable Architecture**: Designed for processing large datasets of video content.

## Directory Structure

```
video_pipeline/
├── config.py                # Configuration settings
├── main.py                  # Main entry point
├── documentation.md         # Comprehensive documentation
├── requirements.txt         # Dependencies
├── scrapers/                # Video source scrapers
│   ├── __init__.py
│   ├── base_scraper.py      # Base scraper class
│   ├── parallel_scraper_manager.py  # Parallel scraping orchestration
│   ├── pexels_scraper.py    # Pexels implementation
│   ├── videvo_scraper.py    # Videvo implementation
│   ├── nasa_scraper.py      # NASA implementation
│   ├── internet_archive_scraper.py  # Internet Archive implementation
│   ├── wikimedia_scraper.py # Wikimedia implementation
│   ├── coverr_scraper.py    # Coverr implementation
│   └── noaa_scraper.py      # NOAA implementation
├── processors/              # Video processing modules
│   ├── __init__.py
│   ├── video_processor.py   # Video processing utilities
│   ├── batch_processor.py   # Original batch processor
│   └── enhanced_batch_processor.py  # Enhanced batch processor with parallelization
├── validators/              # Video validation modules
│   ├── __init__.py
│   └── validation_pipeline.py  # Validation orchestration
├── storage/                 # Storage modules
│   ├── __init__.py
│   └── cloud_storage.py     # Cloud storage integration
├── tests/                   # Validation tests
│   ├── __init__.py
│   └── validation_test.py   # Test suite
├── logs/                    # Log files (created at runtime)
├── downloads/               # Temporary download storage (created at runtime)
├── processed/               # Processed video storage (created at runtime)
├── failed/                  # Failed video storage (created at runtime)
└── temp/                    # Temporary storage (created at runtime)
```

## Requirements

- Python 3.8+
- FFmpeg
- OpenCV dependencies
- API keys for video platforms (Pexels, Pixabay, etc.)
- Sufficient disk space for temporary storage during processing

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set up API keys:

Create a `.env` file in the project root directory with your API keys for the sources you intend to use. This file is ignored by Git to prevent accidental public exposure of your keys. The required variables are:

```dotenv
PEXELS_API_KEY=your_pexels_api_key
PIXABAY_API_KEY=your_pixabay_api_key
VIDEVO_API_KEY=your_videvo_api_key
NASA_API_KEY=your_nasa_api_key
IA_ACCESS_KEY=your_internet_archive_access_key
IA_SECRET_KEY=your_internet_archive_secret_key
NOAA_API_TOKEN=your_noaa_api_token
# Add credentials for cloud storage if using 'cloud' output destination
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=your_aws_region
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/google/credentials.json
AZURE_STORAGE_CONNECTION_STRING=your_azure_storage_connection_string
# Or for Azure using account name and key:
# AZURE_STORAGE_ACCOUNT=your_azure_storage_account
# AZURE_STORAGE_KEY=your_azure_storage_key
```

**Important**: Do NOT commit your `.env` file to your repository.

3. Run the pipeline:

Run the pipeline using `python main.py` with the desired command line arguments. You must specify at least one source and its query using the `--source` argument.

```bash
python main.py --source "pexels:nature,pixabay:animals" --max-videos 50
```

To target a specific duration instead of a maximum number of videos:

```bash
python main.py --source "videvo:technology" --target-hours 2
```

For more options:

```bash
python main.py --help
```

## Command Line Options

The pipeline supports the following command line options:

```
--config CONFIG           Path to configuration file (default: config.json)
--log-dir LOG_DIR         Directory for log files (default: logs)
--log-level LOG_LEVEL     Logging level (default: INFO)
--source SOURCE           Sources and queries in 'source1:query1;source2:query2' or 'source1:query1,source2:query2' format (required)
--max-videos MAX_VIDEOS   Maximum number of videos to process per source (default: 10)
--batch-id BATCH_ID       Resume processing for a specific batch ID
--batch-size BATCH_SIZE   Size of each processing batch (overrides config.json)
--target-hours HOURS      Target hours of video to collect per source (overrides config.json, alternative to --max-videos)
--output-destination DEST Output destination (local or cloud) (overrides config.json)
--max-workers WORKERS     Maximum number of worker threads for processing (overrides config.json)
--max-scrapers SCRAPERS   Maximum number of concurrent scrapers (overrides config.json)
--disk-overhead-threshold THRESHOLD Disk space overhead threshold (overrides config.json)
```

## Parallelization Features

The pipeline now includes robust parallelization capabilities:

- **Multi-scraper Parallelization**: Run multiple scrapers concurrently
- **Multi-worker Processing**: Process multiple videos in parallel
- **Resource-aware Scheduling**: Monitors CPU and memory usage to prevent overload
- **Configurable Concurrency**: Adjust worker and scraper counts based on available resources

## Batch Processing Controls

Enhanced batch processing controls include:

- **Configurable Batch Size**: Control the number of videos processed in each batch
- **Target Hours**: Specify the total hours of video to collect
- **Local Overhead Management**: Automatically cleans up temporary files between batches
- **Output Destination Control**: Choose between local storage or cloud upload
- **Resource Monitoring**: Tracks disk space, memory, and CPU usage

## Important Notes

- **API Keys**: Valid API keys are required for full functionality
- **Storage Requirements**: Processing 1,000 hours of video requires significant storage space
- **Computational Resources**: The pipeline is resource-intensive; adjust worker count based on available CPU/memory
- **Temporary Storage**: The pipeline uses temporary storage for downloads and processing; ensure sufficient disk space

## Scaling Considerations

The pipeline is designed to scale through:

- Horizontal scaling (adding more worker nodes)
- Parallel scraper execution
- Configurable batch processing
- Resource-aware scheduling
- Checkpointing for resumable operations

For full details on scaling to process 1000+ hours of video, see the [documentation](documentation.md#scaling-considerations).

## Validation

Run the validation tests to verify pipeline functionality:

```bash
python -m tests.validation_test
```

Note: Full validation requires valid API keys for the video platforms.

## Documentation

For comprehensive documentation, see [documentation.md](documentation.md).

## Contributing

Details on how to contribute to this project.

## License

Information about the project's license.
