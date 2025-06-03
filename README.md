# AfterQuery Video Collection Pipeline - README

## Project Overview

This project implements a complete pipeline for collecting, processing, and validating 1,000+ hours of video content. This project was developed as a solution to the AfterQuery SWE Intern Take Home assignment. The pipeline is designed to be scalable, modular, and robust, with careful attention to technical specifications and content requirements.

## Key Features

- **Multi-source Video Collection**: Scrapes videos from legally approved platforms
- **Technical Processing**: Ensures all videos meet 512x512 resolution, 20+ FPS, and MP4 format requirements
- **Content Validation**: Detects and filters out videos with cut scenes, text overlays, or unrealistic physics
- **Parallelized Processing**: Efficiently processes videos using multiple workers and scrapers concurrently
- **Batch Processing Controls**: Manages local overhead with configurable batch sizes and cleanup
- **Comprehensive Validation**: Includes tests to verify pipeline functionality
- **Scalable Architecture**: Designed to scale to process 1000+ hours of video content

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

```bash
# Create a .env file with your API keys
echo "PEXELS_API_KEY=your_pexels_api_key" > .env
echo "PIXABAY_API_KEY=your_pixabay_api_key" >> .env
echo "VIDEVO_API_KEY=your_videvo_api_key" >> .env
echo "NASA_API_KEY=your_nasa_api_key" >> .env
echo "IA_ACCESS_KEY=your_internet_archive_access_key" >> .env
echo "IA_SECRET_KEY=your_internet_archive_secret_key" >> .env
echo "NOAA_API_TOKEN=your_noaa_api_token" >> .env
```

3. Run the pipeline with default settings:

```bash
python main.py
```

4. Run with enhanced parallelization and batch controls:

```bash
python main.py --enhanced --batch-size 20 --target-hours 10 --output-destination cloud
```

5. For more options:

```bash
python main.py --help
```

## Command Line Options

The pipeline supports the following command line options:

```
--config CONFIG         Path to configuration file (default: config.json)
--log-dir LOG_DIR       Directory for log files (default: logs)
--log-level LOG_LEVEL   Logging level (default: INFO)
--source SOURCE         Video source to use (default: pexels)
--query QUERY           Search query (default: nature)
--max-videos MAX_VIDEOS Maximum number of videos to process (default: 10)
--batch-id BATCH_ID     Resume processing for a specific batch ID
--enhanced              Use enhanced batch processor with parallelization
--batch-size BATCH_SIZE Size of each processing batch
--target-hours HOURS    Target hours of video to collect
--output-destination DEST Output destination (local or cloud)
--max-workers WORKERS   Maximum number of worker threads
--max-scrapers SCRAPERS Maximum number of concurrent scrapers
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

## License

This project is licensed under the [Choose Your License, e.g., MIT License] - see the LICENSE.md file for details.

## Contributing

Contributions are welcome! Please see the CONTRIBUTING.md for details on how to contribute to this project.
