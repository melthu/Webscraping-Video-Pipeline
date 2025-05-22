# AfterQuery Video Collection Pipeline - README

## Project Overview

This project implements a complete pipeline for collecting, processing, and validating 1,000 hours of video content according to the AfterQuery SWE Intern Take Home assignment requirements. The pipeline is designed to be scalable, modular, and robust, with careful attention to technical specifications and content requirements.

## Key Features

- **Multi-source Video Collection**: Scrapes videos from legally approved platforms
- **Technical Processing**: Ensures all videos meet 512x512 resolution, 20+ FPS, and MP4 format requirements
- **Content Validation**: Detects and filters out videos with cut scenes, text overlays, or unrealistic physics
- **Parallelized Processing**: Efficiently processes videos using multiple workers
- **Comprehensive Validation**: Includes tests to verify pipeline functionality
- **Scalable Architecture**: Designed to scale to 100-1000x the current capacity

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
│   └── pexels_scraper.py    # Pexels implementation
├── processors/              # Video processing modules
│   ├── __init__.py
│   └── video_processor.py   # Video processing and validation
├── orchestrator/            # Pipeline coordination
│   ├── __init__.py
│   └── pipeline.py          # Main orchestrator
├── tests/                   # Validation tests
│   ├── __init__.py
│   └── validation_test.py   # Test suite
├── temp/                    # Temporary storage (created at runtime)
└── output/                  # Processed video output (created at runtime)
```

## Requirements

- Python 3.8+
- FFmpeg
- OpenCV dependencies
- API keys for video platforms (Pexels, Pixabay, etc.)

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
```

3. Run the pipeline:

```bash
python main.py
```

4. For more options:

```bash
python main.py --help
```

## Important Notes

- **API Keys**: Valid API keys are required for full functionality
- **Storage Requirements**: Processing 1,000 hours of video requires significant storage space
- **Computational Resources**: The pipeline is resource-intensive; adjust worker count based on available CPU/memory

## Scaling Considerations

The pipeline is designed to scale through:

- Horizontal scaling (adding more worker nodes)
- Queue-based architecture for component decoupling
- Checkpointing for resumable operations
- Distributed processing capabilities

For full details on scaling to 100-1000x capacity, see the [documentation](documentation.md#scaling-considerations).

## Validation

Run the validation tests to verify pipeline functionality:

```bash
python -m tests.validation_test
```

Note: Full validation requires valid API keys for the video platforms.

## Documentation

For comprehensive documentation, see [documentation.md](documentation.md).
