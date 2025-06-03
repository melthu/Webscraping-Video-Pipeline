AfterQuery Video Collection Pipeline

Project Overview

The AfterQuery Video Collection Pipeline automates the collection, processing, and validation of 1,000+ hours of video content. Developed as part of the AfterQuery SWE Intern assessment, this pipeline is designed to be scalable, modular, and reliable, with careful attention to both technical and content requirements.

Key Features
	•	Multi-source Video Collection
Scrapes videos from approved platforms (Pexels, Pixabay, Videvo, NASA, Internet Archive, Wikimedia Commons, Coverr, NOAA).
	•	Technical Processing
Ensures each video meets 512×512 resolution, ≥20 FPS, and MP4 format requirements.
	•	Content Validation
Detects and filters out videos with cut scenes, text overlays, or unrealistic physics.
	•	Parallelized Processing
Processes videos concurrently using multiple workers and scrapers.
	•	Batch Processing Controls
Configurable batch size, local-overhead management (automatic cleanup), and checkpoints for resumable operations.
	•	Comprehensive Validation
Includes automated tests to verify pipeline functionality.
	•	Scalable Architecture
Designed to scale horizontally (multiple worker nodes) and vertically (configurable concurrency, resource-aware scheduling).

Technologies & Dependencies
	•	Language: Python 3.8+
	•	Video Tools: FFmpeg, OpenCV
	•	APIs & Services: Pexels, Pixabay, Videvo, NASA, Internet Archive, Wikimedia, Coverr, NOAA, AWS S3 (optional)
	•	Other: python-dotenv for environment variable management

Directory Structure

video_pipeline/
├── config.py                    # Configuration settings
├── main.py                      # Main entry point
├── documentation.md             # Detailed documentation
├── requirements.txt             # Python dependencies
├── scrapers/                    # Video source scrapers
│   ├── __init__.py
│   ├── base_scraper.py          # Base scraper class
│   ├── parallel_scraper_manager.py  # Orchestrates parallel scraping
│   ├── pexels_scraper.py        # Pexels implementation
│   ├── videvo_scraper.py        # Videvo implementation
│   ├── nasa_scraper.py          # NASA implementation
│   ├── internet_archive_scraper.py  # Internet Archive implementation
│   ├── wikimedia_scraper.py     # Wikimedia implementation
│   ├── coverr_scraper.py        # Coverr implementation
│   └── noaa_scraper.py          # NOAA implementation
├── processors/                  # Video processing modules
│   ├── __init__.py
│   ├── video_processor.py       # Video processing utilities
│   ├── batch_processor.py       # Original batch processor
│   └── enhanced_batch_processor.py  # Parallelized batch processor
├── validators/                  # Video validation modules
│   ├── __init__.py
│   └── validation_pipeline.py   # Validation orchestration
├── storage/                     # Storage modules
│   ├── __init__.py
│   └── cloud_storage.py         # Cloud storage integration
├── tests/                       # Validation tests
│   ├── __init__.py
│   └── validation_test.py       # Test suite
├── logs/                        # Runtime log files
├── downloads/                   # Temporary download storage
├── processed/                   # Processed video storage
├── failed/                      # Failed video storage
└── temp/                        # Temporary storage

Installation
	1.	Clone the repository

git clone https://github.com/yourusername/afterquery-video-pipeline.git
cd afterquery-video-pipeline/video_pipeline


	2.	Install dependencies

pip install -r requirements.txt


	3.	Install FFmpeg & OpenCV prerequisites
	•	On macOS:

brew install ffmpeg opencv


	•	On Ubuntu/Debian:

sudo apt-get update && sudo apt-get install ffmpeg libopencv-dev


	4.	Set up API keys
Create a .env file in the project root and add your keys:

PEXELS_API_KEY=your_pexels_api_key
PIXABAY_API_KEY=your_pixabay_api_key
VIDEVO_API_KEY=your_videvo_api_key
NASA_API_KEY=your_nasa_api_key
IA_ACCESS_KEY=your_internet_archive_access_key
IA_SECRET_KEY=your_internet_archive_secret_key
NOAA_API_TOKEN=your_noaa_api_token



Quick Start
	1.	Run with default settings

python main.py


	2.	Use enhanced parallelization

python main.py --enhanced --batch-size 20 --target-hours 10 --output-destination cloud


	3.	Display help for all options

python main.py --help



Command Line Options

--config CONFIG           Path to configuration file (default: config.json)
--log-dir LOG_DIR         Directory for log files (default: logs)
--log-level LOG_LEVEL     Logging level (default: INFO)
--source SOURCE           Video source to use (default: pexels)
--query QUERY             Search query (default: nature)
--max-videos MAX_VIDEOS   Maximum number of videos to process (default: 10)
--batch-id BATCH_ID       Resume processing for a specific batch ID
--enhanced                Use enhanced batch processor with parallelization
--batch-size BATCH_SIZE   Number of videos per batch
--target-hours HOURS      Target total hours of video to collect
--output-destination DEST Output destination (local or cloud)
--max-workers WORKERS     Maximum number of worker threads
--max-scrapers SCRAPERS   Maximum number of concurrent scrapers

Parallelization Features
	•	Multi-scraper Execution: Run multiple scrapers concurrently
	•	Multi-worker Processing: Process videos in parallel threads
	•	Resource-aware Scheduling: Monitors CPU and memory usage to avoid overload
	•	Configurable Concurrency: Customize worker and scraper counts based on resources

Batch Processing Controls
	•	Configurable Batch Size: Set the number of videos per batch
	•	Target Hours: Define the total video duration to collect
	•	Local Overhead Management: Automatically remove temporary files between batches
	•	Output Destination: Choose between local storage or cloud (AWS S3)
	•	Resource Monitoring: Track disk space, memory, and CPU usage

Scaling Considerations

To scale for 1,000+ hours of video:
	1.	Horizontal Scaling: Distribute work across multiple machines or containers
	2.	Parallel Scraping: Increase the number of concurrent scrapers based on API rate limits
	3.	Configurable Batch Processing: Adjust batch size and worker count to maximize throughput
	4.	Resource-aware Scheduling: Dynamically throttle tasks if system resources become constrained
	5.	Checkpointing: Use batch IDs to resume long-running operations without reprocessing

For detailed instructions, see documentation.md.

Validation & Testing

Run the validation tests to verify end-to-end pipeline functionality:

python -m tests.validation_test

Note: Full validation requires valid API keys for all video platforms.

Important Notes
	•	API Keys: Required for full functionality—ensure each key is valid and has sufficient quota.
	•	Storage Requirements: Collecting 1,000+ hours may require multiple terabytes of temporary storage.
	•	Compute Resources: Adjust --max-workers and --max-scrapers according to available CPU/memory.
	•	Temporary Storage: The pipeline uses downloads/, temp/, processed/, and failed/ directories—ensure you have enough free disk space.

Documentation

For detailed usage, configuration, and architecture, refer to documentation.md.

Contributing

Contributions are welcome! To get started:
	1.	Fork the repository.
	2.	Create a new branch (git checkout -b feature/your-feature).
	3.	Make changes and add tests if applicable.
	4.	Submit a pull request.

See CONTRIBUTING.md for guidelines.

License

This project is licensed under the MIT License. See LICENSE.md for details.
