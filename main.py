import os
from dotenv import load_dotenv
load_dotenv()
import sys
import logging
import argparse
import json
import time
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from scrapers.parallel_scraper_manager import ParallelScraperManager
from processors.enhanced_batch_processor import EnhancedBatchProcessor
from validators.validation_pipeline import ValidationPipeline
from storage.cloud_storage import CloudStorageUploader

# Import scrapers
from scrapers.pexels_scraper import PexelsScraper
from scrapers.videvo_scraper import VidevoScraper
from scrapers.nasa_scraper import NASAScraper
from scrapers.internet_archive_scraper import InternetArchiveScraper
from scrapers.wikimedia_scraper import WikimediaScraper
from scrapers.coverr_scraper import CoverrScraper
from scrapers.noaa_scraper import NOAAScraper
from scrapers.pixabay_scraper import PixabayScraper

# Configure logging
def setup_logging(log_dir: str, log_level: str):
    """Set up logging configuration."""
    os.makedirs(log_dir, exist_ok=True)
    
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    
    level = log_level_map.get(log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(log_dir, "pipeline.log")),
            logging.StreamHandler()
        ]
    )
    
    # Set up validation log
    validation_logger = logging.getLogger("validation")
    validation_handler = logging.FileHandler(os.path.join(log_dir, "validation.log"))
    validation_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    validation_logger.addHandler(validation_handler)
    validation_logger.setLevel(level)
    
    return logging.getLogger("main")

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from file."""
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        return {}

def parse_sources_arg(sources_arg: str) -> Dict[str, str]:
    """Parse sources argument in format 'source1:query1;source2:query2'."""
    sources = {}
    if not sources_arg:
        return sources
    
    pairs = sources_arg.replace(";", ",").split(",")
    for pair in pairs:
        if ":" in pair:
            source, query = pair.split(":", 1)
            sources[source.strip()] = query.strip()
    
    return sources

def main():
    """Main entry point for the video pipeline."""
    parser = argparse.ArgumentParser(description="Video Collection Pipeline")
    parser.add_argument("--config", default="config.json", help="Path to configuration file")
    parser.add_argument("--log-dir", default="logs", help="Directory for log files")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--source", help="Single or multiple sources in 'source:query' format (semicolon-separated)")
    parser.add_argument("--max-videos", type=int, default=10, help="Maximum number of videos to process")
    parser.add_argument("--batch-id", help="Resume processing for a specific batch ID")
    parser.add_argument("--batch-size", type=int, help="Size of each processing batch")
    parser.add_argument("--target-hours", type=float, help="Target hours of video to collect")
    parser.add_argument("--output-destination", choices=["local", "cloud"], help="Output destination")
    parser.add_argument("--max-workers", type=int, help="Maximum number of worker threads")
    parser.add_argument("--max-scrapers", type=int, help="Maximum number of concurrent scrapers")
    parser.add_argument("--disk-overhead-threshold", type=float, help="Disk space overhead threshold")
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging(args.log_dir, args.log_level)
    logger.info("Starting video pipeline")
    
    # Load configuration
    config = load_config(args.config)
    if not config:
        logger.error("Failed to load configuration")
        return 1
    
    # Initialize components
    try:
        # Initialize validation pipeline
        validation_config = config.get("validators", {})
        validation_pipeline = ValidationPipeline(validation_config)
        
        # Initialize cloud storage uploader if needed
        storage_config = config.get("storage", {})
        cloud_uploader = CloudStorageUploader(storage_config)
        
        # Initialize batch processor
        batch_config = config.get("batch", {})
        
        # Apply command line overrides to batch config
        if args.batch_size is not None:
            batch_config["batch_size"] = args.batch_size
        if args.target_hours is not None:
            batch_config["target_hours"] = args.target_hours
        if args.output_destination is not None:
            batch_config["output_destination"] = args.output_destination
        if args.max_workers is not None:
            batch_config["max_workers"] = args.max_workers
        
        logger.info("Using enhanced batch processor with parallelization")
        batch_processor = EnhancedBatchProcessor(batch_config)
        
        # Set validation pipeline and cloud uploader
        batch_processor.set_validation_pipeline(validation_pipeline)
        batch_processor.set_cloud_uploader(cloud_uploader)
        
        # Initialize scrapers
        scrapers_config = config.get("scrapers", {})
        
        # Initialize parallel scraper manager
        parallel_config = config.get("parallel", {})
        
        # Apply command line overrides to parallel config
        if args.max_workers is not None:
            parallel_config["max_workers"] = args.max_workers
        if args.max_scrapers is not None:
            parallel_config["max_scrapers"] = args.max_scrapers
        if args.target_hours is not None:
            parallel_config["target_hours"] = args.target_hours
        
        parallel_scraper_manager = batch_processor.parallel_scraper_manager
        
        # Register all available scrapers
        scraper_classes = {
            "pexels": PexelsScraper,
            "videvo": VidevoScraper,
            "nasa": NASAScraper,
            "internet_archive": InternetArchiveScraper,
            "wikimedia": WikimediaScraper,
            "coverr": CoverrScraper,
            "noaa": NOAAScraper,
            "pixabay": PixabayScraper
        }
        
        for name, scraper_class in scraper_classes.items():
            scraper_config = scrapers_config.get(name, {})
            scraper = scraper_class(scraper_config)
            batch_processor.register_scraper(name, scraper)
            logger.info(f"Registered scraper: {name}")
        
        # Determine sources and queries
        sources = parse_sources_arg(args.source)
        if not sources:
            logger.error("No valid sources provided via --source (expected format: 'pexels:nature')")
            return 1
        logger.info(f"Using sources: {list(sources.keys())}")
        
        config_override = {}
        if args.disk_overhead_threshold is not None:
            config_override["disk_space_threshold"] = args.disk_overhead_threshold
        
        # Process videos
        if args.batch_id:
            # Resume batch processing
            logger.info(f"Resuming batch processing for batch ID: {args.batch_id}")
            result = batch_processor.resume_batch(args.batch_id)
            if result.get("success", False):
                logger.info("Batch processing completed successfully")
                # No per-source logging in resume mode, assume totals are handled within resume_batch or log totals after this block
            else:
                logger.error(f"Batch processing failed: {result.get('error', 'Unknown error')}")
                return 1
        else:
            total_processed = 0
            total_validated = 0
            total_uploaded = 0
            total_duration = 0.0

            if len(sources) > 1:
                # Multiple sources: process in parallel using ThreadPoolExecutor
                results = []
                with ThreadPoolExecutor(max_workers=len(sources)) as executor:
                    future_to_source = {
                        executor.submit(
                            batch_processor.process_batch,
                            sources={source: query},
                            batch_size=args.batch_size,
                            target_hours=args.target_hours,
                            output_destination=args.output_destination,
                            config_override=config_override
                        ): source for source, query in sources.items()
                    }
                    for future in as_completed(future_to_source):
                        source = future_to_source[future]
                        try:
                            result = future.result()
                            results.append((source, result))
                        except Exception as exc:
                            logger.error(f"Batch processing failed for source {source}: {exc}")
                            # Continue processing other sources even if one fails
                            results.append((source, {"success": False, "error": str(exc)}))
                
                # Aggregate results from all sources
                all_success = True
                for source, result in results:
                    if result.get("success", False):
                        logger.info(f"Batch processing completed successfully for source: {source}")
                        total_processed += result.get("videos_downloaded", 0)
                        total_validated += result.get("videos_validated", 0)
                        total_uploaded += result.get("videos_uploaded", 0)
                        total_duration += result.get("video_hours", 0)
                    else:
                        logger.error(f"Batch processing failed for source {source}: {result.get('error', 'Unknown error')}")
                        all_success = False # Mark overall process as failed if any source fails

                if not all_success and not results: # If results is empty, it means something failed before any processing started
                    return 1 # Exit if no sources were processed successfully at all
            
            else:
                # Single source: process directly
                logger.info("Starting new batch processing")
                result = batch_processor.process_batch(
                    sources=sources,
                    batch_size=args.batch_size,
                    target_hours=args.target_hours,
                    output_destination=args.output_destination,
                    config_override=config_override
                )
                
                if result.get("success", False):
                    logger.info("Batch processing completed successfully")
                    total_processed = result.get("videos_downloaded", 0)
                    total_validated = result.get("videos_validated", 0)
                    total_uploaded = result.get("videos_uploaded", 0)
                    total_duration = result.get("video_hours", 0)
                else:
                    logger.error(f"Batch processing failed: {result.get('error', 'Unknown error')}")
                    return 1
            
            # Calculate failed videos
            total_failed = total_processed - total_validated

            # Log total results
            logger.info("\n--- Overall Pipeline Summary ---")
            logger.info(f"Total videos processed: {total_processed}")
            logger.info(f"Total videos validated: {total_validated}")
            logger.info(f"Total videos failed validation: {total_failed}")
            logger.info(f"Total videos uploaded: {total_uploaded}")
            logger.info(f"Total video duration: {total_duration:.2f} hours")
            logger.info("------------------------------")

            return 0
        
    except Exception as e:
        logger.exception(f"Error in pipeline: {str(e)}")
        return 1

if __name__ == "__main__":
    start_time = time.time()
    exit_code = main()
    elapsed = time.time() - start_time
    # Ensure logging is configured before logging here
    logger = logging.getLogger("main")
    logger.info(f"Total pipeline execution time: {elapsed:.2f} seconds")
    sys.exit(exit_code)