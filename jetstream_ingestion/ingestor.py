import asyncio
import json
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import websockets
from .writer import ParquetWriter
from .utils import setup_logging, parse_args

logger = logging.getLogger(__name__)

class JetstreamIngestor:
    def __init__(self, output_path: Union[str, Path], batch_size: int = 1000):
        """Initialize the Jetstream ingestor.
        
        Args:
            output_path: Local directory or GCS bucket path for output
            batch_size: Number of records to batch before writing
        """
        self.output_path = str(output_path)
        self.batch_size = batch_size
        self.writer = ParquetWriter(self.output_path)
        self.running = True
        self.messages = []
        self.websocket = None
        self._shutdown_event = asyncio.Event()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """Handle graceful shutdown on signals."""
        logger.info("Shutdown signal received, cleaning up...")
        self.running = False
        # Use call_soon_threadsafe since we're in a signal handler
        asyncio.get_event_loop().call_soon_threadsafe(self._shutdown_event.set)

    async def process_message(self, message: str):
        """Process a single message from the websocket.
        
        Args:
            message: JSON string from websocket
        """
        received_ts = datetime.utcnow().isoformat()
        self.messages.append({
            'receivedTimestamp': received_ts,
            'payload': message
        })
        
        if len(self.messages) >= self.batch_size:
            await self.flush_messages()

    async def flush_messages(self):
        """Flush current batch of messages to storage."""
        if self.messages:
            try:
                self.writer.write_batch(self.messages)
                logger.info(f"Wrote batch of {len(self.messages)} messages")
                self.messages = []
            except Exception as e:
                logger.error(f"Error writing batch: {e}")
                raise

    async def connect(self, uri: str = "wss://jetstream2.us-east.bsky.network/subscribe"):
        """Connect to Jetstream websocket and begin processing messages.
        
        Args:
            uri: Websocket URI for Jetstream
        """
        while self.running:
            try:
                async with websockets.connect(uri) as websocket:
                    logger.info(f"Connected to {uri}")
                    while self.running:
                        try:
                            message = await websocket.recv()
                            await self.process_message(message)
                        except websockets.ConnectionClosed:
                            logger.warning("Connection closed, attempting to reconnect...")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            continue
            except Exception as e:
                logger.error(f"Connection error: {e}")
                if self.running:
                    await asyncio.sleep(5)  # Wait before reconnecting

        # Final flush of messages
        await self.flush_messages()                        
            

async def run_ingestor(output_path: Union[str, Path], batch_size: int = 1000):
    """Run the ingestor process.
    
    Args:
        output_path: Path to write output files
        batch_size: Number of records to batch before writing
    """
    ingestor = JetstreamIngestor(output_path, batch_size)
    await ingestor.connect()

def main():
    """Main entry point for the application."""
    args = parse_args()
    setup_logging()
    
    try:
        # Set up asyncio policy for better signal handling
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        main_task = asyncio.ensure_future(run_ingestor(args.output_path, args.batch_size))
        
        for s in [signal.SIGINT, signal.SIGTERM]:
            loop.add_signal_handler(s, main_task.cancel)

        try:
            loop.run_until_complete(main_task)
        except asyncio.exceptions.CancelledError as cex:
            logger.info(f"Cancelled {cex}")
        finally:
            # Ensure proper cleanup of the event loop
            try:
                loop.run_until_complete(main_task)
                # loop.run_until_complete(loop.shutdown_asyncgens())
                # all_tasks = asyncio.all_tasks(loop)
                # if all_tasks:
                #     loop.run_until_complete(asyncio.gather(*all_tasks))
            except asyncio.exceptions.CancelledError as cex:
                pass # wacky... Ctrl+C gets propagated to the finally?
            finally:
                loop.close()
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
